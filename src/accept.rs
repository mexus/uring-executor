use std::{
    alloc::Layout,
    future::Future,
    net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6, TcpListener, TcpStream},
    ptr::NonNull,
    task::Poll,
};

use crate::with_shared;

// We use a layout for IPv6 because it is large enough to handle both IPv4 and
// IPv6 addresses.
const ADDRESS_LAYOUT: Layout = Layout::new::<libc::sockaddr_in6>();
const LENGTH_LAYOUT: Layout = Layout::new::<libc::socklen_t>();

static_assertions::assert_eq_align!(libc::sockaddr_in6, libc::sockaddr_in);
static_assertions::const_assert!(
    std::mem::size_of::<libc::sockaddr_in6>() >= std::mem::size_of::<libc::sockaddr_in>()
);

/// A placeholder for an accepted address.
pub struct AcceptAddress {
    pub(crate) socket_address: NonNull<libc::sockaddr>,
    pub(crate) address_length: NonNull<libc::socklen_t>,
}

impl Drop for AcceptAddress {
    fn drop(&mut self) {
        unsafe {
            std::alloc::dealloc(self.socket_address.as_ptr().cast(), ADDRESS_LAYOUT);
            std::alloc::dealloc(self.address_length.as_ptr().cast(), LENGTH_LAYOUT);
        }
    }
}

impl Default for AcceptAddress {
    fn default() -> Self {
        Self::new()
    }
}

impl AcceptAddress {
    /// Allocates memory for an address.
    pub fn new() -> Self {
        let socket_address = NonNull::new(unsafe { std::alloc::alloc(ADDRESS_LAYOUT) })
            .expect("Unable to allocate space for a socket address")
            .cast();
        let address_length = NonNull::new(unsafe { std::alloc::alloc(LENGTH_LAYOUT) })
            .expect("Unable to allocate space for a socket address length")
            .cast();

        let mut this = Self {
            socket_address,
            address_length,
        };

        this.reset();
        this
    }

    pub(crate) fn reset(&mut self) {
        unsafe {
            // Fill address with zeroes.
            *self.socket_address.as_mut() = std::mem::zeroed();
            // Set address length to the length we can hold.
            *self.address_length.as_mut() = ADDRESS_LAYOUT.size() as u32;
        }
    }

    /// # Safety
    ///
    /// This is only safe to use when the internal `socket_address` is filled by
    /// a system call.
    pub(crate) unsafe fn assume_init(&self) -> SocketAddr {
        const AF_INET: libc::sa_family_t = libc::AF_INET as libc::sa_family_t;
        const AF_INET6: libc::sa_family_t = libc::AF_INET6 as libc::sa_family_t;
        println!(
            "Accept: family = {}, len = {}",
            self.socket_address.as_ref().sa_family,
            self.address_length.as_ref()
        );
        let family = self.socket_address.as_ref().sa_family;
        match family {
            AF_INET => {
                let socket_address = self.socket_address.cast::<libc::sockaddr_in>().as_ref();
                let [a, b, c, d] = socket_address.sin_addr.s_addr.to_ne_bytes();
                let ip = Ipv4Addr::new(a, b, c, d);
                let port = u16::from_be(socket_address.sin_port);
                SocketAddr::V4(SocketAddrV4::new(ip, port))
            }
            AF_INET6 => {
                let socket_address = self.socket_address.cast::<libc::sockaddr_in6>().as_ref();
                println!("{:?}", socket_address.sin6_addr.s6_addr);
                let ip = Ipv6Addr::from(socket_address.sin6_addr.s6_addr);
                let port = u16::from_be(socket_address.sin6_port);
                SocketAddr::V6(SocketAddrV6::new(
                    ip,
                    port,
                    socket_address.sin6_flowinfo,
                    socket_address.sin6_scope_id,
                ))
            }
            family => panic!("Unexpected socket family 0x{:x}", family),
        }
    }
}

unsafe impl Send for AcceptAddress {}
unsafe impl Sync for AcceptAddress {}

/// A future which resolves into a new incoming connection.
pub struct AcceptFuture {
    pub(crate) token: Option<usize>,
}

impl Future for AcceptFuture {
    type Output = (std::io::Result<(TcpStream, SocketAddr)>, AcceptAddress);

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let token = self.token.take().expect("polled after completion");
        match crate::with_shared(|shared| shared.accept_ready(cx, token)) {
            Some((address, result)) => Poll::Ready((result, address)),
            None => {
                self.token = Some(token);
                Poll::Pending
            }
        }
    }
}

/// An extension trait for [TcpListener].
pub trait ListenerExt {
    /// Creates a future that will resolve when a new connection arrives.
    fn async_accept(&self, address_holder: AcceptAddress) -> AcceptFuture;
}

impl ListenerExt for TcpListener {
    fn async_accept(&self, address_holder: AcceptAddress) -> AcceptFuture {
        with_shared(|shared| shared.accept_socket(self, address_holder))
    }
}
