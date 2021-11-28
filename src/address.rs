//! Socket address manipulations.

use std::{
    alloc::Layout,
    marker::PhantomData,
    net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6},
    ptr::NonNull,
};

// We use a layout for IPv6 because it is large enough to handle both IPv4 and
// IPv6 addresses.
const ADDRESS_LAYOUT: Layout = Layout::new::<libc::sockaddr_in6>();
const LENGTH_LAYOUT: Layout = Layout::new::<libc::socklen_t>();

static_assertions::assert_eq_align!(libc::sockaddr_in6, libc::sockaddr_in);
static_assertions::const_assert!(
    std::mem::size_of::<libc::sockaddr_in6>() >= std::mem::size_of::<libc::sockaddr_in>()
);

/// This types is used as a marker for properly initialized socket address.
pub struct Initialized;

/// This types is used as a marker for maybe uninitialized socket address.
pub struct Uninitialized;

/// A placeholder for an accepted address.
pub struct SocketAddress<InitializationMarker> {
    pub(crate) socket_address: NonNull<libc::sockaddr>,
    pub(crate) address_length: NonNull<libc::socklen_t>,
    _pd: PhantomData<InitializationMarker>,
}

unsafe impl<Marker> Send for SocketAddress<Marker> {}
unsafe impl<Marker> Sync for SocketAddress<Marker> {}

impl<Marker> Drop for SocketAddress<Marker> {
    fn drop(&mut self) {
        unsafe {
            std::alloc::dealloc(self.socket_address.as_ptr().cast(), ADDRESS_LAYOUT);
            std::alloc::dealloc(self.address_length.as_ptr().cast(), LENGTH_LAYOUT);
        }
    }
}

impl Default for SocketAddress<Uninitialized> {
    fn default() -> Self {
        Self::new()
    }
}

impl SocketAddress<Uninitialized> {
    /// Allocates memory for an address.
    pub fn new() -> Self {
        let socket_address = NonNull::new(unsafe { std::alloc::alloc(ADDRESS_LAYOUT) })
            .expect("Unable to allocate space for a socket address")
            .cast();
        let address_length = NonNull::new(unsafe { std::alloc::alloc(LENGTH_LAYOUT) })
            .expect("Unable to allocate space for a socket address length")
            .cast();

        (Self {
            socket_address,
            address_length,
            _pd: PhantomData,
        })
        .reset()
    }

    /// # Safety
    ///
    /// The address must have been filled either by a user or by a system call.
    pub(crate) unsafe fn assume_init(self) -> SocketAddress<Initialized> {
        SocketAddress {
            socket_address: self.socket_address,
            address_length: self.address_length,
            _pd: PhantomData,
        }
    }
}

impl<Marker> SocketAddress<Marker> {
    pub(crate) fn reset(mut self) -> SocketAddress<Uninitialized> {
        unsafe {
            // Fill address with zeroes.
            *self.socket_address.as_mut() = std::mem::zeroed();
            // Set address length to the length we can hold.
            *self.address_length.as_mut() = ADDRESS_LAYOUT.size() as u32;
        }
        self.into_uninit()
    }

    /// Changes the marker type to the [Uninitialized] without actually changing
    /// any data fields.
    pub fn into_uninit(self) -> SocketAddress<Uninitialized> {
        SocketAddress {
            socket_address: self.socket_address,
            address_length: self.address_length,
            _pd: PhantomData,
        }
    }
}

impl SocketAddress<Initialized> {
    /// Converts [SocketAddress] into a [SocketAddr].
    pub fn as_socket_addr(&self) -> SocketAddr {
        const AF_INET: libc::sa_family_t = libc::AF_INET as libc::sa_family_t;
        const AF_INET6: libc::sa_family_t = libc::AF_INET6 as libc::sa_family_t;

        let family = unsafe { self.socket_address.as_ref() }.sa_family;
        match family {
            AF_INET => {
                let socket_address =
                    unsafe { self.socket_address.cast::<libc::sockaddr_in>().as_ref() };
                let [a, b, c, d] = socket_address.sin_addr.s_addr.to_ne_bytes();
                let ip = Ipv4Addr::new(a, b, c, d);
                let port = u16::from_be(socket_address.sin_port);
                SocketAddr::V4(SocketAddrV4::new(ip, port))
            }
            AF_INET6 => {
                let socket_address =
                    unsafe { self.socket_address.cast::<libc::sockaddr_in6>().as_ref() };
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

impl From<&'_ SocketAddress<Initialized>> for SocketAddr {
    fn from(address: &'_ SocketAddress<Initialized>) -> Self {
        address.as_socket_addr()
    }
}
