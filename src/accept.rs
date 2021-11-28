use std::{
    future::Future,
    net::{TcpListener, TcpStream},
    task::Poll,
};

use crate::{
    address::{Initialized, Uninitialized},
    with_shared, SocketAddress,
};

/// A future which resolves into a new incoming connection.
pub struct AcceptFuture {
    pub(crate) token: Option<usize>,
}

impl Future for AcceptFuture {
    type Output = Result<
        (TcpStream, SocketAddress<Initialized>),
        (std::io::Error, SocketAddress<Uninitialized>),
    >;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let token = self.token.take().expect("polled after completion");
        match crate::with_shared(|shared| shared.connection_ready(cx, token)) {
            Some(result) => Poll::Ready(result),
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
    fn async_accept<InitializedMarker>(
        &self,
        address_holder: SocketAddress<InitializedMarker>,
    ) -> AcceptFuture;
}

impl ListenerExt for TcpListener {
    fn async_accept<InitializedMarker>(
        &self,
        address_holder: SocketAddress<InitializedMarker>,
    ) -> AcceptFuture {
        with_shared(|shared| shared.accept_socket(self, address_holder))
    }
}
