//! Accept 2

use std::{
    future::Future,
    net::{TcpListener, TcpStream},
    os::unix::prelude::AsRawFd,
    task::Poll,
};

use io_uring::{opcode, types};

use crate::{
    address::{Initialized, Uninitialized},
    with_shared, AssociatedData, PendingEvent, SocketAddress,
};

/// A prepared "accept".
pub struct AcceptPrepared {
    /// Token for the stored data.
    token: usize,

    /// Entry for a submission queue.
    entry: io_uring::squeue::Entry,
}

impl AcceptPrepared {
    /// Converts the [AcceptPrepared] into a [Future].
    pub fn into_future(self) -> AcceptFuture {
        AcceptFuture {
            state: State::ToRegister {
                token: self.token,
                entry: self.entry,
            },
        }
    }
}

/// A future which resolves into a new incoming connection.
pub struct AcceptFuture {
    state: State,
}

enum State {
    /// The event has not yet been pushed to the submission queue.
    ToRegister {
        token: usize,
        entry: io_uring::squeue::Entry,
    },
    /// The event has been pushed to the submission queue, but hasn't been
    /// received in the completion queue.
    Waiting { token: usize },
    /// The future has completed.
    Ready,
}

impl AcceptFuture {
    fn poll_unpin(&mut self, cx: &mut std::task::Context<'_>) -> Poll<<Self as Future>::Output> {
        loop {
            match std::mem::replace(&mut self.state, State::Ready) {
                State::ToRegister { token, entry } => {
                    let result = with_shared(|shared| {
                        // Safety: the data buffer is already stored in the
                        // `Shared::pending_events`, the `token` is a kind of a
                        // proof.
                        unsafe { shared.try_add_event(&entry, cx) }
                    });
                    if result {
                        self.state = State::Waiting { token }
                    } else {
                        self.state = State::ToRegister { token, entry };
                        break Poll::Pending;
                    }
                }
                State::Waiting { token } => {
                    break match with_shared(|shared| shared.connection_ready(cx, token)) {
                        Some(result) => Poll::Ready(result),
                        None => {
                            self.state = State::Waiting { token };
                            Poll::Pending
                        }
                    }
                }
                State::Ready => panic!("Polled after completion"),
            }
        }
    }
}

impl Future for AcceptFuture {
    type Output = Result<
        (TcpStream, SocketAddress<Initialized>),
        (std::io::Error, SocketAddress<Uninitialized>),
    >;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Self::Output> {
        self.poll_unpin(cx)
    }
}

/// An extension trait for [TcpListener].
pub trait ListenerExt {
    /// Creates a future that will resolve when a new connection arrives.
    fn async_accept<InitializedMarker>(
        &self,
        address_holder: SocketAddress<InitializedMarker>,
    ) -> AcceptPrepared;
}

impl ListenerExt for TcpListener {
    fn async_accept<InitializedMarker>(
        &self,
        address_holder: SocketAddress<InitializedMarker>,
    ) -> AcceptPrepared {
        let address_holder = address_holder.reset();

        let entry = opcode::Accept::new(
            types::Fd(self.as_raw_fd()),
            address_holder.socket_address.as_ptr(),
            address_holder.address_length.as_ptr(),
        )
        .build();

        let event = PendingEvent {
            data: AssociatedData::Address(address_holder.into_uninit()),
            waker: None,
        };

        let token = with_shared(|shared| shared.pending_events.lock().insert(event));

        let entry = entry.user_data(token as u64);

        AcceptPrepared { token, entry }
    }
}
