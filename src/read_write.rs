//! I/O utilities.

use std::{
    future::Future, net::TcpStream, os::unix::prelude::AsRawFd, slice::SliceIndex, task::Poll,
};

use io_uring::{opcode, types};

use crate::{with_shared, AssociatedData, Buffer, BufferMut, PendingEvent};

/// A prepared "read" or "write" event.
pub struct PreparedIo<B: Buffer> {
    mode: PreparedMode<B>,
}

enum PreparedMode<B: Buffer> {
    Real {
        entry: io_uring::squeue::Entry,
        token: usize,
    },
    NoOp {
        buffer: B,
    },
}

impl<B: Buffer> PreparedIo<B> {
    /// Converts a [PreparedIo] into a [Future].
    pub fn into_future(self) -> IoFuture<B> {
        match self.mode {
            PreparedMode::Real { entry, token } => IoFuture {
                state: State::ToRegister { entry, token },
            },
            PreparedMode::NoOp { buffer } => IoFuture {
                state: State::NoOp { buffer },
            },
        }
    }
}

/// A future for read/write operations.
pub struct IoFuture<B: Buffer> {
    state: State<B>,
}

enum State<B: Buffer> {
    ToRegister {
        token: usize,
        entry: io_uring::squeue::Entry,
    },
    Waiting {
        token: usize,
    },
    NoOp {
        buffer: B,
    },
    Done,
}

impl<B: Buffer> IoFuture<B> {
    fn poll_unpin(&mut self, cx: &mut std::task::Context<'_>) -> Poll<<Self as Future>::Output>
    where
        B: Unpin,
    {
        loop {
            match std::mem::replace(&mut self.state, State::Done) {
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
                    break if let Some((buffer_parts, result)) =
                        with_shared(|shared| shared.read_write_ready(cx, token))
                    {
                        // Safety: the buffer is reconstructed from its own parts.
                        let buffer = unsafe { B::reconstruct(buffer_parts) };
                        Poll::Ready((result, buffer))
                    } else {
                        self.state = State::Waiting { token };
                        Poll::Pending
                    };
                }
                State::NoOp { buffer } => break Poll::Ready((Ok(0), buffer)),
                State::Done => panic!("Polled after completion"),
            }
        }
    }
}

impl<B: Buffer + Unpin> Future for IoFuture<B> {
    type Output = (std::io::Result<usize>, B);

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Self::Output> {
        self.poll_unpin(cx)
    }
}

/// An extension trait for [TcpStream].
pub trait StreamExt {
    /// Creates a future that will write some bytes to the [TcpStream] from the
    /// provided buffer.
    ///
    /// # Notes
    ///
    /// 1. No more than first [u32::MAX] bytes of the buffer (after `range`
    ///    applied) will be sent.
    /// 2. If applying the `range` results in an empty slice, the returned
    ///    future will be immediately ready.
    /// 3. See the `Panics` sections.
    ///
    /// # Panics
    ///
    /// If the `range` is out of bounds of the provided buffer, the function
    /// will panic.
    fn async_write<B, R>(&self, buffer: B, range: R) -> PreparedIo<B>
    where
        B: Buffer,
        R: SliceIndex<[u8], Output = [u8]>;

    /// Creates a future that will read some bytes from the [TcpStream] into the
    /// provided buffer.
    ///
    /// # Notes
    ///
    /// 1. No more than first [u32::MAX] bytes of the buffer (after `range`
    ///    applied) will be filled.
    /// 2. If applying the `range` results in an empty slice, the returned
    ///    future will be immediately ready.
    /// 3. See the `Panics` sections.
    ///
    /// # Panics
    ///
    /// If the `range` is out of bounds of the provided buffer, the function
    /// will panic.
    fn async_read<B, R>(&self, buffer: B, range: R) -> PreparedIo<B>
    where
        B: BufferMut,
        R: SliceIndex<[u8], Output = [u8]>;
}

impl StreamExt for TcpStream {
    fn async_write<B, R>(&self, buffer: B, range: R) -> PreparedIo<B>
    where
        B: Buffer,
        R: SliceIndex<[u8], Output = [u8]>,
    {
        let slice = match buffer.slice().get(range) {
            Some(slice) => slice,
            None => panic!("Range out of bounds"),
        };
        if slice.is_empty() {
            return PreparedIo {
                mode: PreparedMode::NoOp { buffer },
            };
        }

        let entry = opcode::Write::new(
            types::Fd(self.as_raw_fd()),
            slice.as_ptr(),
            u32::try_from(slice.len()).unwrap_or(u32::MAX),
        )
        .build();

        let buffer_parts = buffer.split_into_raw_parts();
        let event = PendingEvent {
            data: AssociatedData::Buffer(buffer_parts),
            waker: None,
        };

        let token = crate::with_shared(|shared| shared.pending_events.lock().insert(event));

        let entry = entry.user_data(token as u64);

        PreparedIo {
            mode: PreparedMode::Real { token, entry },
        }
    }

    fn async_read<B, R>(&self, mut buffer: B, range: R) -> PreparedIo<B>
    where
        B: BufferMut,
        R: SliceIndex<[u8], Output = [u8]>,
    {
        let slice = match buffer.slice_mut().get_mut(range) {
            Some(slice) => slice,
            None => panic!("Range out of bounds"),
        };
        if slice.is_empty() {
            return PreparedIo {
                mode: PreparedMode::NoOp { buffer },
            };
        }

        let entry = opcode::Read::new(
            types::Fd(self.as_raw_fd()),
            slice.as_mut_ptr(),
            u32::try_from(slice.len()).unwrap_or(u32::MAX),
        )
        .build();

        let buffer_parts = buffer.split_into_raw_parts();
        let event = PendingEvent {
            data: AssociatedData::Buffer(buffer_parts),
            waker: None,
        };

        let token = crate::with_shared(|shared| shared.pending_events.lock().insert(event));

        let entry = entry.user_data(token as u64);

        PreparedIo {
            mode: PreparedMode::Real { token, entry },
        }
    }
}
