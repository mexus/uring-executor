use std::{future::Future, marker::PhantomData, net::TcpStream, slice::SliceIndex, task::Poll};

use crate::{with_shared, Buffer, BufferMut};

/// A future for read/write operations.
pub struct IoFuture<B: Buffer> {
    inner: IoFutureInner<B>,
    _pd: PhantomData<B>,
}

impl<B: Buffer> IoFuture<B> {
    pub(crate) fn with_token(token: usize) -> Self {
        Self {
            inner: IoFutureInner::Waiting { token },
            _pd: PhantomData,
        }
    }

    pub(crate) fn noop(buffer: B) -> Self {
        Self {
            inner: IoFutureInner::NoOp { buffer },
            _pd: PhantomData,
        }
    }
}

enum IoFutureInner<B: Buffer> {
    Waiting { token: usize },
    NoOp { buffer: B },
    Done,
}

impl<B: Buffer + Unpin> Future for IoFuture<B> {
    type Output = (std::io::Result<usize>, B);

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Self::Output> {
        let token = match std::mem::replace(&mut self.inner, IoFutureInner::Done) {
            IoFutureInner::Waiting { token } => token,
            IoFutureInner::NoOp { buffer } => return Poll::Ready((Ok(0), buffer)),
            IoFutureInner::Done => panic!("polled after completion"),
        };
        if let Some((buffer_parts, result)) =
            with_shared(|shared| shared.read_write_ready(cx, token))
        {
            // Safety: the buffer is reconstructed from its own parts.
            let buffer = unsafe { B::reconstruct(buffer_parts) };
            Poll::Ready((result, buffer))
        } else {
            self.inner = IoFutureInner::Waiting { token };
            Poll::Pending
        }
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
    fn async_write<B, R>(&self, buffer: B, range: R) -> IoFuture<B>
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
    fn async_read<B, R>(&self, buffer: B, range: R) -> IoFuture<B>
    where
        B: BufferMut,
        R: SliceIndex<[u8], Output = [u8]>;
}

impl StreamExt for TcpStream {
    fn async_write<B, R>(&self, buffer: B, range: R) -> IoFuture<B>
    where
        B: Buffer,
        R: SliceIndex<[u8], Output = [u8]>,
    {
        crate::with_shared(|shared| shared.socket_write(self, buffer, range))
    }

    fn async_read<B, R>(&self, buffer: B, range: R) -> IoFuture<B>
    where
        B: BufferMut,
        R: SliceIndex<[u8], Output = [u8]>,
    {
        crate::with_shared(|shared| shared.socket_read(self, buffer, range))
    }
}
