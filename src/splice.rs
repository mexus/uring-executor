use std::{future::Future, os::unix::prelude::AsRawFd, task::Poll};

use io_uring::{opcode, squeue, types};

/// Moves  data  between  two  file descriptors without copying between kernel
/// address space and user address space.
///
/// # Note
///
/// The resulting [SpliceFuture] will terminate with an error if at least one of
/// the descriptors is not a pipe.
pub fn splice<Source, Destination>(from: &Source, to: &Destination, length: u32) -> SplicePrepared
where
    Source: AsRawFd,
    Destination: AsRawFd,
{
    let entry = opcode::Splice::new(
        types::Fd(from.as_raw_fd()),
        0,
        types::Fd(to.as_raw_fd()),
        0,
        length,
    );
    let token = crate::with_shared(|shared| {
        shared.pending_events.lock().insert(crate::PendingEvent {
            data: crate::AssociatedData::NoData,
            waker: None,
        })
    });
    let entry = entry.build().user_data(token as u64);
    SplicePrepared { token, entry }
}

/// A prepared "splice" command.
pub struct SplicePrepared {
    token: usize,
    entry: squeue::Entry,
}

impl SplicePrepared {
    /// Converts the [SplicePrepared] into a [Future].
    pub fn into_future(self) -> SpliceFuture {
        SpliceFuture {
            state: State::ToRegister {
                token: self.token,
                entry: self.entry,
            },
        }
    }
}

/// A future that resolves when the "splice" operation completes.
pub struct SpliceFuture {
    state: State,
}

enum State {
    ToRegister { token: usize, entry: squeue::Entry },
    Waiting { token: usize },
    Done,
}

impl Future for SpliceFuture {
    type Output = std::io::Result<usize>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Self::Output> {
        loop {
            match std::mem::replace(&mut self.state, State::Done) {
                State::ToRegister { token, entry } => {
                    let result = crate::with_shared(|shared| {
                        // Safety: the data buffer is already stored in the
                        // `Shared::pending_events`, the `token` is a kind of a
                        // proof.
                        unsafe { shared.try_add_event(&entry, cx) }
                    });
                    if result {
                        self.state = State::Waiting { token };
                    } else {
                        self.state = State::ToRegister { token, entry };
                        break Poll::Pending;
                    }
                }
                State::Waiting { token } => {
                    break if let Some((_no_data, result)) =
                        crate::with_shared(|shared| shared.take_token(cx, token))
                    {
                        Poll::Ready(result)
                    } else {
                        self.state = State::Waiting { token };
                        Poll::Pending
                    };
                }
                State::Done => panic!("Polled after completion"),
            }
        }
    }
}
