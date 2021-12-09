//! Experimental io_uring-driven asynchronous runtime.

#![deny(missing_docs)]
#![deny(unsafe_op_in_unsafe_fn)]

use std::{
    cell::RefCell,
    collections::{HashMap, VecDeque},
    future::Future,
    net::TcpStream,
    os::unix::prelude::FromRawFd,
    pin::Pin,
    sync::{atomic::AtomicBool, Arc},
    task::{Context, Poll, Wake},
};

use address::{Initialized, Uninitialized};
use buffers::BufferRawParts;
use io_uring::IoUring;
use once_cell::unsync::Lazy;
use parking_lot::Mutex;
use slab::Slab;

mod accept;
pub mod address;
pub mod buffers;
mod read_write;

pub use accept::{AcceptFuture, AcceptPrepared, ListenerExt};
pub use address::SocketAddress;
pub use buffers::{Buffer, BufferMut};
pub use read_write::{IoFuture, StreamExt};

thread_local! {
    static SHARED: Lazy<RefCell<Option<Arc<Shared>>>> = Lazy::new(|| RefCell::new(None));
}

fn with_shared<F, R>(f: F) -> R
where
    F: FnOnce(&Arc<Shared>) -> R,
{
    SHARED.with(|maybe_shared| f(maybe_shared.borrow().as_ref().expect("Executor is not set")))
}

/// An io_uring-driven asynchronous runtime.
pub struct Runtime {
    // ring: IoUring,
    shared: Arc<Shared>,
}

struct Shared {
    pending_events: Mutex<Slab<PendingEvent>>,
    ready_events: Mutex<HashMap<usize, EventResult>>,

    wait_for_slot: Mutex<VecDeque<std::task::Waker>>,

    submission_queue_lock: Mutex<()>,

    ring: IoUring,
}

enum EventResult {
    IoEvent(std::io::Result<usize>),
}

struct PendingEvent {
    data: AssociatedData,
    waker: Option<std::task::Waker>,
}

enum AssociatedData {
    Buffer(BufferRawParts),
    Address(SocketAddress<Uninitialized>),
}

impl Runtime {
    /// Creates a new io_uring runtime, using the provided [IoUring] as a
    /// backend.
    pub fn new(ring: IoUring) -> Self {
        Self {
            shared: Arc::new(Shared {
                pending_events: Mutex::default(),
                ready_events: Mutex::default(),
                ring,
                wait_for_slot: Mutex::default(),
                submission_queue_lock: Mutex::default(),
            }),
        }
    }

    /// Sets up the execution context and runs the provided future to the
    /// completion.
    pub fn block_on<F>(&mut self, mut future: F) -> F::Output
    where
        F: Future,
    {
        let original_waker = Arc::new(UringWaker(AtomicBool::new(true)));
        let waker = original_waker.clone().into();
        let mut context = Context::from_waker(&waker);

        SHARED.with(|shared| {
            *shared.borrow_mut() = Some(self.shared.clone());
        });

        // Pin down the future. Since we shadow the original `future`, it can't
        // be accessed directly anymore.
        let mut future = unsafe { Pin::new_unchecked(&mut future) };
        let ready = loop {
            if original_waker
                .0
                .swap(false, std::sync::atomic::Ordering::Relaxed)
            {
                match future.as_mut().poll(&mut context) {
                    Poll::Ready(ready) => break ready,
                    Poll::Pending => { /* Continue the cycle */ }
                };
            }

            let awaken = original_waker.0.load(std::sync::atomic::Ordering::Relaxed);

            let ring = &self.shared.ring;

            ring.submit_and_wait(if awaken { 0 } else { 1 })
                .expect("Unexpected uring I/O error");

            let queue_is_full = {
                let _guard = self.shared.submission_queue_lock.lock();
                // Safety: the guard has been acquired.
                unsafe { ring.submission_shared() }.is_full()
            };
            if !queue_is_full {
                // Let's wake the futures, since the submission queue has some space.
                while let Some(waker) = self.shared.wait_for_slot.lock().pop_front() {
                    waker.wake()
                }
            }

            // Safety: this is the only place (apart from the `Drop`
            // implementation) where completion queue is accessed.
            for completed in unsafe { ring.completion_shared() } {
                let result = completed.result();
                let result =
                    usize::try_from(result).map_err(|_| std::io::Error::from_raw_os_error(-result));
                let token = completed.user_data() as usize;
                self.shared
                    .ready_events
                    .lock()
                    .insert(token, EventResult::IoEvent(result));
                if let Some(waker) = self.shared.pending_events.lock()[token].waker.take() {
                    waker.wake()
                }
            }
        };

        SHARED.with(|shared| {
            *shared.borrow_mut() = None;
        });

        ready
    }
}

type AcceptResult =
    Result<(TcpStream, SocketAddress<Initialized>), (std::io::Error, SocketAddress<Uninitialized>)>;

impl Shared {
    fn take_token(
        &self,
        cx: &mut Context<'_>,
        token: usize,
    ) -> Option<(AssociatedData, std::io::Result<usize>)> {
        if let Some(event) = self.ready_events.lock().remove(&token) {
            let PendingEvent { data, .. } = self
                .pending_events
                .lock()
                .try_remove(token)
                .expect("Where did it go?");
            match event {
                EventResult::IoEvent(result) => Some((data, result)),
            }
        } else {
            // Not ready yet.
            self.pending_events.lock()[token].waker = Some(cx.waker().clone());
            None
        }
    }

    fn read_write_ready(
        &self,
        cx: &mut Context<'_>,
        token: usize,
    ) -> Option<(BufferRawParts, std::io::Result<usize>)> {
        let (data, result) = self.take_token(cx, token)?;

        let buffer_parts = match data {
            AssociatedData::Buffer(buffer_parts) => buffer_parts,
            _ => unreachable!(),
        };

        Some((buffer_parts, result))
    }

    fn connection_ready(&self, cx: &mut Context<'_>, token: usize) -> Option<AcceptResult> {
        let (data, result) = self.take_token(cx, token)?;
        let address = match data {
            AssociatedData::Address(address) => address,
            _ => unreachable!(),
        };
        Some(match result {
            Ok(fd) => {
                // Safety: `accept` returns a file descriptor of the accepted
                // connection.
                let stream = unsafe { TcpStream::from_raw_fd(fd as _) };
                // Safety: `accept` assigns correct address.
                let address = unsafe { address.assume_init() };
                Ok((stream, address))
            }
            Err(error) => Err((error, address)),
        })
    }
}

impl Shared {
    /// Tries to add an event to the submission queue.
    ///
    /// # Safety
    ///
    /// The buffer associated with the [`entry`] must be stored in the
    /// [Self::pending_events].
    unsafe fn try_add_event(
        &self,
        entry: &io_uring::squeue::Entry,
        cx: &mut std::task::Context<'_>,
    ) -> bool {
        let ring = &self.ring;

        let result = {
            let _queue_guard = self.submission_queue_lock.lock();
            // Safety: the guard for "submission queue" has been acquired.
            let mut queue = unsafe { ring.submission_shared() };
            // Safety: the buffer is stored properly.
            unsafe { queue.push(entry) }
        };
        if result.is_ok() {
            true
        } else {
            // The submission queue is full!
            let waker = cx.waker().clone();
            self.wait_for_slot.lock().push_back(waker);
            false
        }
    }
}

struct UringWaker(AtomicBool);

impl Wake for UringWaker {
    fn wake(self: Arc<Self>) {
        self.0.store(true, std::sync::atomic::Ordering::Relaxed)
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.store(true, std::sync::atomic::Ordering::Relaxed)
    }
}

impl Drop for Runtime {
    fn drop(&mut self) {
        if self.shared.pending_events.lock().is_empty() {
            return;
        }
        // Ensure that there are no pending requests in the queues which
        // reference the buffers we are going to drop when `self.events` is
        // dropped.
        let ring = &self.shared.ring;
        let pending_events = {
            let _guard = self.shared.submission_queue_lock.lock();
            // Safety: the guard for "submission queue" has been acquired.
            unsafe { ring.submission_shared() }.len()
        };

        if let Err(_e) = ring.submit_and_wait(pending_events) {
            // TODO: log the error
            return;
        };
        // Safety: since we've got here, it means the main loop has been
        // terminated and we are the only possible user of the completion queue.
        unsafe { ring.completion_shared() }.for_each(drop);
    }
}
