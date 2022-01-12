//! Experimental io_uring-driven asynchronous runtime.

#![deny(missing_docs)]
#![deny(unsafe_op_in_unsafe_fn)]
#![deny(rustdoc::all)]

use std::{
    cell::RefCell,
    collections::{HashMap, VecDeque},
    future::Future,
    net::TcpStream,
    os::unix::prelude::FromRawFd,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicU64},
        Arc,
    },
    task::{Context, Poll, Wake},
    thread::JoinHandle,
};

use address::{Initialized, Uninitialized};
use buffers::BufferRawParts;
use crossbeam_deque::Injector;
use io_uring::IoUring;
use once_cell::{sync::OnceCell, unsync::Lazy};
use parking_lot::Mutex;
use parking_tools::ParkingManager;
use slab::Slab;

mod accept;
pub mod address;
pub mod buffers;
mod executor;
mod parking_tools;
mod read_write;
mod splice;
mod uring_wrapper;

pub use accept::{AcceptFuture, AcceptPrepared, ListenerExt};
pub use address::SocketAddress;
pub use buffers::{Buffer, BufferMut};
pub use read_write::{IoFuture, StreamExt};
pub use splice::{splice, SpliceFuture, SplicePrepared};

use crate::executor::SpawnedExecutor;

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
    shared: Arc<Shared>,
}

struct Shared {
    pending_events: Mutex<Slab<PendingEvent>>,
    ready_events: Mutex<HashMap<usize, std::io::Result<usize>>>,
    wait_for_slot: Mutex<VecDeque<std::task::Waker>>,
    ring: uring_wrapper::Uring,
    tasks_injector: Arc<Injector<Task>>,
    executor_handles: OnceCell<ParkingManager>,
}

struct PendingEvent {
    data: AssociatedData,
    waker: Option<std::task::Waker>,
}

enum AssociatedData {
    Buffer(BufferRawParts),
    Address(SocketAddress<Uninitialized>),
    NoData,
}

impl Runtime {
    /// Creates a new io_uring runtime, using the provided [IoUring] as a
    /// backend.
    pub fn new(ring: IoUring) -> Self {
        const WORKERS: usize = 5;

        let injector = Arc::new(Injector::new());
        let shared = Arc::new(Shared {
            pending_events: Mutex::default(),
            ready_events: Mutex::default(),
            wait_for_slot: Mutex::default(),
            ring: uring_wrapper::Uring::new(ring),
            tasks_injector: injector.clone(),
            executor_handles: OnceCell::new(),
        });

        let termination = Arc::new(AtomicBool::new(false));

        let mut join_handles = Vec::with_capacity(WORKERS);
        let mut stealers = Vec::with_capacity(WORKERS);
        let mut stealers_providers = Vec::with_capacity(WORKERS);

        for executor_id in 0..WORKERS {
            let SpawnedExecutor {
                join_handle,
                stealer,
                stealers_provider,
            } = crate::executor::spawn_executor(
                executor_id,
                shared.clone(),
                injector.clone(),
                termination.clone(),
            );
            join_handles.push(join_handle);
            stealers.push(stealer);
            stealers_providers.push(stealers_provider);
        }
        shared
            .executor_handles
            .set(ParkingManager::new(join_handles))
            .expect("This is the only time when the handles are set");

        for stealers_provider in stealers_providers {
            stealers
                .iter()
                .cloned()
                .try_for_each(|stealer| stealers_provider.send(stealer))
                .expect("Some thread already panicked? O_o");
        }

        Self { shared }
    }

    /// Sets up the execution context and runs the provided future to the
    /// completion.
    pub fn block_on<F>(&mut self, mut future: F) -> F::Output
    where
        F: Future,
    {
        let original_waker = Arc::new(UringWaker {
            awakened: AtomicBool::new(true),
        });
        let waker = original_waker.clone().into();
        let mut context = Context::from_waker(&waker);

        SHARED.with(|shared| {
            *shared.borrow_mut() = Some(self.shared.clone());
        });

        // Pin down the future. Safety: since we shadow the original `future`,
        // it can't be accessed directly anymore.
        let mut future = unsafe { Pin::new_unchecked(&mut future) };
        let ready = loop {
            // If the "main" future has been awakened, disarm the flag and poll
            // it!
            if original_waker
                .awakened
                .swap(false, std::sync::atomic::Ordering::Relaxed)
            {
                match future.as_mut().poll(&mut context) {
                    Poll::Ready(ready) => break ready,
                    Poll::Pending => { /* Continue the cycle */ }
                };
            }

            // If the freshly polled "main" future woken up itself, we shouldn't
            // block in the "enter" syscall (submit-and-wait). So we "want" zero
            // events when the `awakened` flag is true and at least one event
            // when the flag is `false`. Since `false` -> `0` and `true` -> `1`,
            // we need to inverse the flag before converting it to `usize`.
            let want_events = usize::from(
                !original_waker
                    .awakened
                    .load(std::sync::atomic::Ordering::Relaxed),
            );

            let ring = &self.shared.ring;
            if want_events == 0 && ring.submission_is_empty() {
                // No need to fall into a syscall if we don't wait for any
                // events and the submission queue is empty.
            } else {
                ring.submit_and_wait(want_events)
                    .expect("Unexpected uring I/O error");
            }

            if !ring.submission_is_full() {
                // Let's wake the futures which are waiting for a slot in the
                // submission queue.
                while let Some(waker) = self.shared.wait_for_slot.lock().pop_front() {
                    waker.wake()
                }
            }

            // Drain the completion queue.
            for completed in ring.completed_entries() {
                let result = completion_entry_result(&completed);
                let token = completed.user_data() as usize;
                let previous = self.shared.ready_events.lock().insert(token, result);
                if previous.is_some() {
                    // This actually means a bug in the runtime implementation.
                    log::error!(
                        "Result of an event with the token {} has been overwritten!!",
                        token
                    );
                }
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
        if let Some(result) = self.ready_events.lock().remove(&token) {
            let PendingEvent { data, .. } = self
                .pending_events
                .lock()
                .try_remove(token)
                .expect("Where did it go?");
            Some((data, result))
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

        // Safety: the buffer is stored properly.
        if unsafe { ring.submission_push(entry) }.is_ok() {
            true
        } else {
            // The submission queue is full!
            let waker = cx.waker().clone();
            self.wait_for_slot.lock().push_back(waker);
            false
        }
    }

    fn spawn(&self, task: Task) {
        self.tasks_injector.push(task);

        // After pushing a task to the tasks injector, wake up a parker executor (if any).
        if let Some((batch_id, executor_id)) = self
            .executors_park_bitmap
            .iter()
            .enumerate()
            .map(|(batch_id, bitmap)| (batch_id, bitmap.load(std::sync::atomic::Ordering::SeqCst)))
            .find_map(|(batch_id, bitmap)| {
                if bitmap == 0 {
                    // No "true" bits => no executor in the batch is parked.
                    return None;
                }
                let position_of_true = bitmap.trailing_zeros();
                Some((batch_id, position_of_true))
            })
        {
            // Found a parked executor.
            let executor_id = batch_id * 64 + (executor_id as usize);
            log::debug!("Unpark executor {}", executor_id);
            self.executor_handles.get().expect("Must be set up")[executor_id]
                .thread()
                .unpark();
        }
    }
}

struct UringWaker {
    awakened: AtomicBool,
}

impl Wake for UringWaker {
    fn wake(self: Arc<Self>) {
        self.awakened
            .store(true, std::sync::atomic::Ordering::Relaxed)
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.awakened
            .store(true, std::sync::atomic::Ordering::Relaxed)
    }
}

impl Drop for Shared {
    fn drop(&mut self) {
        if self.pending_events.lock().is_empty() {
            return;
        }
        // Ensure that there are no pending requests in the submission queue
        // which reference the buffers we are going to drop when `self.events`
        // is dropped.
        let ring = &self.ring;
        let pending_events = ring.submission_len();

        if let Err(e) = ring.submit_and_wait(pending_events) {
            log::error!("Submit-and-wait terminated with an error: {:#}", e);
            return;
        };
        ring.completed_entries().for_each(drop);
    }
}

fn completion_entry_result(entry: &io_uring::cqueue::Entry) -> std::io::Result<usize> {
    let raw_result = entry.result();
    // uring encodes error codes as negative value of the result, so if we try
    // to convert the result to an unsigned integer and the conversion fails, it
    // means en error has been encountered.
    usize::try_from(raw_result).map_err(|_| std::io::Error::from_raw_os_error(-raw_result))
}

/// A "task" to execute.
pub type Task = Pin<Box<dyn Future<Output = ()> + Send>>;

/// Spawns a task onto an executor.
pub fn spawn(task: Task) {
    with_shared(|shared| shared.spawn(task))
}
