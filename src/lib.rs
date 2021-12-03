//! Experimental io_uring-driven asynchronous runtime.

#![deny(missing_docs)]
#![deny(unsafe_op_in_unsafe_fn)]

use std::{
    cell::RefCell,
    collections::{HashMap, VecDeque},
    future::Future,
    net::{TcpListener, TcpStream},
    os::unix::prelude::{AsRawFd, FromRawFd},
    pin::Pin,
    slice::SliceIndex,
    sync::{atomic::AtomicBool, Arc},
    task::{Context, Poll, Wake},
};

use address::{Initialized, Uninitialized};
use buffers::BufferRawParts;
use io_uring::{opcode, types, IoUring};
use once_cell::unsync::Lazy;
use parking_lot::Mutex;
use slab::Slab;

mod accept;
pub mod address;
pub mod buffers;
mod read_write;

pub use accept::{AcceptFuture, ListenerExt};
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

/// A detached handle that can be used to create futures.
pub struct Handle {
    inner: Arc<Shared>,
}

impl Handle {
    /// Creates an execution handle.
    ///
    /// # Panics
    ///
    /// Panics when executed out of the runtime's context.
    pub fn out_of_thin_air() -> Self {
        let shared = with_shared(Arc::clone);
        Self { inner: shared }
    }
}

/// An io_uring-driven asynchronous runtime.
pub struct Runtime {
    // ring: IoUring,
    shared: Arc<Shared>,
}

struct Shared {
    /// Pending to submit events.
    to_submit: Mutex<VecDeque<io_uring::squeue::Entry>>,

    pending_events: Mutex<Slab<PendingEvent>>,
    ready_events: Mutex<HashMap<usize, EventResult>>,

    ring: Mutex<IoUring>,
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
                to_submit: Mutex::default(),
                ring: Mutex::new(ring),
            }),
        }
    }

    /// Creates a handle to spawn the futures outside of the main execution
    /// context.
    pub fn handle(&self) -> Handle {
        Handle {
            inner: self.shared.clone(),
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

            // The future might have registered some events. Add them to the
            // queue!
            let submitted_everything = self.submit_pending();

            let awaken = original_waker.0.load(std::sync::atomic::Ordering::Relaxed);
            let consumed_entires;
            {
                let mut ring = self.shared.ring.lock();
                {
                    let mut to_submit = self.shared.to_submit.lock();
                    while let Some(entry) = to_submit.pop_front() {
                        if unsafe { ring.submission().push(&entry) }.is_err() {
                            to_submit.push_front(entry);
                            break;
                        }
                    }
                }

                consumed_entires = ring
                    .submit_and_wait(if awaken { 0 } else { 1 })
                    .expect("Unexpected uring I/O error");

                for completed in ring.completion() {
                    let result = completed.result();
                    let result = usize::try_from(result)
                        .map_err(|_| std::io::Error::from_raw_os_error(-result));
                    let token = completed.user_data() as usize;
                    self.shared
                        .ready_events
                        .lock()
                        .insert(token, EventResult::IoEvent(result));
                    if let Some(waker) = self.shared.pending_events.lock()[token].waker.take() {
                        waker.wake()
                    }
                }
            }
            if consumed_entires != 0 && !submitted_everything {
                // If we have something to submit (!submitted_everything) and
                // the "submit_and_wait" has consumed some entries
                // (consumed_entires != 0), we've got a chance to submit pending
                // entries.
                self.submit_pending();
            }
        };

        SHARED.with(|shared| {
            *shared.borrow_mut() = None;
        });

        ready
    }

    fn submit_pending(&mut self) -> bool {
        let mut to_submit = self.shared.to_submit.lock();
        let mut ring = self.shared.ring.lock();
        while let Some(entry) = to_submit.pop_front() {
            if unsafe { ring.submission().push(&entry) }.is_err() {
                to_submit.push_front(entry);
                break;
            }
        }
        to_submit.is_empty()
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
    pub fn socket_write<B, R>(&self, socket: &TcpStream, buffer: B, range: R) -> IoFuture<B>
    where
        B: Buffer,
        R: SliceIndex<[u8], Output = [u8]>,
    {
        let slice = match buffer.slice().get(range) {
            Some(slice) => slice,
            None => panic!("Range out of bounds"),
        };
        if slice.is_empty() {
            return IoFuture::noop(buffer);
        }

        let entry = opcode::Write::new(
            types::Fd(socket.as_raw_fd()),
            slice.as_ptr(),
            u32::try_from(slice.len()).unwrap_or(u32::MAX),
        )
        .build();

        let buffer_parts = buffer.split_into_raw_parts();
        let event = PendingEvent {
            data: AssociatedData::Buffer(buffer_parts),
            waker: None,
        };

        // Safety: event owns the data pointed by the entry.
        let event = unsafe {AugmentedSubmitEntry::new(entry, event)};
        let token =  self.add_event(event);
        IoFuture::with_token(token)
    }

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
    pub fn socket_read<B, R>(&self, socket: &TcpStream, mut buffer: B, range: R) -> IoFuture<B>
    where
        B: BufferMut,
        R: SliceIndex<[u8], Output = [u8]>,
    {
        let slice = match buffer.slice_mut().get_mut(range) {
            Some(slice) => slice,
            None => panic!("Range out of bounds"),
        };
        if slice.is_empty() {
            return IoFuture::noop(buffer);
        }

        let entry = opcode::Read::new(
            types::Fd(socket.as_raw_fd()),
            slice.as_mut_ptr(),
            u32::try_from(slice.len()).unwrap_or(u32::MAX),
        )
        .build();

        let buffer_parts = buffer.split_into_raw_parts();
        let event = PendingEvent {
            data: AssociatedData::Buffer(buffer_parts),
            waker: None,
        };

        // Safety: event owns the data pointed by the entry.
        let event = unsafe {AugmentedSubmitEntry::new(entry, event)};
        let token =  self.add_event(event);

        IoFuture::with_token(token)
    }

    /// Creates a future that will resolve when a new connection arrives.
    pub fn accept_socket<Marker>(
        &self,
        socket: &TcpListener,
        address: SocketAddress<Marker>,
    ) -> AcceptFuture {
        let address = address.reset();

        let entry = opcode::Accept::new(
            types::Fd(socket.as_raw_fd()),
            address.socket_address.as_ptr(),
            address.address_length.as_ptr(),
        )
        .build();

        let event = PendingEvent {
            data: AssociatedData::Address(address.into_uninit()),
            waker: None,
        };

        // Safety: event owns the data pointed by the entry.
        let event = unsafe {AugmentedSubmitEntry::new(entry, event)};
        let token =  self.add_event(event);

        AcceptFuture { token: Some(token) }
    }

    /// # Safety
    ///
    /// The `event` must own the data which is pointed by the `entry`.
    fn add_event(&self, AugmentedSubmitEntry { entry, event }: AugmentedSubmitEntry) -> usize {
        let mut pending_events = self.pending_events.lock();
        let vacant = pending_events.vacant_entry();
        let token = vacant.key();
        vacant.insert(event);
        let entry = entry.user_data(token as u64);
        // When the ring is not locked and submission queue is not full, add the
        // entry directly to the queue. Otherwise fall back to the supplementary
        // `to_submit` queue.
        if let Some(mut ring) = self.ring.try_lock() {
            // Safety: the possible pointers from the `event` are already stored
            // in the `pending_events` storage.
            if unsafe { ring.submission().push(&entry) }.is_ok() {
                return token;
            }
        }
        self.to_submit.lock().push_back(entry);

        token
    }
}

impl Handle {
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
    pub fn socket_write<B, R>(&self, socket: &TcpStream, buffer: B, range: R) -> IoFuture<B>
    where
        B: Buffer,
        R: SliceIndex<[u8], Output = [u8]>,
    {
        self.inner.socket_write(socket, buffer, range)
    }

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
    pub fn socket_read<B, R>(&self, socket: &TcpStream, buffer: B, range: R) -> IoFuture<B>
    where
        B: BufferMut,
        R: SliceIndex<[u8], Output = [u8]>,
    {
        self.inner.socket_read(socket, buffer, range)
    }

    /// Creates a future that will resolve when a new connection arrives.
    pub fn accept_socket<Marker>(
        &self,
        socket: &TcpListener,
        address: SocketAddress<Marker>,
    ) -> AcceptFuture {
        self.inner.accept_socket(socket, address)
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
        let mut ring = self.shared.ring.lock();
        let pending_events = ring.submission().len();
        if let Err(_e) = ring.submit_and_wait(pending_events) {
            // TODO: log the error
            return;
        };
        ring.completion().for_each(drop);
    }
}

/// An entry for the submission queue, augmented with "description" of the entry
/// and an owned data associated with it.
pub(crate) struct AugmentedSubmitEntry {
    entry: io_uring::squeue::Entry,
    event: PendingEvent,
}

impl AugmentedSubmitEntry {
    /// Creates an [AugmentedSubmitEntry] which stores an uring entry and the
    /// data associated with it.
    ///
    /// # Safety
    ///
    /// The `event` must own the data which is pointed by the `entry`.
    pub unsafe fn new(entry: io_uring::squeue::Entry, event: PendingEvent) -> Self {
        Self { entry, event }
    }
}
