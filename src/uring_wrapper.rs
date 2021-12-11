use io_uring::{CompletionQueue, IoUring, SubmissionQueue};
use parking_lot::{Mutex, MutexGuard};

struct CompletionMarker;
struct SubmissionMarker;

/// A wrapper to safely access some [IoUring]'s methods.
pub struct Uring {
    inner: IoUring,
    submission_queue_lock: Mutex<SubmissionMarker>,
    completion_queue_lock: Mutex<CompletionMarker>,
}

impl Uring {
    /// Creates a wrapper.
    pub fn new(uring: IoUring) -> Self {
        Self {
            inner: uring,
            submission_queue_lock: Mutex::new(SubmissionMarker),
            completion_queue_lock: Mutex::new(CompletionMarker),
        }
    }

    /// Calls [IoUring::submit_and_wait].
    pub fn submit_and_wait(&self, want: usize) -> std::io::Result<usize> {
        self.inner.submit_and_wait(want)
    }

    /// Obtains an internal lock for the completion queue and returns an
    /// iterator over completed entries.
    pub fn completed_entries(&self) -> CompletedEntires<'_> {
        let guard = self.completion_queue_lock.lock();
        // Safety: the access is guarded.
        let queue = unsafe { self.inner.completion_shared() };
        CompletedEntires {
            _guard: guard,
            inner: queue,
        }
    }

    /// Obtains an internal lock for the submission queue and returns a guarded
    /// [SubmissionQueue].
    fn submission_queue(&self) -> GuardedSubmission<'_> {
        let guard = self.submission_queue_lock.lock();
        // Safety: the access is guarded.
        let queue = unsafe { self.inner.submission_shared() };
        GuardedSubmission {
            _guard: guard,
            inner: queue,
        }
    }

    // /// Returns the amount of free entries of the submission queue.
    // pub fn submission_remaining(&self) -> usize {
    //     let queue = self.submission_queue();
    //     let capacity = queue.inner.capacity();
    //     let length = queue.inner.len();
    //     capacity - length
    // }

    /// Returns `true` if the submission queue ring buffer has reached capacity, and no more events
    /// can be added before the kernel consumes some.
    pub fn submission_is_full(&self) -> bool {
        self.submission_queue().inner.is_full()
    }

    /// Get the number of submission queue events in the ring buffer.
    pub fn submission_len(&self) -> usize {
        self.submission_queue().inner.len()
    }

    /// Returns whether the submission queue is empty or not.
    pub fn submission_is_empty(&self) -> bool {
        self.submission_queue().inner.is_empty()
    }

    /// Tries to push an entry to the submission queue.
    ///
    /// # Safety
    ///
    /// Developers must ensure that parameters of the
    /// [`Entry`][io_uring::squeue::Entry] (such as buffer) are valid and will
    /// be valid for the entire duration of the operation, otherwise it may
    /// cause memory problems.
    pub unsafe fn submission_push(
        &self,
        entry: &io_uring::squeue::Entry,
    ) -> Result<(), io_uring::squeue::PushError> {
        let mut queue = self.submission_queue();
        // Safety is uphold by the called.
        unsafe { queue.inner.push(entry) }
    }
}

/// A guarded [SubmissionQueue].
struct GuardedSubmission<'a> {
    _guard: MutexGuard<'a, SubmissionMarker>,
    inner: SubmissionQueue<'a>,
}

/// An iterator over completed entries from the [CompletionQueue].
pub struct CompletedEntires<'a> {
    _guard: MutexGuard<'a, CompletionMarker>,
    inner: CompletionQueue<'a>,
}

impl<'a> Iterator for CompletedEntires<'a> {
    type Item = io_uring::cqueue::Entry;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}
