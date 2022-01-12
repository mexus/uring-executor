//! Execution of [Future]s.

use std::{
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    task::{Context, Wake, Waker},
    thread::JoinHandle,
};

use crossbeam_deque::{Injector, Stealer, Worker};

use crate::{Shared, Task};

/// A wrapper handle to a thread.
pub(crate) struct ThreadWaker(std::thread::Thread);

impl Wake for ThreadWaker {
    #[inline]
    fn wake(self: Arc<Self>) {
        self.wake_by_ref()
    }

    #[inline]
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark()
    }
}

/// A collection of handles to an executor.
pub struct SpawnedExecutor {
    pub join_handle: JoinHandle<()>,
    pub stealer: Stealer<Task>,
    pub stealers_provider: crossbeam_channel::Sender<Stealer<Task>>,
}

pub(crate) fn spawn_executor(
    executor_id: usize,
    uring: Arc<Shared>,
    global: Arc<Injector<Task>>,
    termination: Arc<AtomicBool>,
) -> SpawnedExecutor {
    let (local_stealer_sender, receiver) = crossbeam_channel::bounded(0);
    let (stealers_provider, remote_stealers) = crossbeam_channel::unbounded::<Stealer<Task>>();

    let join_handle = std::thread::spawn(move || {
        let thread = std::thread::current();
        let waker = Arc::new(ThreadWaker(thread));
        let worker = Worker::new_fifo();
        let stealer = worker.stealer();

        local_stealer_sender.send(stealer).expect("Shouldn't fail");

        // Now wait for other executors stealers.
        let stealers = remote_stealers.into_iter().collect::<Vec<_>>();

        // Finally ready to start!
        execute(
            executor_id,
            uring,
            &worker,
            &global,
            &stealers,
            waker,
            &termination,
        );
    });
    let stealer = receiver.recv().expect("Shouldn't fail");
    SpawnedExecutor {
        join_handle,
        stealer,
        stealers_provider,
    }
}

/// Executes tasks from the shared pool.
fn execute(
    executor_id: usize,
    uring: Arc<Shared>,
    local: &Worker<Task>,
    global: &Injector<Task>,
    stealers: &[Stealer<Task>],
    waker: Arc<ThreadWaker>,
    termination: &AtomicBool,
) {
    let batch_id = executor_id / 64;
    let batch_executor_id = executor_id % 64;
    let parking_mask = 1u64 << batch_executor_id;
    let unpark_mask = u64::MAX - (1u64 << batch_executor_id);

    log::debug!(
        "Executor #{} park mask {:064b}, unpark mask: {:064b}",
        executor_id,
        parking_mask,
        unpark_mask
    );

    crate::SHARED.with(|shared| {
        *shared.borrow_mut() = Some(uring.clone());
    });
    let waker = Waker::from(waker);
    let mut context = Context::from_waker(&waker);
    while !termination.load(Ordering::SeqCst) {
        if let Some(mut task) = find_task(local, global, stealers) {
            // TODO: handle possible panics.
            match Pin::new(&mut task).poll(&mut context) {
                std::task::Poll::Ready(()) => {
                    // The task is ready! Move to the next one, this one is
                    // dropped.

                    // TODO: handle possible panics when the task is dropped.
                }
                std::task::Poll::Pending => {
                    // The task needs to be polled again.
                    local.push(task)
                }
            }
        } else {
            uring.executors_park_bitmap[batch_id].fetch_or(parking_mask, Ordering::SeqCst);
            std::thread::park();
            uring.executors_park_bitmap[batch_id].fetch_and(unpark_mask, Ordering::SeqCst);
        }
    }

    crate::SHARED.with(|shared| *shared.borrow_mut() = None);
}

fn find_task<T>(local: &Worker<T>, global: &Injector<T>, stealers: &[Stealer<T>]) -> Option<T> {
    // Pop a task from the local queue, if not empty.
    local.pop().or_else(|| {
        // Otherwise, we need to look for a task elsewhere.
        std::iter::repeat_with(|| {
            // Try stealing a batch of tasks from the global queue.
            global
                .steal_batch_and_pop(local)
                // Or try stealing a task from one of the other threads.
                .or_else(|| stealers.iter().map(|s| s.steal()).collect())
        })
        // Loop while no task was stolen and any steal operation needs to be retried.
        .find(|s| !s.is_retry())
        // Extract the stolen task, if there is one.
        .and_then(|s| s.success())
    })
}
