use std::{sync::atomic::AtomicBool, thread::JoinHandle};

#[derive(Debug)]
struct Thread {
    handle: JoinHandle<()>,
    parked: AtomicBool,
}

#[derive(Debug)]
pub struct ParkingManager {
    threads: Vec<Thread>,
}

impl ParkingManager {
    /// Constructs a parking manager for the given thread handles.
    pub fn new<I>(handles: I) -> Self
    where
        I: IntoIterator<Item = JoinHandle<()>>,
    {
        let threads = handles
            .into_iter()
            .map(|handle| Thread {
                handle,
                parked: AtomicBool::new(false),
            })
            .collect::<Vec<_>>();
        Self { threads }
    }

    /// Unparks the first found parked thread.
    pub fn unpark_any(&self) {
        if let Some(thread) = self
            .threads
            .iter()
            .find(|thread| thread.parked.load(std::sync::atomic::Ordering::SeqCst))
        {
            thread.handle.thread().unpark()
        }
    }
}

/// Per-thread parking helper.
pub struct ParkingHelper<'a> {
    manager: &'a ParkingManager,
    index: usize,
}

impl<'a> ParkingHelper<'a> {
    /// # Panics
    ///
    /// Will panic if
    pub fn new(manager: &'a ParkingManager, index: usize) -> Self {
        assert!(index < manager.threads.len());
        Self { manager, index }
    }

    /// Marks the thread as parked and returns a guard that remove the mark when dropped.
    pub fn guard<'helper>(&'helper self) -> ParkingGuard<'a, 'helper> {
        ParkingGuard { helper: self }
    }

    /// Marks it as not parked.
    fn clear(&self) {
        // Safety: the index is checked when the type is constructed.
        let thread = unsafe { self.manager.threads.get_unchecked(self.index) };
        thread
            .parked
            .store(false, std::sync::atomic::Ordering::SeqCst)
    }
}

/// A guard that automatically marks the thread as not parked when dropped.
pub struct ParkingGuard<'manager, 'helper> {
    helper: &'helper ParkingHelper<'manager>,
}

impl Drop for ParkingGuard<'_, '_> {
    fn drop(&mut self) {
        self.helper.clear()
    }
}
