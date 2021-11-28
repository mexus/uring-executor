//! Abstractions over data buffers.

use std::ptr::NonNull;

/// Buffer as a raw parts representation.
///
/// Fields description are only given as a hint, implementors of the [Buffer]
/// trait are free to use them to their own liking.
pub struct BufferRawParts {
    /// Where the buffer begins.
    pub begin: NonNull<u8>,

    /// Size of the occupied part of the buffer.
    pub length: usize,

    /// The amount of bytes the buffer can hold.
    pub capacity: usize,
}

unsafe impl Send for BufferRawParts {}
unsafe impl Sync for BufferRawParts {}

/// # Safety
///
/// The types implementing the trait must return the very same slice (pointing
/// at the very same location in memory) from the [slice][Buffer::slice]
/// function, even when moved around the stack.
pub unsafe trait Buffer: Send + Sync {
    /// Returns a reference to the bytes representation of the type.
    ///
    /// The slice is guaranteed by the safety contract to be the same every time
    /// it is called on the same object.
    fn slice(&self) -> &[u8];

    /// Splits the buffer into its raw parts for later reconstruction with
    /// [reconstruct].
    ///
    /// [reconstruct]: Buffer::reconstruct
    fn split_into_raw_parts(self) -> BufferRawParts;

    /// # Safety
    ///
    /// The function must be called on the [BufferRawParts] returned from the
    /// [split_into_raw_parts] call.
    ///
    /// [split_into_raw_parts]: Buffer::split_into_raw_parts
    unsafe fn reconstruct(parts: BufferRawParts) -> Self;
}

/// # Safety
///
/// The types implementing the trait must return the very same slice (pointing
/// at the very same location in memory) from the [slice_mut] function, even
/// when moved around the stack.
///
/// * [slice_mut]: BufferMut::slice_mut
pub unsafe trait BufferMut: Buffer {
    /// Returns a mutable reference to the bytes representation of the type.
    ///
    /// The slice is guaranteed by the safety contract to be the same every time
    /// it is called on the same object.
    fn slice_mut(&mut self) -> &mut [u8];
}

unsafe impl Buffer for Vec<u8> {
    fn slice(&self) -> &[u8] {
        self.as_ref()
    }

    fn split_into_raw_parts(mut self) -> BufferRawParts {
        let begin =
            NonNull::new(self.as_mut_ptr()).expect("Vec<_> is never based on a null pointer");
        let capacity = self.capacity();
        let length = self.len();

        // Prevent the memory from being freed when the "Vec<_>" goes out of
        // scope.
        std::mem::forget(self);

        BufferRawParts {
            begin,
            length,
            capacity,
        }
    }

    unsafe fn reconstruct(
        BufferRawParts {
            begin,
            length,
            capacity,
        }: BufferRawParts,
    ) -> Self {
        Vec::from_raw_parts(begin.as_ptr(), length, capacity)
    }
}

unsafe impl BufferMut for Vec<u8> {
    fn slice_mut(&mut self) -> &mut [u8] {
        self.as_mut()
    }
}

unsafe impl Buffer for Box<[u8]> {
    fn slice(&self) -> &[u8] {
        self.as_ref()
    }

    fn split_into_raw_parts(mut self) -> BufferRawParts {
        let begin =
            NonNull::new(self.as_mut_ptr()).expect("Box<[u8]> is never based on a null pointer");
        let length = self.len();
        let capacity = length;

        // Prevent the memory from being freed when the "box" goes out of scope.
        std::mem::forget(self);

        BufferRawParts {
            begin,
            length,
            capacity,
        }
    }

    unsafe fn reconstruct(parts: BufferRawParts) -> Self {
        let vec = <Vec<u8> as Buffer>::reconstruct(parts);
        vec.into_boxed_slice()
    }
}

unsafe impl BufferMut for Box<[u8]> {
    fn slice_mut(&mut self) -> &mut [u8] {
        self.as_mut()
    }
}

unsafe impl Buffer for &'static [u8] {
    fn slice(&self) -> &[u8] {
        self
    }

    fn split_into_raw_parts(self) -> BufferRawParts {
        BufferRawParts {
            begin: NonNull::new(self.as_ptr() as *mut _)
                .expect("Slices are never based on a null pointer"),
            length: self.len(),
            capacity: self.len(),
        }
    }

    unsafe fn reconstruct(parts: BufferRawParts) -> Self {
        std::slice::from_raw_parts(parts.begin.as_ptr(), parts.length)
    }
}
