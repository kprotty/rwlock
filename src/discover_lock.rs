use std::{
    cell::UnsafeCell,
    ops::{Deref, DerefMut},
    sync::atomic::{AtomicU32, Ordering},
};

const UNLOCKED: u32 = 0;
const WRITER: u32 = 0;
const READER: u32 = 16;

fn one(shift: u32) -> u32 {
    1 << shift
}

fn mask(shift: u32) -> u32 {
    0xffff << shift
}

pub struct RwLock<T> {
    state: AtomicU32,
    readers: AtomicU32,
    writers: AtomicU32,
    rpending: AtomicU32,
    value: UnsafeCell<T>,
}

unsafe impl<T: Send> Send for RwLock<T> {}
unsafe impl<T: Send> Sync for RwLock<T> {}

impl<T> RwLock<T> {
    pub const fn new(value: T) -> Self {
        Self {
            state: AtomicU32::new(0),
            readers: AtomicU32::new(0),
            writers: AtomicU32::new(0),
            rpending: AtomicU32::new(0),
            value: UnsafeCell::new(value),
        }
    }

    #[inline]
    pub fn try_write(&self) -> Option<RwLockWriteGuard<'_, T>> {
        self.state
            .compare_exchange(
                UNLOCKED,
                one(WRITER),
                Ordering::Acquire,
                Ordering::Relaxed,
            )
            .ok()
            .map(|_| RwLockWriteGuard(self))
    }

    #[inline]
    pub fn write(&self) -> RwLockWriteGuard<'_, T> {
        let state = self.state.fetch_add(one(WRITER), Ordering::Acquire);
        assert_ne!(state & mask(WRITER), mask(WRITER), "writer count overflow");

        if state != UNLOCKED {
            self.write_slow(state);
        }
        RwLockWriteGuard(self)
    }

    #[cold]
    fn write_slow(&self, state: u32) {
        if state & mask(WRITER) == 0 {
            let readers = state >> READER;
            if readers > 0 {
                let rpending = self.rpending.fetch_add(readers, Ordering::Acquire);
                if rpending.wrapping_add(readers) == 0 {
                    return;
                }
            }
        }

        while self.writers.swap(UNLOCKED, Ordering::Acquire) == UNLOCKED {
            Futex::wait(&self.writers, UNLOCKED);
        }
    }

    #[inline]
    pub unsafe fn force_unlock_write(&self) {
        let state = self.state.fetch_sub(one(WRITER), Ordering::Release);
        debug_assert!(state & mask(WRITER) >= one(WRITER));

        if state != one(WRITER) {
            self.unlock_write_slow(state);
        }
    }

    #[cold]
    fn unlock_write_slow(&self, state: u32) {
        if (state & mask(WRITER)) > one(WRITER) {
            self.writers.store(one(WRITER), Ordering::Release);
            Futex::wake(&self.writers, 1);
            return;
        }
        
        let readers = (state & mask(READER)) >> READER;
        self.readers.fetch_add(readers, Ordering::Release);
        Futex::wake(&self.readers, readers);
    }
    
    #[inline]
    pub fn try_read(&self) -> Option<RwLockReadGuard<'_, T>> {
        let mut state = self.state.load(Ordering::Relaxed);
        loop {
            if state & mask(WRITER) != 0 {
                return None;
            }

            let new_state = match state.checked_add(one(READER)) {
                Some(new) => new,
                None => return None,
            };

            state = match self.state.compare_exchange_weak(
                state,
                new_state,
                Ordering::Acquire,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Some(RwLockReadGuard(self)),
                Err(e) => e,
            };
        }
    }

    #[inline]
    pub fn read(&self) -> RwLockReadGuard<'_, T> {
        let state = self.state.fetch_add(one(READER), Ordering::Acquire);
        assert_ne!(state & mask(READER), mask(READER), "reader count overflowed");

        if state & mask(WRITER) != 0 {
            self.read_slow();
        }
        RwLockReadGuard(self)
    }

    #[cold]
    fn read_slow(&self) {
        loop {
            let mut readers = self.readers.load(Ordering::Relaxed);
            while readers > 0 {
                readers = match self.readers.compare_exchange_weak(
                    readers,
                    readers - 1,
                    Ordering::Acquire,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => return,
                    Err(e) => e,
                };
            }
            Futex::wait(&self.readers, 0);
        }
    }

    #[inline]
    pub unsafe fn force_unlock_read(&self) {
        let state = self.state.fetch_sub(one(READER), Ordering::Release);
        debug_assert!(state & mask(READER) >= one(READER));

        if state & mask(WRITER) != 0 {
            self.unlock_read_slow();
        }
    }

    #[cold]
    fn unlock_read_slow(&self) {
        let rpending = self.rpending.fetch_sub(1, Ordering::AcqRel);
        if rpending.wrapping_sub(1) == 0 {
            self.writers.store(one(WRITER), Ordering::Release);
            Futex::wake(&self.writers, 1);
        }
    }
}

pub struct RwLockReadGuard<'a, T>(&'a RwLock<T>);

impl<'a, T> Drop for RwLockReadGuard<'a, T> {
    fn drop(&mut self) {
        unsafe { self.0.force_unlock_read() }
    }
}

impl<'a, T> Deref for RwLockReadGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &T {
        unsafe { &*self.0.value.get() }
    }
}

pub struct RwLockWriteGuard<'a, T>(&'a RwLock<T>);

impl<'a, T> Drop for RwLockWriteGuard<'a, T> {
    fn drop(&mut self) {
        unsafe { self.0.force_unlock_write() }
    }
}

impl<'a, T> DerefMut for RwLockWriteGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut T {
        unsafe { &mut *self.0.value.get() }
    }
}

impl<'a, T> Deref for RwLockWriteGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &T {
        unsafe { &*self.0.value.get() }
    }
}

struct Futex;

#[cfg(target_os = "linux")]
impl Futex {
    pub fn wait(ptr: &AtomicU32, cmp: u32) {
        for _ in 0..4 {
            std::hint::spin_loop();
            if ptr.load(Ordering::Acquire) != cmp { return; }
        }

        for _ in 0..1 {
            std::thread::yield_now();
            if ptr.load(Ordering::Acquire) != cmp { return; }
        }

        let _ = unsafe {
            libc::syscall(
                libc::SYS_futex,
                ptr,
                libc::FUTEX_WAIT | libc::FUTEX_PRIVATE_FLAG,
                cmp,
                0,
            )
        };
    }

    pub fn wake(ptr: &AtomicU32, count: u32) {
        let _ = unsafe {
            libc::syscall(
                libc::SYS_futex,
                ptr,
                libc::FUTEX_WAKE | libc::FUTEX_PRIVATE_FLAG,
                count,
            )
        };
    }
}

