use std::{
    cell::UnsafeCell,
    ops::{Deref, DerefMut},
    sync::atomic::{AtomicU32, Ordering},
};

const BITS: u32 = 32 / 3;
const MASK: u32 = (1 << BITS) - 1;

const WRITER: u32 = BITS * 0;
const READER: u32 = BITS * 1;
const PENDING: u32 = BITS * 2;

pub struct RwLock<T> {
    state: AtomicU32,
    readers: AtomicU32,
    writers: AtomicU32,
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
            value: UnsafeCell::new(value),
        }
    }

    #[inline]
    pub fn write(&self) -> RwLockWriteGuard<'_, T> {
        let state = self.state.fetch_add(1 << WRITER, Ordering::Acquire);
        
        let writers = (state >> WRITER) & MASK;
        assert!(writers != MASK);

        let readers = (state >> READER) & MASK;
        if readers > 0 || writers > 0 {
            Self::wait(&self.writers);
        }

        RwLockWriteGuard(self)
    }

    pub unsafe fn force_unlock_write(&self) {
        let mut state = self.state.load(Ordering::Relaxed);
        loop {
            let readers = (state >> READER) & MASK;
            assert!(readers == 0);

            let writers = (state >> WRITER) & MASK;
            assert!(writers >= 1);
           
            let pending = (state >> PENDING) & MASK;

            let mut new_state = state - (1 << WRITER);
            if pending > 0 {
                new_state &= MASK << WRITER;
                new_state |= pending << READER;
            }

            if let Err(e) = self.state.compare_exchange_weak(
                state,
                new_state,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                state = e;
                continue;
            }

            if pending > 0 {
                Self::post(&self.readers, pending);
            } else if writers > 1 {
                Self::post(&self.writers, 1);
            } 
            return;
        }
    }

    pub fn read(&self) -> RwLockReadGuard<'_, T> {
        let mut spin = 0;
        let mut state = self.state.load(Ordering::Relaxed);
        loop {
            let writers = (state >> WRITER) & MASK;
            
            let mut new_state = state;
            if writers > 0 {
                if spin < 100 {
                    spin += 1;
                    std::hint::spin_loop();
                    state = self.state.load(Ordering::Relaxed);
                    continue;
                }
                new_state += 1 << PENDING;
            } else {
                new_state += 1 << READER;
            }

            if let Err(e) = self.state.compare_exchange_weak(
                state,
                new_state,
                Ordering::Acquire,
                Ordering::Relaxed,
            ) {
                state = e;
                continue;
            }

            if writers > 0 {
                Self::wait(&self.readers);
            }

            return RwLockReadGuard(self);
        }
    }

    #[inline]
    pub unsafe fn force_unlock_read(&self) {
        let state = self.state.fetch_sub(1 << READER, Ordering::Release);

        let readers = (state >> READER) & MASK;
        assert!(readers > 0);

        let writers = (state >> WRITER) & MASK;
        if readers == 1 && writers > 0 {
            Self::post(&self.writers, 1);
        }
    }

    #[cold]
    fn wait(sema: &AtomicU32) {
        let mut spin = 0;
        loop {    
            let mut count = sema.load(Ordering::Relaxed);
            while count > 0 {
                count = match sema.compare_exchange_weak(
                    count,
                    count - 1,
                    Ordering::Acquire,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => return,
                    Err(e) => e,
                };
            }

            if spin < 100 {
                spin += 1;
                std::hint::spin_loop();
                continue;
            }

            Futex::wait(sema, 0);
            spin = 0;
        }
    }

    #[cold]
    fn post(sema: &AtomicU32, count: u32) {
        sema.fetch_add(count, Ordering::Release);
        Futex::wake(sema, count);
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

