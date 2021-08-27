use std::{
    cell::UnsafeCell,
    ops::{Deref, DerefMut},
    sync::atomic::{AtomicU32, Ordering},
};

const READER_MASK: u32         = 0x0000ffff;
const READER_INC: u32          = 0x00000001;
const PENDING_WRITER_MASK: u32 = 0x0fff0000;
const PENDING_WRITER_INC: u32  = 0x00010000;
const WRITER: u32              = 0x10000000;

pub struct RwLock<T> {
    state: AtomicU32,
    mutex: Mutex,
    semaphore: Semaphore,
    value: UnsafeCell<T>,
}

unsafe impl<T: Send> Send for RwLock<T> {}
unsafe impl<T: Send> Sync for RwLock<T> {}

impl<T> RwLock<T> {
    pub const fn new(value: T) -> Self {
        Self {
            state: AtomicU32::new(0),
            mutex: Mutex::new(),
            semaphore: Semaphore::new(),
            value: UnsafeCell::new(value),
        }
    }

    #[inline]
    pub fn write(&self) -> RwLockWriteGuard<'_, T> {
        self.state.fetch_add(PENDING_WRITER_INC, Ordering::Acquire);
        self.mutex.lock();

        let state = self.state.fetch_add(WRITER.wrapping_sub(PENDING_WRITER_INC), Ordering::Acquire);
        if state & READER_MASK != 0 {
            self.semaphore.wait();
        }

        RwLockWriteGuard(self)
    }

    pub unsafe fn force_unlock_write(&self) {
        self.state.fetch_sub(WRITER, Ordering::Release);
        self.mutex.unlock();
    }

    pub fn read(&self) -> RwLockReadGuard<'_, T> {
        let mut state = self.state.load(Ordering::Relaxed);
        loop {
            if state & (WRITER | PENDING_WRITER_MASK) != 0 {
                self.mutex.lock();
                self.state.fetch_add(READER_INC, Ordering::Acquire);
                self.mutex.unlock();
                break;
            }

            match self.state.compare_exchange(
                state,
                state + READER_INC,
                Ordering::Acquire,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(e) => state = e,
            }
        }
        RwLockReadGuard(self)
    }

    #[inline]
    pub unsafe fn force_unlock_read(&self) {
        let state = self.state.fetch_sub(READER_INC, Ordering::Release);
        if (state & READER_MASK == READER_INC) && (state & WRITER != 0) {
            self.semaphore.post(1);
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

struct Mutex {
    state: AtomicU32,
}

impl Mutex {
    const fn new() -> Self {
        Self {
            state: AtomicU32::new(0),
        }
    }

    fn try_lock(&self) -> bool {
        self.state.compare_exchange(0, 1, Ordering::Acquire, Ordering::Relaxed).is_ok()
    }

    fn lock(&self) {
        if self.try_lock() {
            return;
        }

        for _ in 0..100 {
            std::hint::spin_loop();
            match self.state.load(Ordering::Relaxed) {
                0 => if self.try_lock() { return; },
                1 => continue,
                _ => break,
            }
        }

        loop {
            if self.state.swap(2, Ordering::Acquire) == 0 {
                return;
            }
            Futex::wait(&self.state, 2);
        }
    }

    fn unlock(&self) {
        if self.state.swap(0, Ordering::Release) == 2 {
            Futex::wake(&self.state, 1);
        }
    }
}

struct Semaphore {
    state: AtomicU32,
}

impl Semaphore {
    const fn new() -> Self {
        Self {
            state: AtomicU32::new(0),
        }
    }

    #[cold]
    fn wait(&self) {
        let mut spin = 0;
        loop {    
            let mut count = self.state.load(Ordering::Relaxed);
            while count > 0 {
                count = match self.state.compare_exchange_weak(
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

            Futex::wait(&self.state, 0);
            spin = 0;
        }
    }

    #[cold]
    fn post(&self, count: u32) {
        self.state.fetch_add(count, Ordering::Release);
        Futex::wake(&self.state, count);
    }
}