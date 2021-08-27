use std::{
    cell::UnsafeCell,
    ops::{Deref, DerefMut},
    sync::atomic::{AtomicU32, Ordering},
};

pub struct RwLock<T> {
    mutex: Mutex,
    wrlock: AtomicU32,
    rcount: AtomicU32,
    wwait: AtomicU32,
    rwait: AtomicU32,
    value: UnsafeCell<T>,
}

unsafe impl<T: Send> Send for RwLock<T> {}
unsafe impl<T: Send> Sync for RwLock<T> {}

impl<T> RwLock<T> {
    pub const fn new(value: T) -> Self {
        Self {
            mutex: Mutex::new(),
            wrlock: AtomicU32::new(0),
            rcount: AtomicU32::new(0),
            wwait: AtomicU32::new(0),
            rwait: AtomicU32::new(0),
            value: UnsafeCell::new(value),
        }
    }

    #[inline]
    pub fn write(&self) -> RwLockWriteGuard<'_, T> {
        self.mutex.lock();

        while self.rcount.load(Ordering::Relaxed) > 0 || self.wrlock.load(Ordering::Relaxed) > 0 {
            self.wwait.store(self.wwait.load(Ordering::Relaxed) + 1, Ordering::Relaxed);
            self.mutex.unlock();
            Futex::wait(&self.wwait, 1);
            self.mutex.lock();
            self.wwait.store(self.wwait.load(Ordering::Relaxed) - 1, Ordering::Relaxed);
        }

        self.wrlock.store(1, Ordering::Relaxed);
        self.mutex.unlock();

        RwLockWriteGuard(self)
    }

    pub unsafe fn force_unlock_write(&self) {
        self.mutex.lock();
        self.wrlock.store(0, Ordering::Relaxed);
        self.mutex.unlock();

        if self.wwait.load(Ordering::Relaxed) > 0 {
            Futex::wake(&self.wwait, 1);
            return;
        }

        if self.rwait.load(Ordering::Relaxed) > 0 {
            Futex::wake(&self.rwait, !0);
            return;
        }
    }

    pub fn read(&self) -> RwLockReadGuard<'_, T> {
        let writer_preferred = false;
        self.mutex.lock();

        while self.wwait.load(Ordering::Relaxed) > 0 || (writer_preferred && self.wrlock.load(Ordering::Relaxed) > 0) {
            self.rwait.store(self.rwait.load(Ordering::Relaxed) + 1, Ordering::Relaxed);
            self.mutex.unlock();
            Futex::wait(&self.rwait, 1);
            self.mutex.lock();
            self.rwait.store(self.rwait.load(Ordering::Relaxed) - 1, Ordering::Relaxed);
        }

        self.rcount.store(self.rcount.load(Ordering::Relaxed) + 1, Ordering::Relaxed);
        self.mutex.unlock();

        RwLockReadGuard(self)
    }

    #[inline]
    pub unsafe fn force_unlock_read(&self) {
        self.mutex.lock();
        
        let rcount = self.rcount.load(Ordering::Relaxed) - 1;
        self.rcount.store(rcount, Ordering::Relaxed);
        if rcount > 0 {
            self.mutex.unlock();
            return;
        }

        self.mutex.unlock();

        if self.wwait.load(Ordering::Relaxed) > 0 {
            Futex::wake(&self.wwait, 1);
            return;
        }

        if self.rwait.load(Ordering::Relaxed) > 0 {
            Futex::wake(&self.rwait, !0);
            return;
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
