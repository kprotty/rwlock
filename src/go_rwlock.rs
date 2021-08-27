use std::{
    cell::UnsafeCell,
    ops::{Deref, DerefMut},
    sync::atomic::{AtomicU32, AtomicI32, Ordering},
};

const MAX_READERS: i32 = 1 << 30;

pub struct RwLock<T> {
    wlock: Mutex,
    wsema: Semaphore,
    rsema: Semaphore,
    rcount: AtomicI32,
    rwait: AtomicI32,
    value: UnsafeCell<T>,
}

unsafe impl<T: Send> Send for RwLock<T> {}
unsafe impl<T: Send> Sync for RwLock<T> {}

impl<T> RwLock<T> {
    pub const fn new(value: T) -> Self {
        Self {
            wlock: Mutex::new(),
            wsema: Semaphore::new(),
            rsema: Semaphore::new(),
            rcount: AtomicI32::new(0),
            rwait: AtomicI32::new(0),
            value: UnsafeCell::new(value),
        }
    }

    #[inline]
    pub fn write(&self) -> RwLockWriteGuard<'_, T> {
        self.wlock.lock();

        let r = self.rcount.fetch_sub(MAX_READERS, Ordering::Acquire);
        if r != 0 {
            if self.rwait.fetch_add(r, Ordering::Acquire).wrapping_add(r) != 0 {
                self.wsema.wait();
            }
        }

        RwLockWriteGuard(self)
    }

    #[inline]
    pub unsafe fn force_unlock_write(&self) {
        let r = self.rcount.fetch_add(MAX_READERS, Ordering::Release).wrapping_add(MAX_READERS);
        assert!(r < MAX_READERS);

        use std::convert::TryInto;
        if r > 0 {
            self.rsema.post(r.try_into().unwrap());
        }

        self.wlock.unlock();
    }

    #[inline]
    pub fn read(&self) -> RwLockReadGuard<'_, T> {
        if self.rcount.fetch_add(1, Ordering::Acquire).wrapping_add(1) < 0 {
            self.rsema.wait();
        }

        RwLockReadGuard(self)
    }

    #[inline]
    pub unsafe fn force_unlock_read(&self) {
        let r = self.rcount.fetch_sub(1, Ordering::Release);
        if r.wrapping_sub(1) < 0 {
            self.unlock_read_slow(r);
        }
    }

    #[cold]
    #[inline(never)]
    fn unlock_read_slow(&self, r: i32) {
        assert_ne!(r.wrapping_add(1), 0);
        assert_ne!(r.wrapping_add(1), -MAX_READERS);

        if self.rwait.fetch_sub(1, Ordering::AcqRel).wrapping_sub(1) == 0 {
            self.wsema.post(1);
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
    #[cold]
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

    #[cold]
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

        for spin in 0..=10 {
            let spin = spin + 1;
            if spin <= 3 {
                (0..(1 << spin)).for_each(|_| std::hint::spin_loop());
            } else {
                std::thread::yield_now();
            }

            match self.state.load(Ordering::Relaxed) {
                0 => if self.try_lock() { return; },
                1 => continue,
                _ => break,
            }
        }

        while self.state.swap(2, Ordering::Acquire) != 0 {
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
    value: AtomicU32,
}

impl Semaphore {
    const fn new() -> Self {
        Self {
            value: AtomicU32::new(0),
        }
    }

    #[cold]
    fn wait(&self) {
        let mut spin = 0;
        loop {
            let mut value = self.value.load(Ordering::Relaxed);
            while value > 0 {
                match self.value.compare_exchange_weak(
                    value,
                    value - 1,
                    Ordering::Acquire,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => return,
                    Err(e) => value = e,
                }
            }

            if spin <= 10 {
                spin += 1;
                if spin <= 3 {
                    (0..(1 << spin)).for_each(|_| std::hint::spin_loop());
                } else {
                    std::thread::yield_now();
                }
                continue;
            }

            Futex::wait(&self.value, 0);
            spin = 0;
        }
    }

    #[cold]
    fn post(&self, n: u32) {
        self.value.fetch_add(n, Ordering::Release);
        Futex::wake(&self.value, n);
    }
}