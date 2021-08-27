use std::{
    cell::UnsafeCell,
    ops::{Deref, DerefMut},
    sync::atomic::{AtomicU32, Ordering},
};

const BITS: u32 = 32 / 3;
const MASK: u32 = (1 << BITS) - 1;

const UNLOCKED: u32 = 0;
const READER: u32 = BITS * 0;
const WRITER: u32 = BITS * 1;
const PENDING: u32 = BITS * 2;

pub struct RwLock<T> {
    state: AtomicU32,
    rsema: Semaphore,
    wsema: Semaphore,
    value: UnsafeCell<T>,
}

unsafe impl<T: Send> Send for RwLock<T> {}
unsafe impl<T: Send> Sync for RwLock<T> {}

impl<T> RwLock<T> {
    pub const fn new(value: T) -> Self {
        Self {
            state: AtomicU32::new(0),
            rsema: Semaphore::new(),
            wsema: Semaphore::new(),
            value: UnsafeCell::new(value),
        }
    }

    #[inline]
    pub fn write(&self) -> RwLockWriteGuard<'_, T> {
        let state = self.state.fetch_add(1 << WRITER, Ordering::Acquire);
        if state != UNLOCKED {
            self.wsema.wait();
        }

        RwLockWriteGuard(self)
    }

    pub unsafe fn force_unlock_write(&self) {
        if let Err(_) = self.state.compare_exchange(
            1 << WRITER,
            UNLOCKED,
            Ordering::Release,
            Ordering::Relaxed,
        ) {
            self.unlock_write_slow();
        }
    }

    #[cold]
    fn unlock_write_slow(&self) {
        let mut state = self.state.load(Ordering::Relaxed);
        loop {
            let mut new_state = state;

            let writers = (state >> WRITER) & MASK;
            assert!(writers > 0);
            new_state -= 1 << WRITER;

            let pending = (state >> PENDING) & MASK;
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
                self.rsema.post(pending);
            } else if writers > 1 {
                self.wsema.post(1);
            }
            return;
        }
    }

    #[inline]
    pub fn read(&self) -> RwLockReadGuard<'_, T> {
        if !self.read_fast() {
            self.read_slow();
        }

        RwLockReadGuard(self)
    }

    #[inline]
    fn read_fast(&self) -> bool {
        let state = self.state.load(Ordering::Relaxed);
        if state & (MASK << WRITER) != 0 {
            return false;
        }

        let new_state = match state.checked_add(1 << READER) {
            Some(new) => new,
            None => return false,
        };

        self.state.compare_exchange_weak(
            state,
            new_state,
            Ordering::Acquire,
            Ordering::Relaxed,
        ).is_ok()
    }

    #[cold]
    fn read_slow(&self)  {
        let mut spin = SpinWait::new();
        let mut state = self.state.load(Ordering::Relaxed);

        loop {
            let has_writers = state & (MASK << WRITER) != 0;

            let mut new_state = state;
            if has_writers {
                new_state += 1 << PENDING;
            } else {
                new_state += 1 << READER;
            }

            if let Err(_e) = self.state.compare_exchange_weak(
                state,
                new_state,
                Ordering::Acquire,
                Ordering::Relaxed,
            ) {
                //state = e;
                spin.force_yield();
                state = self.state.load(Ordering::Relaxed);
                continue;
            }

            if has_writers {
                self.rsema.wait();
            }

            return;
        }
    }

    #[inline]
    pub unsafe fn force_unlock_read(&self) {
        let state = self.state.fetch_sub(1 << READER, Ordering::Release);
        let readers = (state >> READER) & MASK;
        let writers = (state >> WRITER) & MASK;

        if readers == 1 && writers > 0 {
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

#[allow(unused)]
struct Mutex {
    state: AtomicU32,
}

#[allow(unused)]
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
        
        let mut spin = SpinWait::new();
        while spin.yield_now() {
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

#[allow(unused)]
struct Semaphore {
    value: AtomicU32,
}

#[allow(unused)]
impl Semaphore {
    const fn new() -> Self {
        Self {
            value: AtomicU32::new(0),
        }
    }

    #[cold]
    fn wait(&self) {
        let mut spin = SpinWait::new();
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

            if spin.yield_now() {
                continue;
            }

            Futex::wait(&self.value, 0);
            spin.reset();
        }
    }

    #[cold]
    fn post(&self, n: u32) {
        self.value.fetch_add(n, Ordering::Release);
        Futex::wake(&self.value, n);
    }
}

struct SpinWait {
    count: usize,
}

impl SpinWait {
    const fn new() -> Self {
        Self { count: 0 }
    }

    fn reset(&mut self) {
        self.count = 0;
    }

    fn yield_now(&mut self) -> bool {
        if self.count >= 100 { return false; }
        self.count += 1;
        std::hint::spin_loop();
        true

        // if self.count > 10 {
        //     return false;
        // }
        
        // self.count += 1;
        // if self.count <= 3 {
        //     (0..(1 << self.count)).for_each(|_| std::hint::spin_loop());
        // } else {
        //     std::thread::yield_now();
        // }

        // true
    }

    fn force_yield(&mut self) {
        let _ = self.yield_now() || {
            //std::thread::yield_now();
            true
        };
    }
}