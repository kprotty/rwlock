use std::{
    cell::UnsafeCell,
    ops::{Deref, DerefMut},
    sync::atomic::{AtomicU32, Ordering},
};

const UNLOCKED: u32 = 0;
const WRITER: u32 = 0b0001;
const PARKED: u32 = 0b0010;
const WRITER_PARKED: u32 = 0b0100;
const READER: u32 = 0b01000;
const READER_MASK: u32 = !(READER - 1);

pub struct RwLock<T> {
    state: AtomicU32,
    queue: WaitQueue,
    value: UnsafeCell<T>,
}

unsafe impl<T: Send> Send for RwLock<T> {}
unsafe impl<T: Send> Sync for RwLock<T> {}

impl<T> RwLock<T> {
    pub fn new(value: T) -> Self {
        Self {
            state: AtomicU32::new(UNLOCKED),
            queue: WaitQueue::new(),
            value: UnsafeCell::new(value),
        }
    }

    #[inline]
    pub fn write(&self) -> RwLockWriteGuard<'_, T> {
        if let Err(_) = self.state.compare_exchange_weak(
            UNLOCKED,
            WRITER,
            Ordering::Acquire,
            Ordering::Relaxed,
        ) {
            self.write_slow();
        }
        RwLockWriteGuard(self)
    }

    #[cold]
    fn write_slow(&self) {
        self.acquire(
            WRITER,
            PARKED,
            || {
                let mut state = self.state.load(Ordering::Relaxed);
                while state & WRITER == 0 {
                    state = match self.state.compare_exchange_weak(
                        state,
                        state | WRITER,
                        Ordering::Acquire,
                        Ordering::Relaxed,
                    ) {
                        Ok(_) => return None,
                        Err(e) => e,
                    };
                }
                return Some(state);
            },
            |state| {
                state & (WRITER | PARKED) == (WRITER | PARKED)
            }
        );

        self.acquire(
            WRITER,
            WRITER_PARKED,
            || {
                let state = self.state.load(Ordering::Acquire);
                assert!(state & WRITER != 0);
                match state & READER_MASK {
                    0 => None,
                    _ => Some(state)
                }
            },
            |state| {
                assert!(state & WRITER != 0);
                if state & READER_MASK == 0 {
                    return false;
                }
                assert!(state & WRITER_PARKED != 0);
                true
            },
        );
    }

    #[inline]
    pub unsafe fn force_unlock_write(&self) {
        if let Err(_) = self.state.compare_exchange(
            WRITER,
            UNLOCKED,
            Ordering::Release,
            Ordering::Relaxed,
        ) {
            self.unlock_write_slow();
        }
    }

    #[cold]
    fn unlock_write_slow(&self) {
        self.notify(PARKED, |has_more| {
            let new_state = if has_more { PARKED } else { UNLOCKED };
            self.state.store(new_state, Ordering::Release);
        })
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
        if state & WRITER != 0 {
            return false;
        }

        let new_state = match state.checked_add(READER) {
            Some(s) => s,
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
    fn read_slow(&self) {
        self.acquire(
            READER,
            PARKED,
            || {
                let mut spin = SpinWait::new();
                loop {
                    let state = self.state.load(Ordering::Relaxed);
                    if state & WRITER != 0 {
                        return Some(state);
                    }

                    let new_state = state.checked_add(READER)
                        .expect("reader count overflowed");

                    match self.state.compare_exchange_weak(
                        state,
                        new_state,
                        Ordering::Acquire,
                        Ordering::Relaxed,
                    ) {
                        Ok(_) => return None,
                        Err(_) => spin.force_yield(),
                    }
                }
            },
            |state| {
                state & (WRITER | PARKED) == (WRITER | PARKED)
            },
        );
    }

    #[inline]
    pub unsafe fn force_unlock_read(&self) {
        let state = self.state.fetch_sub(READER, Ordering::Release);
        assert!(state >= READER);

        if state & (READER_MASK | WRITER_PARKED) == (READER | WRITER_PARKED) {
            self.unlock_read_slow();
        }
    }

    #[cold]
    fn unlock_read_slow(&self) {
        self.notify(WRITER_PARKED, |has_more| {
            assert!(!has_more);
            self.state.fetch_and(!WRITER_PARKED, Ordering::Relaxed);
        });
    }

    #[cold]
    fn acquire(&self, token: u32, addr: u32, try_lock: impl Fn() -> Option<u32>, should_wait: impl Fn(u32) -> bool) {
        let mut spin = SpinWait::new();
        loop {
            let state = match try_lock() {
                None => return,
                Some(s) => s,
            };

            if state & addr == 0 {
                if spin.yield_now() {
                    continue;
                }

                if let Err(_) = self.state.compare_exchange_weak(
                    state,
                    state | addr,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    spin.force_yield();
                    continue;
                }
            }

            self.queue.wait(addr, token, || {
                let state = self.state.load(Ordering::Relaxed);
                should_wait(state)
            });
        }
    }

    #[cold]
    fn notify(&self, addr: u32, on_notify: impl FnOnce(bool)) {
        let mut saw_writer = false;
        let filter = |token| {
            if saw_writer {
                return false;
            }

            saw_writer = token == WRITER;
            return true;
        };
        self.queue.wake(addr, filter, on_notify);
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

        let mut spin = SpinWait::new();
        while spin.yield_now() {
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

struct SpinWait {
    count: usize,
}

#[allow(unused)]
impl SpinWait {
    const fn new() -> Self {
        Self { count: 0 }
    }

    fn reset(&mut self) {
        self.count = 0;
    }

    fn yield_now(&mut self) -> bool {
        if self.count >= 10 { return false; }
        self.count += 1;
        
        if self.count <= 3 {
            (0..(1 << self.count)).for_each(|_| std::sync::atomic::spin_loop_hint());
        } else {
            std::thread::yield_now();
        }
        true
    }

    fn force_yield(&mut self) {
        let _ = self.yield_now() || {
            std::thread::yield_now();
            true
        };
    }
}

use std::{
    cell::Cell,
    ptr::NonNull,
};

struct Waiter {
    token: u32,
    futex: AtomicU32,
    next: Cell<Option<NonNull<Self>>>,
    tail: Cell<Option<NonNull<Self>>>,
}

struct WaitQueueInner {
    parked: Option<NonNull<Waiter>>,
    writer: Option<NonNull<Waiter>>,
}

struct WaitQueue {
    mutex: Mutex,
    queue: UnsafeCell<WaitQueueInner>,
}

impl WaitQueue {
    fn new() -> Self {
        Self {
            mutex: Mutex::new(),
            queue: UnsafeCell::new(WaitQueueInner {
                parked: None,
                writer: None,
            }),
        }
    }

    fn with_queue<T>(&self, f: impl FnOnce(&mut WaitQueueInner) -> T) -> T {
        self.mutex.lock();
        let result = f(unsafe { &mut *self.queue.get() });
        self.mutex.unlock();
        result
    }

    fn wait(&self, addr: u32, token: u32, validate: impl FnOnce() -> bool) {
        let waiter = Waiter {
            token,
            futex: AtomicU32::new(0),
            next: Cell::new(None),
            tail: Cell::new(None),
        };

        if self.with_queue(|queue: &mut WaitQueueInner| unsafe {
            if !validate() {
                return false;
            }

            let head = match addr {
                PARKED => &mut queue.parked,
                WRITER_PARKED => &mut queue.writer,
                _ => unreachable!(),
            };
            
            if let Some(head) = *head {
                let tail = head.as_ref().tail.get().unwrap();
                tail.as_ref().next.set(Some(NonNull::from(&waiter)));
                head.as_ref().tail.set(Some(NonNull::from(&waiter)));
            } else {
                waiter.tail.set(Some(NonNull::from(&waiter)));
                *head = Some(NonNull::from(&waiter));
            }

            true
        }) {
            while waiter.futex.load(Ordering::Acquire) == 0 {
                Futex::wait(&waiter.futex, 0);
            }
        }
    }

    fn wake(&self, addr: u32, mut filter: impl FnMut(u32) -> bool, awoken: impl FnOnce(bool)) {
        let mut notified = self.with_queue(|queue: &mut WaitQueueInner| unsafe {
            let mut notified = None;
            
            let head = match addr {
                PARKED => &mut queue.parked,
                WRITER_PARKED => &mut queue.writer,
                _ => unreachable!(),
            };

            while let Some(waiter) = *head {
                if !filter(waiter.as_ref().token) {
                    break;
                }

                *head = waiter.as_ref().next.get();
                if let Some(new_head) = *head {
                    new_head.as_ref().tail.set(waiter.as_ref().tail.get());
                }

                waiter.as_ref().next.set(notified);
                notified = Some(waiter);
            }

            awoken(head.is_some());
            notified
        });

        while let Some(waiter) = notified {
            let waiter = unsafe { waiter.as_ref() };
            notified = waiter.next.get();
            waiter.futex.store(1, Ordering::Release);
            Futex::wake(&waiter.futex, 1);
        }
    }
}