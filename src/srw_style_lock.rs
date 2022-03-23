use std::{
    hint::spin_loop,
    ptr::{self, NonNull},
    cell::{Cell, UnsafeCell},
    ops::{Deref, DerefMut},
    sync::atomic::{AtomicU32, AtomicUsize, AtomicPtr, fence, Ordering},
};

const UNLOCKED: usize = 0;
const LOCKED: usize = 1;
const QUEUED: usize = 2;
const READING: usize = 4;
const QUEUE_LOCKED: usize = 8;

const READER_SHIFT: u32 = 16usize.trailing_zeros();
const WAITER_MASK: usize = !((1 << READER_SHIFT) - 1);

#[repr(align(16))]
#[derive(Default)]
struct Waiter {
    next: Cell<Option<NonNull<Self>>>,
    prev: AtomicWaiterCell,
    tail: AtomicWaiterCell,
    is_writer: Cell<bool>,
    readers: AtomicUsize,
    event: ResetEvent,
}

#[derive(Default)]
struct AtomicWaiterCell(AtomicPtr<Waiter>);

impl AtomicWaiterCell {
    fn set(&self, p: Option<NonNull<Waiter>>) {
        let p = p.map(|p| p.as_ptr()).unwrap_or(ptr::null_mut());
        self.0.store(p, Ordering::Relaxed);
    }

    fn get(&self) -> Option<NonNull<Waiter>> {
        NonNull::new(self.0.load(Ordering::Relaxed))
    }
}

pub struct RwLock<T> {
    state: AtomicUsize,
    value: UnsafeCell<T>,
}

unsafe impl<T: Send> Send for RwLock<T> {}
unsafe impl<T: Send> Sync for RwLock<T> {}

impl<T> RwLock<T> {
    pub fn new(value: T) -> Self {
        Self {
            state: AtomicUsize::new(UNLOCKED),
            value: UnsafeCell::new(value),
        }
    }

    #[inline]
    pub fn write(&self) -> RwLockWriteGuard<'_, T> {
        if !self.write_fast(UNLOCKED) {
            self.write_slow();
        }

        RwLockWriteGuard(self)
    }

    #[inline(always)]
    #[cfg(target_arch = "x86_64")]
    fn write_fast(&self, _state: usize) -> bool {
        unsafe {
            let mut old_locked_bit: u8;
            std::arch::asm!(
                "lock bts qword ptr [{0:r}], 0",
                "setc {1}",
                in(reg) &self.state,
                out(reg_byte) old_locked_bit,
                options(nostack),
            );
            old_locked_bit == 0
        }
    }

    #[inline(always)]
    #[cfg(target_arch = "x86")]
    fn write_fast(&self, _state: usize) -> bool {
        unsafe {
            let mut old_locked_bit: u8;
            std::arch::asm!(
                "lock bts dword ptr [{0:e}], 0",
                "setc {1}",
                in(reg) &self.state,
                out(reg_byte) old_locked_bit,
                options(nostack),
            );
            old_locked_bit == 0
        }
    }

    #[inline(always)]
    #[cfg(not(any(target_arch = "x86", target_arch = "x86_64")))]
    fn write_fast(&self, state: usize) -> bool {
        let new_state = state | LOCKED;
        self.state
            .compare_exchange_weak(state, new_state, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
    }

    #[cold]
    fn write_slow(&self) {
        let is_writer = true;
        self.lock(is_writer, Self::try_lock_writer);
    }

    #[inline(always)]
    fn try_lock_writer(&self, state: &mut usize) -> Option<bool> {
        match *state & LOCKED {
            0 => Some(self.write_fast(*state)),
            _ => None,
        }
    }

    #[inline]
    pub unsafe fn force_unlock_write(&self) {
        let state = self.state.fetch_sub(LOCKED, Ordering::Release);
        assert_ne!(state & LOCKED, 0);

        if state & (QUEUED | QUEUE_LOCKED) == QUEUED {
            self.unlock_write_slow(state);
        }
    }

    #[cold]
    unsafe fn unlock_write_slow(&self, mut state: usize) {
        state -= LOCKED;

        while state & (LOCKED | QUEUED | QUEUE_LOCKED) == QUEUED {
            let new_state = state | QUEUE_LOCKED;
            match self.state.compare_exchange_weak(
                state,
                new_state,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return self.unpark(new_state),
                Err(e) => state = e,
            }
        }
    }

    #[inline]
    pub fn read(&self) -> RwLockReadGuard<'_, T> {
        if !self.read_fast() {
            self.read_slow();
        }

        RwLockReadGuard(self)
    }

    #[inline(always)]
    fn try_lock_reader(&self, state: &mut usize) -> Option<bool> {
        if *state != UNLOCKED {
            if *state & (LOCKED | READING | QUEUED) != (LOCKED | READING) {
                return None; 
            }
        }

        if let Some(new_state) = (*state | LOCKED | READING).checked_add(1 << READER_SHIFT) {
            match self.state.compare_exchange_weak(
                *state,
                new_state,
                Ordering::Acquire,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Some(true),
                Err(e) => return Some({
                    *state = e;
                    false
                }),
            }
        }

        None
    }

    #[inline(always)]
    fn read_fast(&self) -> bool {
        let mut state = self.state.load(Ordering::Relaxed);
        self.try_lock_reader(&mut state) == Some(true)
    }

    #[cold]
    fn read_slow(&self) {
        let is_writer = false;
        self.lock(is_writer, Self::try_lock_reader);
    }

    #[inline]
    pub unsafe fn force_unlock_read(&self) {
        let mut state = self.state.load(Ordering::Relaxed);

        if state == (LOCKED | READING | (1 << READER_SHIFT)) {
            match self.state.compare_exchange_weak(
                state,
                UNLOCKED,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(e) => state = e,
            }
        }

        self.unlock_read_slow(state);
    }

    #[cold]
    unsafe fn unlock_read_slow(&self, mut state: usize) {
        while state & QUEUED == 0 {
            assert_ne!(state & LOCKED, 0);
            assert_ne!(state & READING, 0);
            assert_ne!(state >> READER_SHIFT, 0);

            let mut new_state = state - (1 << READER_SHIFT);
            if new_state == (LOCKED | READING) {
                new_state = UNLOCKED;
            }

            match self.state.compare_exchange_weak(
                state,
                new_state,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(e) => state = e,
            }
        }

        assert_ne!(state & LOCKED, 0);
        assert_ne!(state & READING, 0);
        assert_ne!(state & QUEUED, 0);

        fence(Ordering::Acquire);
        let (_, tail) = self.get_and_link_queue(state);

        let readers = tail.as_ref().readers.fetch_sub(1, Ordering::Release);
        assert_ne!(readers, 0);
        if readers > 1 {
            return;
        }

        state = self.state.load(Ordering::Relaxed);
        loop {
            assert_ne!(state & LOCKED, 0);
            assert_ne!(state & READING, 0);
            assert_ne!(state & QUEUED, 0);

            let mut new_state = state & !(LOCKED | READING);
            new_state |= QUEUE_LOCKED;

            if let Err(e) = self.state.compare_exchange_weak(
                state,
                new_state,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                state = e;
                continue;
            }

            if state & QUEUE_LOCKED == 0 {
                self.unpark(new_state);
            }

            return;
        }
    }

    fn lock(&self, is_writer: bool, mut try_lock: impl FnMut(&Self, &mut usize) -> Option<bool>) {
        let mut spin = 0;
        let waiter = Waiter::default();
        let mut state = self.state.load(Ordering::Relaxed);

        loop {
            let mut backoff = 0;
            loop {
                match try_lock(self, &mut state) {
                    None => break,
                    Some(false) => {},
                    Some(true) => return,
                }

                backoff = (backoff << 1).min(32);
                (0..backoff).for_each(|_| spin_loop());
                state = self.state.load(Ordering::Relaxed);
            }

            if (state & QUEUED == 0) && (spin < 100) {
                spin += 1;
                std::hint::spin_loop();
                state = self.state.load(Ordering::Relaxed);
                continue;
            }

            waiter.is_writer.set(is_writer);
            waiter.event.reset();
            waiter.prev.set(None);
            waiter.next.set(None);
            waiter.tail.set(None);

            let waiter_ptr = &waiter as *const Waiter as usize;
            let mut new_state = (state & !WAITER_MASK) | waiter_ptr | QUEUED;

            if state & QUEUED == 0 {
                waiter.readers.store(state >> READER_SHIFT, Ordering::Relaxed);
                waiter.tail.set(Some(NonNull::from(&waiter)));
            } else {
                new_state |= QUEUE_LOCKED;
                waiter.next.set(NonNull::new((state & WAITER_MASK) as *mut Waiter));
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

            if state & (QUEUED | QUEUE_LOCKED) == QUEUED {
                unsafe { self.link_queue_or_wake(new_state) };
            }
            
            spin = 0;
            waiter.event.wait();
            state = self.state.load(Ordering::Relaxed);
        }
    }

    #[cold]
    unsafe fn link_queue_or_wake(&self, mut state: usize) {
        loop {
            assert_ne!(state & QUEUE_LOCKED, 0);
            assert_ne!(state & QUEUED, 0);
            
            if state & LOCKED == 0 {
                return self.unpark(state);
            }

            fence(Ordering::Acquire);
            let _ = self.get_and_link_queue(state);

            match self.state.compare_exchange_weak(
                state,
                state & !QUEUE_LOCKED,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(e) => state = e,
            }
        }
    }

    unsafe fn unpark(&self, mut state: usize) {
        let tail = loop {
            assert_ne!(state & QUEUE_LOCKED, 0);
            assert_ne!(state & QUEUED, 0);

            if state & LOCKED != 0 {
                match self.state.compare_exchange_weak(
                    state,
                    state & !QUEUE_LOCKED,
                    Ordering::Release,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => return,
                    Err(e) => state = e,
                }
                continue;
            }

            fence(Ordering::Acquire);
            let (head, tail) = self.get_and_link_queue(state);

            if tail.as_ref().is_writer.get() {
                if let Some(new_tail) = tail.as_ref().prev.get() {
                    head.as_ref().tail.set(Some(new_tail));
                    self.state.fetch_and(!QUEUE_LOCKED, Ordering::Release);

                    tail.as_ref().prev.set(None);
                    break tail;
                }
            }

            match self.state.compare_exchange_weak(
                state,
                state & !(WAITER_MASK | QUEUE_LOCKED | QUEUED),
                Ordering::AcqRel, // could be Release
                Ordering::Acquire,
            ) {
                Ok(_) => break tail,
                Err(e) => state = e,
            }
        };

        let mut wake = Some(tail);
        while let Some(waiter) = wake {
            wake = waiter.as_ref().prev.get();
            waiter.as_ref().event.set();
        }
    }

    unsafe fn get_and_link_queue(&self, state: usize) -> (NonNull<Waiter>, NonNull<Waiter>) {
        let head = NonNull::new((state & WAITER_MASK) as *mut Waiter);
        let head = head.expect("wait queue with invalid head");

        let tail = head.as_ref().tail.get().unwrap_or_else(|| {
            let mut current = head;
            loop {
                let next = current.as_ref().next.get();
                let next = next.expect("waiter with unreachable tail");

                next.as_ref().prev.set(Some(current));
                current = next;

                if let Some(tail) = current.as_ref().tail.get() {
                    head.as_ref().tail.set(Some(tail));
                    return tail;
                }
            }
        });

        (head, tail)
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

#[derive(Default)]
struct ResetEvent {
    state: AtomicU32,
}

impl ResetEvent {
    fn reset(&self) {
        self.state.store(0, Ordering::Relaxed);
    }

    fn wait(&self) {
        if self
            .state
            .compare_exchange(0, 1, Ordering::Acquire, Ordering::Acquire)
            .is_ok()
        {
            loop {
                Futex::wait(&self.state, 1);
                if self.state.load(Ordering::Acquire) == 2 {
                    return;
                }
            }
        }
    }

    fn set(&self) {
        if self.state.swap(2, Ordering::Release) == 1 {
            Futex::wake(&self.state, 1);
        }
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

#[cfg(target_os = "windows")]
impl Futex {
    pub fn wait(ptr: &AtomicU32, cmp: u32) {
        #[link(name = "synchronization")]
        extern "system" {
            fn WaitOnAddress(addr: usize, cmp_addr: usize, size: usize, tm: u32) -> u32;
        }

        let _ = unsafe {
            WaitOnAddress(
                ptr as *const _ as usize,
                &cmp as *const _ as usize,
                std::mem::size_of::<AtomicU32>(),
                !0,
            );
        };
    }

    pub fn wake(ptr: &AtomicU32, count: u32) {
        #[link(name = "synchronization")]
        extern "system" {
            fn WakeByAddressSingle(addr: usize);
            fn WakeByAddressAll(addr: usize);
        }

        match count {
            0 => {},
            1 => unsafe { WakeByAddressSingle(ptr as *const _ as usize) },
            _ => unsafe { WakeByAddressAll(ptr as *const _ as usize) },
        }
    }
}

