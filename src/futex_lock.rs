use std::{
    hint::spin_loop,
    cell::{UnsafeCell},
    ops::{Deref, DerefMut},
    sync::atomic::{AtomicU32, Ordering},
};

const UNLOCKED: u32 = 0;
const READERS_PARKED: u32 = 1 << 0;
const WRITERS_PARKED: u32 = 1 << 1;

const VALUE: u32 = 1 << 2;
const MASK: u32 = !(VALUE - 1);

const READER: u32 = VALUE;
const WRITER: u32 = MASK;

pub struct RwLock<T> {
    state: AtomicU32,
    epoch: AtomicU32,
    value: UnsafeCell<T>,
}

unsafe impl<T: Send> Send for RwLock<T> {}
unsafe impl<T: Send> Sync for RwLock<T> {}

impl<T> RwLock<T> {
    pub fn new(value: T) -> Self {
        Self {
            state: AtomicU32::new(UNLOCKED),
            epoch: AtomicU32::new(0),
            value: UnsafeCell::new(value),
        }
    }

    #[inline]
    pub fn write(&self) -> RwLockWriteGuard<'_, T> {
        if let Err(state) = self.state.compare_exchange_weak(
            UNLOCKED,
            WRITER,
            Ordering::Acquire,
            Ordering::Relaxed,
        ) {
            self.write_slow(state);
        }

        RwLockWriteGuard(self)
    }

    #[cold]
    fn write_slow(&self, mut state: u32) {
        let mut spin = 0;
        let mut acquire_with: u32 = 0;

        loop {
            while state & MASK == UNLOCKED {
                match self.state.compare_exchange_weak(
                    state,
                    state | WRITER | acquire_with,
                    Ordering::Acquire,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => return,
                    Err(e) => state = e,
                }
            }

            if state & WRITERS_PARKED == 0 {
                if spin < 100 {
                    spin += 1;
                    spin_loop();
                    state = self.state.load(Ordering::Relaxed);
                    continue;
                }

                if let Err(e) = self.state.compare_exchange_weak(
                    state,
                    state | WRITERS_PARKED,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    state = e;
                    continue;
                }
            }

            acquire_with = WRITERS_PARKED;
            loop {
                let epoch = self.epoch.load(Ordering::Acquire);

                state = self.state.load(Ordering::Relaxed);
                if (state & MASK == UNLOCKED) || (state & WRITERS_PARKED == 0) {
                    break;
                }

                Futex::wait(&self.epoch, epoch);
                spin = 0;
            }
        }
    }

    #[inline]
    pub unsafe fn force_unlock_write(&self) {
        if let Err(state) = self.state.compare_exchange(
            WRITER,
            UNLOCKED,
            Ordering::Release,
            Ordering::Relaxed,
        ) {
            self.unlock_write_slow(state);
        }
    }

    #[cold]
    fn unlock_write_slow(&self, state: u32) {
        assert_eq!(state & MASK, WRITER);

        let mut parked = state & (READERS_PARKED | WRITERS_PARKED);
        assert_ne!(parked, 0);
        
        if parked != (READERS_PARKED | WRITERS_PARKED) {
            if let Err(e) = self.state.compare_exchange(
                state,
                UNLOCKED,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                assert_eq!(e, WRITER | READERS_PARKED | WRITERS_PARKED);
                parked = READERS_PARKED | WRITERS_PARKED;
            }
        }

        if parked == (READERS_PARKED | WRITERS_PARKED) {
            self.state.store(WRITERS_PARKED, Ordering::Release);
            parked = READERS_PARKED;
        }

        if parked == READERS_PARKED {
            Futex::wake(&self.state, u32::MAX);
            return;
        }
        
        assert_eq!(parked, WRITERS_PARKED);
        self.epoch.fetch_add(1, Ordering::Release);
        Futex::wake(&self.epoch, 1);
    }

    #[inline]
    pub fn read(&self) -> RwLockReadGuard<'_, T> {
        if !self.read_fast() {
            self.read_slow();
        }

        RwLockReadGuard(self)
    }

    #[inline(always)]
    fn read_fast(&self) -> bool {
        let state = self.state.load(Ordering::Relaxed);
        if state & MASK >= (WRITER - 1) {
            return false;
        }

        let new_state = state + READER;
        self.state
            .compare_exchange_weak(state, new_state, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
    }

    #[cold]
    fn read_slow(&self) {
        let mut spin = 0;
        let mut state = self.state.load(Ordering::Relaxed);

        loop {
            while state & MASK < WRITER {
                let new_state = state + READER;
                assert_ne!(new_state & MASK, WRITER, "RwLock reader count overflowed");

                match self.state.compare_exchange_weak(
                    state,
                    new_state,
                    Ordering::Acquire,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => return,
                    Err(e) => state = e,
                }
            }

            if state & READERS_PARKED == 0 {
                if spin < 100 {
                    spin += 1;
                    spin_loop();
                    state = self.state.load(Ordering::Relaxed);
                    continue;
                }

                if let Err(e) = self.state.compare_exchange_weak(
                    state,
                    state | READERS_PARKED,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    state = e;
                    continue;
                }
            }
            
            Futex::wait(&self.state, state | READERS_PARKED);
            state = self.state.load(Ordering::Relaxed);
            spin = 0;
        }
    }

    #[inline]
    pub unsafe fn force_unlock_read(&self) {
        let state = self.state.fetch_sub(READER, Ordering::Release);
        assert!(state & MASK < WRITER);
        assert_ne!(state & MASK, UNLOCKED);
        assert_eq!(state & READERS_PARKED, 0);

        if state & (MASK | WRITERS_PARKED) == READER | WRITERS_PARKED {
            self.unlock_read_slow();
        }
    }

    #[cold]
    unsafe fn unlock_read_slow(&self) {
        if let Ok(_) = self.state.compare_exchange(
            WRITERS_PARKED,
            UNLOCKED,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            self.epoch.fetch_add(1, Ordering::Release);
            Futex::wake(&self.epoch, 1);
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
                count.min(i32::MAX as u32),
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

