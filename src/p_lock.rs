use std::{
    convert::TryInto,
    cell::{Cell, UnsafeCell},
    ptr::NonNull,
    ops::{Deref, DerefMut},
    sync::atomic::{AtomicU32, AtomicUsize, Ordering},
};

const UNLOCKED: usize = 0;
const WRITER: usize = 1 << 0;
const PARKED: usize = 1 << 1;
const WRITER_PARKED: usize = 1 << 2;
const READER: usize = 1 << 3;
const READER_MASK: usize = !(READER - 1);

const WAITERS: usize = !0b11;

#[repr(align(8))]
struct Waiter {
    writer: Cell<Option<NonNull<Self>>>,
    next: Cell<Option<NonNull<Self>>>,
    tail: Cell<Option<NonNull<Self>>>,
    futex: AtomicU32,
    tag: usize,
}

pub struct RwLock<T> {
    state: AtomicUsize,
    queue: AtomicUsize,
    value: UnsafeCell<T>,
}

unsafe impl<T: Send> Send for RwLock<T> {}
unsafe impl<T: Send> Sync for RwLock<T> {}

impl<T> RwLock<T> {
    pub const fn new(value: T) -> Self {
        Self {
            state: AtomicUsize::new(0),
            queue: AtomicUsize::new(0),
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
            || loop {
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
                    _ => Some(state),
                }
            },
            |state| {
                state & READER_MASK != 0
            }
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
        if self.try_read_fast() != Ok(None) {
            self.read_slow();
        }
        RwLockReadGuard(self)
    }

    #[inline]
    fn try_read_fast(&self) -> Result<Option<usize>, Option<usize>> {
        let state = self.state.load(Ordering::Relaxed);
        if state & WRITER != 0 {
            return Err(Some(state));
        }

        let new_state = match state.checked_add(READER) {
            Some(s) => s,
            None => return Err(None),
        };

        match self.state.compare_exchange_weak(
            state,
            new_state,
            Ordering::Acquire,
            Ordering::Relaxed,
        ) {
            Ok(_) => Ok(None),
            Err(s) => Ok(Some(s)),
        }
    }

    #[cold]
    fn read_slow(&self) {
        self.acquire(
            READER,
            PARKED,
            || loop {
                match self.try_read_fast() {
                    Ok(None) => return None,
                    Ok(Some(_)) => std::hint::spin_loop(),
                    Err(Some(s)) => return Some(s),
                    Err(None) => unreachable!("reader count overflow"),
                }
            },
            |state| {
                state & (WRITER | PARKED) == (WRITER | PARKED)
            }
        );
    }

    #[inline]
    pub unsafe fn force_unlock_read(&self) {
        let state = self.state.fetch_sub(READER, Ordering::Release);
        assert_ne!(state & READER_MASK, 0);

        if state & (READER_MASK | WRITER_PARKED) == (READER | WRITER_PARKED) {
            self.unlock_read_slow();
        }
    }

    #[cold]
    fn unlock_read_slow(&self) {
        self.state.fetch_and(!WRITER_PARKED, Ordering::Relaxed);
        self.notify(WRITER_PARKED, |_| {});
    }

    #[cold]
    fn acquire(
        &self,
        tag: usize,
        token: usize,
        try_acquire: impl Fn() -> Option<usize>,
        should_wait: impl Fn(usize) -> bool,
    ) {
        let mut spin = 0;
        let waiter = Waiter {
            tag: tag | token,
            writer: Cell::new(None),
            next: Cell::new(None),
            tail: Cell::new(None),
            futex: AtomicU32::new(0),
        };

        loop {
            let state = match try_acquire() {
                Some(state) => state,
                None => return,
            };

            if state & token == 0 {
                if spin < 100 {
                    spin += 1;
                    std::hint::spin_loop();
                    continue;
                }

                if let Err(_) = self.state.compare_exchange_weak(
                    state,
                    state | token,
                    Ordering::Acquire,
                    Ordering::Relaxed,
                ) {
                    std::hint::spin_loop();
                    continue;
                }
            }
            
            spin = 0;
            if self.with_queue(|queue| unsafe {
                let state = self.state.load(Ordering::Relaxed);
                if !should_wait(state) {
                    return false;
                }

                if token == WRITER_PARKED {
                    if let Some(head) = *queue {
                        assert!(head.as_ref().writer.get().is_none());
                        head.as_ref().writer.set(Some(NonNull::from(&waiter)));
                    } else {
                        *queue = Some(NonNull::from(&waiter));
                    }
                } else {
                    waiter.next.set(None);
                    waiter.tail.set(Some(NonNull::from(&waiter)));
                    if let Some(head) = *queue {
                        if head.as_ref().tag & WRITER_PARKED != 0 {
                            waiter.writer.set(Some(head));
                            *queue = Some(NonNull::from(&waiter));
                        } else {
                            let tail = head.as_ref().tail.get().unwrap();
                            tail.as_ref().next.set(Some(NonNull::from(&waiter)));
                            head.as_ref().tail.set(Some(NonNull::from(&waiter)));
                        }
                    } else {
                       *queue = Some(NonNull::from(&waiter));
                    }
                }

                waiter.futex.store(0, Ordering::Relaxed);
                return true;
            }) {
                while waiter.futex.load(Ordering::Acquire) == 0 {
                    Futex::wait(&waiter.futex, 0);
                }
            }
        }
    }

    #[cold]
    fn notify(&self, token: usize, on_unpark: impl FnOnce(bool)) {
        let mut notified = self.with_queue(|queue| unsafe {
            let mut notified = None;

            let has_more = if token == WRITER_PARKED {
                if let Some(head) = *queue {
                    if head.as_ref().tag & WRITER_PARKED != 0 {
                        *queue = None;
                        notified = Some(head);
                    } else if let Some(waiter) = head.as_ref().writer.get() {
                        notified = Some(waiter);
                    }
                }
                false
            } else {
                if let Some(head) = *queue {
                    while head.as_ref().tag & WRITER_PARKED == 0 {
                        *queue = head.as_ref().next.get();
                        if let Some(new_head) = *queue {
                            new_head.as_ref().writer.set(head.as_ref().writer.get());
                            new_head.as_ref().tail.set(head.as_ref().tail.get());
                        } else if let Some(w) = head.as_ref().writer.get() {
                            *queue = Some(w);
                        }

                        head.as_ref().next.set(notified);
                        notified = Some(head);
                        if head.as_ref().tag & WRITER != 0 {
                            break;
                        }
                    }
                    queue.map(|w| w.as_ref().tag & WRITER_PARKED == 0).unwrap_or(false)
                } else {
                    false
                }
            };
            
            on_unpark(has_more);
            notified
        });

        while let Some(waiter) = notified {
            unsafe {
                notified = waiter.as_ref().next.get();
                waiter.as_ref().futex.store(1, Ordering::Release);
                Futex::wake(&waiter.as_ref().futex, 1);
            }
        }
    }

    fn with_queue<Q>(&self, f: impl FnOnce(&mut Option<NonNull<Waiter>>) -> Q) -> Q {
        let mut queue = self.lock_queue();
        let result = f(&mut queue);
        self.unlock_queue(queue);
        result
    }

    fn lock_queue(&self) -> Option<NonNull<Waiter>> {
        let mut spin = 0;
        let mut acquire_with = 0;
        let mut queue = self.queue.load(Ordering::Relaxed);

        loop {
            if queue & WRITER == 0 {
                match self.queue.compare_exchange_weak(
                    queue,
                    queue | WRITER | acquire_with,
                    Ordering::Acquire,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => return NonNull::new((queue & WAITERS) as *mut Waiter),
                    Err(e) => queue = e,
                }
                continue;
            }

            if queue & PARKED == 0 {
                if spin < 100 {
                    spin += 1;
                    std::hint::spin_loop();
                    queue = self.queue.load(Ordering::Relaxed);
                    continue;
                }

                if let Err(e) = self.queue.compare_exchange_weak(
                    queue,
                    queue | PARKED,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    queue = e;
                    continue;
                }
            }

            Futex::wait(
                unsafe { &*(&self.queue as *const _ as *const AtomicU32) },
                (queue | PARKED).try_into().unwrap(),
            );

            spin = 0;
            acquire_with = PARKED;
            queue = self.queue.load(Ordering::Relaxed);
        }
    }

    fn unlock_queue(&self, queue: Option<NonNull<Waiter>>) {
        let new_queue = queue.map(|q| q.as_ptr() as usize).unwrap_or(0);
        let old_queue = self.queue.swap(new_queue, Ordering::Release);

        if old_queue & PARKED != 0 {
            Futex::wake(
                unsafe { &*(&self.queue as *const _ as *const AtomicU32) },
                1,
            );
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

