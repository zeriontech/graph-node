use std::{collections::VecDeque, sync::Mutex};

use crate::prelude::tokio::sync::{Semaphore, TryAcquireError};

/// An async-friendly queue of bounded size. In contrast to a bounded channel,
/// the queue makes it possible to modify and remove entries in it.
pub struct BoundedQueue<T> {
    /// The maximum number of entries allowed in the queue
    capacity: usize,
    /// The actual items in the queue. New items are appended at the back, and
    /// popped off the front.
    queue: Mutex<VecDeque<T>>,
    /// This semaphore has as many permits as there are empty spots in the
    /// `queue`, i.e., `capacity - queue.len()` many permits
    push_semaphore: Semaphore,
    /// This semaphore has as many permits as there are entrie in the queue,
    /// i.e., `queue.len()` many
    pop_semaphore: Semaphore,
}

impl<T> std::fmt::Debug for BoundedQueue<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let queue = self.queue.lock().unwrap();
        write!(
            f,
            "BoundedQueue[cap: {}, queue: {}/{}, push: {}, pop: {}]",
            self.capacity,
            queue.len(),
            queue.capacity(),
            self.push_semaphore.available_permits(),
            self.pop_semaphore.available_permits(),
        )
    }
}

impl<T: Clone> BoundedQueue<T> {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            capacity,
            queue: Mutex::new(VecDeque::with_capacity(capacity)),
            push_semaphore: Semaphore::new(capacity),
            pop_semaphore: Semaphore::new(0),
        }
    }

    /// Get an item from the queue. If the queue is currently empty
    /// this method blocks until an item is available.
    pub async fn pop(&self) -> T {
        let permit = self.pop_semaphore.acquire().await.unwrap();
        let item = self
            .queue
            .lock()
            .unwrap()
            .pop_front()
            .expect("the queue is not empty");
        permit.forget();
        self.push_semaphore.add_permits(1);
        item
    }

    /// Get an item from the queue without blocking; if the queue is empty,
    /// return `None`
    pub fn try_pop(&self) -> Option<T> {
        let permit = match self.pop_semaphore.try_acquire() {
            Err(_) => return None,
            Ok(permit) => permit,
        };
        let item = self
            .queue
            .lock()
            .unwrap()
            .pop_front()
            .expect("the queue is not empty");
        permit.forget();
        self.push_semaphore.add_permits(1);
        Some(item)
    }

    /// Take an item from the front of the queue and return a copy. If the
    /// queue is currently empty this method blocks until an item is
    /// available.
    pub async fn peek(&self) -> T {
        let _permit = self.pop_semaphore.acquire().await.unwrap();
        let queue = self.queue.lock().unwrap();
        let item = queue.front().expect("the queue is not empty");
        item.clone()
    }

    /// Same as `peek`, but also call `f` while the queue is still locked
    /// and safe from modification
    pub async fn peek_with<F>(&self, f: F) -> T
    where
        F: FnOnce(&T),
    {
        let _permit = self.pop_semaphore.acquire().await.unwrap();
        let queue = self.queue.lock().unwrap();
        let item = queue.front().expect("the queue is not empty");
        f(item);
        item.clone()
    }

    /// Push an item into the queue. If the queue is currently full this method
    /// blocks until an item is available
    pub async fn push(&self, item: T) {
        let permit = self.push_semaphore.acquire().await.unwrap();
        self.queue.lock().unwrap().push_back(item);
        permit.forget();
        self.pop_semaphore.add_permits(1);
    }

    pub async fn wait_empty(&self) {
        self.push_semaphore
            .acquire_many(self.capacity as u32)
            .await
            .map(|_| ())
            .expect("we never close the push_semaphore")
    }

    pub fn len(&self) -> usize {
        self.queue.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.queue.lock().unwrap().is_empty()
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Iterate over the entries in the queue from newest to oldest entry
    /// atomically, applying `f` to each entry and returning the first
    /// result that is not `None`.
    ///
    /// This method locks the queue while it is executing, and `f` should
    /// therefore not do any slow work.
    pub fn find_map<B, F>(&self, f: F) -> Option<B>
    where
        F: FnMut(&T) -> Option<B>,
    {
        let queue = self.queue.lock().unwrap();
        queue.iter().rev().find_map(f)
    }

    /// Execute `f` on the newest entry in the queue atomically, i.e., while
    /// the queue is locked. The function `f` should therefore not do any
    /// slow work
    pub fn map_newest<F, R>(&self, f: F) -> R
    where
        F: FnOnce(Option<&T>) -> R,
    {
        let queue = self.queue.lock().unwrap();
        f(queue.back())
    }

    /// Iterate over the entries in the queue from newest to oldest entry
    /// atomically, applying `f` to each entry and returning the result of
    /// the last invocation of `f`.
    ///
    /// This method locks the queue while it is executing, and `f` should
    /// therefore not do any slow work.
    pub fn fold<B, F>(&self, init: B, f: F) -> B
    where
        F: FnMut(B, &T) -> B,
    {
        let queue = self.queue.lock().unwrap();
        queue.iter().rev().fold(init, f)
    }

    /// Remove the entries that are poppable when `clear` starts. Entries
    /// that a concurrent `push` inserts while `clear` runs stay in the
    /// queue, and so does an entry whose pop permit has not been added yet
    pub fn clear(&self) {
        let count = self.acquire_all_pop_permits();
        if count == 0 {
            return;
        }
        let drained: Vec<T> = self.queue.lock().unwrap().drain(..count).collect();
        self.push_semaphore.add_permits(count);
        drop(drained);
    }

    fn acquire_all_pop_permits(&self) -> usize {
        loop {
            let count = self.pop_semaphore.available_permits();
            if count == 0 {
                return 0;
            }
            match self.pop_semaphore.try_acquire_many(count as u32) {
                Ok(permit) => {
                    permit.forget();
                    return count;
                }
                Err(TryAcquireError::NoPermits) => continue,
                Err(TryAcquireError::Closed) => unreachable!("we never close the pop_semaphore"),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::future::Future;
    use std::pin::Pin;
    use std::rc::Rc;
    use std::task::{Context, Poll};

    use futures03::FutureExt;
    use futures03::task::noop_waker_ref;

    use super::BoundedQueue;

    type PendingPush = Rc<RefCell<Option<Pin<Box<dyn Future<Output = ()>>>>>>;

    #[derive(Clone)]
    struct Item {
        id: u32,
        on_drop: Option<PendingPush>,
    }

    impl Item {
        fn new(id: u32) -> Self {
            Item { id, on_drop: None }
        }
    }

    impl std::fmt::Debug for Item {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "Item({})", self.id)
        }
    }

    impl PartialEq for Item {
        fn eq(&self, other: &Self) -> bool {
            self.id == other.id
        }
    }

    impl Drop for Item {
        fn drop(&mut self) {
            if let Some(pending) = self.on_drop.take() {
                poll_once(&pending);
            }
        }
    }

    fn poll_once(pending: &PendingPush) {
        let mut slot = pending.borrow_mut();
        if let Some(fut) = slot.as_mut() {
            let mut cx = Context::from_waker(noop_waker_ref());
            if let Poll::Ready(()) = fut.as_mut().poll(&mut cx) {
                *slot = None;
            }
        }
    }

    #[test]
    fn clear_keeps_item_pushed_during_clear() {
        let queue = Rc::new(BoundedQueue::with_capacity(1));
        let pending: PendingPush = Rc::new(RefCell::new(None));

        let pushing = queue.clone();
        *pending.borrow_mut() = Some(Box::pin(async move { pushing.push(Item::new(2)).await }));

        let a = Item {
            id: 1,
            on_drop: Some(pending.clone()),
        };
        queue.push(a).now_or_never().expect("queue has room for A");

        poll_once(&pending);
        assert!(pending.borrow().is_some(), "push(B) blocks on a full queue");

        queue.clear();

        assert!(
            pending.borrow().is_none(),
            "push(B) completed while A was dropped inside clear"
        );
        assert_eq!(queue.len(), 1);
        assert_eq!(queue.try_pop(), Some(Item::new(2)));
    }

    #[test]
    fn clear_restores_permits() {
        for capacity in 1..=4usize {
            for prefill in 0..=capacity {
                let queue = BoundedQueue::with_capacity(capacity);
                for i in 0..prefill {
                    queue
                        .push(i)
                        .now_or_never()
                        .expect("prefill fits in the queue");
                }

                queue.clear();

                assert_eq!(queue.try_pop(), None, "cap {capacity} prefill {prefill}");
                assert_eq!(queue.len(), 0, "cap {capacity} prefill {prefill}");
                for i in 0..capacity {
                    assert!(
                        queue.push(i).now_or_never().is_some(),
                        "cap {capacity} prefill {prefill}: push {i} blocked"
                    );
                }
            }
        }
    }

    #[test]
    fn clear_skips_item_without_pop_permit() {
        let queue = BoundedQueue::with_capacity(2);
        queue.push(1).now_or_never().expect("queue has room");
        queue.queue.lock().unwrap().push_back(2);

        queue.clear();

        assert_eq!(queue.len(), 1);
        assert_eq!(queue.queue.lock().unwrap().front(), Some(&2));
    }
}
