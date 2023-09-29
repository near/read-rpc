/// A memory-size based wrapper around the lru crate.

const INITIAL_CAPACITY: Option<std::num::NonZeroUsize> = std::num::NonZeroUsize::new(10);

/// An LRU-cache which operates on memory used.
pub struct LruMemoryCache<K, V> {
    inner: lru::LruCache<K, V>,
    current_size: usize,
    max_size: usize,
}

impl<K: std::hash::Hash + Eq, V> LruMemoryCache<K, V> {
    /// Create a new cache with a maximum memory size of values.
    pub fn new(max_size: usize) -> Self {
        LruMemoryCache {
            inner: lru::LruCache::new(INITIAL_CAPACITY.unwrap()),
            current_size: 0,
            max_size,
        }
    }

    /// Remove elements until we are below the memory target.
    fn decrease(&mut self) {
        while self.current_size > self.max_size {
            match self.inner.pop_lru() {
                Some((_, v)) => self.current_size -= std::mem::size_of_val(&v),
                _ => break,
            }
        }
    }

    /// Puts a key-value pair into cache.
    /// If the key already exists in the cache, then it updates the key's value
    pub fn put(&mut self, key: K, val: V) {
        let cap = self.inner.cap().get();

        // grow the cache as necessary,
        // lru operates on amount of items,
        // but we're working based on memory usage.
        if self.inner.len() == cap {
            let new_cap = std::num::NonZeroUsize::new(cap.saturating_mul(2))
                .expect("only returns None if value is zero");
            self.inner.resize(new_cap);
        }

        self.current_size += std::mem::size_of_val(&val);

        // subtract any element displaced from the hash.
        if let Some(lru) = self.inner.put(key, val) {
            self.current_size -= std::mem::size_of_val(&lru);
        }

        self.decrease();
    }

    /// Returns a reference to the value of the key in the cache or None if it is not present in the cache.
    pub fn get(&mut self, key: &K) -> Option<&V> {
        self.inner.get(key)
    }

    /// Returns a bool indicating whether the given key is in the cache.
    /// Does not update the LRU list.
    pub fn contains(&self, key: &K) -> bool {
        self.inner.contains(key)
    }

    /// Currently-used size of values in bytes.
    pub fn current_size(&self) -> usize {
        self.current_size
    }

    /// Max cache size of values in bytes.
    pub fn max_size(&self) -> usize {
        self.max_size
    }

    /// Returns the number of key-value pairs that are currently in the the cache.
    pub fn len(&self) -> usize {
        self.inner.len()
    }
}
