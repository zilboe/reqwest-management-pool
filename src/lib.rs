//! # ReqwestManagementPool
//!
//! A client management pool based on ReqWest and Tokio ensures
//! that clients with different configurations can be reused,
//! reducing memory consumption.
//!
//! ## Features
//!
//! - Automatically manages the reqwest client connection pool
//! - Supports connection reuse, improving performance
//! - Automatically releases and cleans up idle connections
//! - Thread-safe, supporting concurrent access
//! - Automatically expands and shrinks the pool size
//!
//! ## Usage Examples
//!
//! ```no_run
//! use reqwest_management_pool::ClientPool;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Create and initialize the connection pool
//!     let pool = ClientPool::new();
//!     
//!     // Get clients from the pool
//!     if let Some(client) = pool.malloc().await {
//!         // Send a request using the client.
//!         let response = client.get()
//!             .get("https://example.com")
//!             .send()
//!             .await?;
//!         
//!         // The client will automatically release it back into the pool when the scope ends.
//!     }
//!     
//!     Ok(())
//! }
//! ```
// Default client pool size
const CLIENT_POOL_DEFAULT_SIZE: usize = 8;
// Scheduled task processing by the idle client(sec)
const CLIENT_POOL_IDLE_TASK_TIMEOUT: u64 = 120; 
// Idle client timeout value(sec)
const CLIENT_POOL_IDLE_CLEANUP_TIMEOUT: u64 = 180;

use reqwest::Client;
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    sync::{RwLock, mpsc},
    time::{self, Instant},
};


#[derive(Clone)]
struct ClientInner {
    // client obtained from the reqwest library.
    client: Client,
    // flag that was requested to be used
    used_flag: bool,
    // tick used to record applications and releases
    idle_tick: Instant,
}

impl ClientInner {
    // `new` function creates and initializes an Inner.
    fn new() -> Self {
        ClientInner {
            client: Client::new(),
            used_flag: false,
            idle_tick: Instant::now(),
        }
    }
}

impl Default for ClientInner {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for ClientInner {
    /// Clear the used flag and set the tick
    fn drop(&mut self) {
        self.used_flag = false;
        self.idle_tick = Instant::now();
    }
}

// Pooled clients
pub struct PooledClientInner {
    // Client for external use
    client: Client,
    // tx channel, used for dropping
    release_tx: mpsc::UnboundedSender<usize>,
    // ID used to notify drop
    id: usize,
}

impl PooledClientInner {
    pub fn get(&self) -> &Client {
        &self.client
    }

    pub fn id(&self) -> usize {
        self.id
    }
}

impl Drop for PooledClientInner {
    /// When dropping, send an ID notification.
    fn drop(&mut self) {
        if let Err(e) = self.release_tx.send(self.id) {
            eprintln!(
                "Failed to send release request for client {}: {:?}",
                self.id, e
            );
        }
    }
}

// Main structure of the client management pool
#[derive(Clone)]
pub struct ClientPool {
    // Client-side architecture.
    // Uses a HashMap for storage.
    // Manages concurrency and multitasking using Arc and Rwlock.
    inner: Arc<RwLock<HashMap<usize, ClientInner>>>,
    // Used to update the ID of a position in the hashmap.
    // Primarily for easier push functionality.
    update_id: Arc<AtomicUsize>,

    // This is the same as `release_tx` in the `PooledClient` structure.
    release_tx: mpsc::UnboundedSender<usize>,
}

impl ClientPool {
    /// Create a new HTTP client pool
    ///
    /// The pool will pre-create `CLIENT_POOL_DEFAULT_SIZE` clients.
    pub fn new() -> Self {
        let mut map_client: HashMap<usize, ClientInner> = HashMap::new();
        for i in 0..CLIENT_POOL_DEFAULT_SIZE {
            let one_client = ClientInner::new();
            map_client.insert(i, one_client);
        }
        let lock = RwLock::new(map_client);

        let arc = Arc::new(lock);
        let (release_tx, release_rx) = mpsc::unbounded_channel();

        // Starting a background task is used to set the client to terminate after use.
        let pool_inner = arc.clone();
        tokio::spawn(async move {
            let mut rx = release_rx;
            while let Some(id) = rx.recv().await {
                let mut pool = pool_inner.write().await;
                if let Some(inner) = pool.get_mut(&id) {
                    inner.used_flag = false;
                    inner.idle_tick = Instant::now();
                } else {
                    eprintln!("Warning: tried to release non-existent client {}", id);
                }
            }
        });

        let pool = ClientPool {
            inner: arc,
            update_id: Arc::new(AtomicUsize::new(CLIENT_POOL_DEFAULT_SIZE)),
            release_tx,
        };

        // Start a background task to periodically clean up timed-out and unused clients.
        let pool_clone = pool.clone();
        tokio::spawn(async move {
            let pool_inner_cleanup = pool_clone.inner.clone();
            let mut interval = time::interval(Duration::from_secs(CLIENT_POOL_IDLE_TASK_TIMEOUT));
            loop {
                interval.tick().await;
                let now = Instant::now();
                let idle_timeout = Duration::from_secs(CLIENT_POOL_IDLE_CLEANUP_TIMEOUT);
                let mut clients = pool_inner_cleanup.write().await;

                let pool_len = clients.len();

                // If the current quantity is too low, stop recycling.
                if pool_len <= CLIENT_POOL_DEFAULT_SIZE {
                    continue;
                }

                // Delete expired and unused clients
                // These client IDs are temporarily stored as a vec structure.
                let mut to_remove = Vec::new();
                for (id, h) in clients.iter() {
                    if !h.used_flag && h.idle_tick + idle_timeout < now {
                        if pool_len - to_remove.len() > CLIENT_POOL_DEFAULT_SIZE {
                            to_remove.push(*id);
                        }
                    }
                }

                // Find the minimum value in vec and update update_id
                // if let Some(min_id) = to_remove.iter().min() {
                //     pool_clone.update_id.store(*min_id, Ordering::Relaxed);
                // }

                for id in to_remove {
                    clients.remove(&id);
                }
            }
        });

        pool
    }

    pub async fn malloc(&self) -> Option<PooledClientInner> {
        for _ in 0..3 {
            // Acquire read lock
            let pool = self.inner.read().await;

            // Find idle clients
            let (id, used_flag) = {
                let selected = pool
                    .iter()
                    .find(|(_, h)| !h.used_flag)
                    .map(|(id, h)| (*id, h.used_flag));

                match selected {
                    Some((id, false)) => (id, false),
                    _ => (0, true), // No idle clients
                }
            };

            // If there are idle clients, try upgrading to a write lock to mark them for use.
            if !used_flag {
                // Release read lock
                drop(pool);

                // Acquire a write lock to modify the state
                let mut pool_write = self.inner.write().await;
                if let Some(client_inner) = pool_write.get_mut(&id) {
                    if !client_inner.used_flag {
                        client_inner.used_flag = true;
                        client_inner.idle_tick = Instant::now();
                        let client = client_inner.client.clone();

                        return Some(PooledClientInner {
                            client,
                            release_tx: self.release_tx.clone(),
                            id,
                        });
                    }
                }
                // If another thread preempts it, continue the loop.
            } else {
                // No idle client is available. Create a new client (requires write lock).
                drop(pool); // Release read lock
                break;
            }

            // Try again after a short wait.
            tokio::time::sleep(Duration::from_millis(1)).await;
        }

        // If you still can't find it, create a new client.
        let mut pool = self.inner.write().await;
        let new_id = self.update_id.fetch_add(1, Ordering::Relaxed);
        let mut new_client = ClientInner::new();
        new_client.used_flag = true;
        new_client.idle_tick = Instant::now();
        let client = new_client.client.clone();
        pool.insert(new_id, new_client);
        Some(PooledClientInner {
            client,
            release_tx: self.release_tx.clone(),
            id: new_id,
        })
    }

    /// Get the current pool size
    pub async fn size(&self) -> usize {
        self.inner.read().await.len()
    }

    /// Get the number of clients currently in use
    pub async fn used_count(&self) -> usize {
        self.inner
            .read()
            .await
            .iter()
            .filter(|(_, h)| h.used_flag)
            .count()
    }

    /// Get the number of currently idle clients
    pub async fn idle_count(&self) -> usize {
        let pool = self.inner.read().await;
        pool.len() - pool.iter().filter(|(_, h)| h.used_flag).count()
    }
}

impl Default for ClientPool {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    /// Normal requests during testing do not exceed the pool size.
    async fn test_normal_requests(pool: &ClientPool, count: usize) {
        println!("Start with {} normal requests...", count);

        for i in 0..count {
            if let Some(client) = pool.malloc().await {
                println!("  Request {}: Get Client ID: {}", i, client.id());

                // Simulate network requests
                match tokio::time::timeout(
                    Duration::from_millis(100),
                    simulate_http_request(&client),
                )
                .await
                {
                    Ok(Ok(_)) => println!("  Request {}: ✓ Success", i),
                    Ok(Err(e)) => println!("  Request {}: ✗ Failed: {}", i, e),
                    Err(_) => println!("  Request {}: ⏱️ Timeout", i),
                }

                // The client is automatically dropped and released back into the pool.
                drop(client);

                // Take a short break
                tokio::time::sleep(Duration::from_millis(50)).await;
            } else {
                println!("  Request {}: ✗ Unable to retrieve client information", i);
            }
        }

        println!("Normal request test completed");
    }

    /// Testing concurrent requests, exceeding pool size
    async fn test_concurrent_requests(pool: &ClientPool, concurrent_count: usize) {
        println!(
            "Start {} concurrent requests (pool default size: {})...",
            concurrent_count, CLIENT_POOL_DEFAULT_SIZE
        );

        let mut handles = Vec::new();
        let start_time = Instant::now();

        for i in 0..concurrent_count {
            let pool_clone = pool.clone();
            let handle = tokio::spawn(async move {
                if let Some(client) = pool_clone.malloc().await {
                    let request_time = start_time.elapsed().as_millis();
                    println!(
                        "  Request {}: [T+{}ms] Get Client ID: {}",
                        i, request_time, client.id
                    );

                    // Simulate requests at different times
                    let sleep_time = 100 + (i as u64 * 20) % 300;
                    tokio::time::sleep(Duration::from_millis(sleep_time)).await;

                    match simulate_http_request(&client).await {
                        Ok(_) => println!("  Request {}: ✓ Successful (Time taken {}ms)", i, sleep_time),
                        Err(e) => println!("  Request {}: ✗ Failed: {}", i, e),
                    }

                    // Note: The client will automatically drop the block at the end.
                } else {
                    println!("  Request {}: ✗ Unable to retrieve client information", i);
                }
            });

            handles.push(handle);
        }

        // Waiting for all requests to complete
        for handle in handles {
            let _ = handle.await;
        }

        let total_time = start_time.elapsed().as_secs_f32();
        println!("Concurrent request test complete, total time: {:.2} seconds", total_time);
    }

    /// Test sudden surge in requests
    async fn test_burst_requests(pool: &ClientPool, burst_count: usize, total_requests: usize) {
        println!(
            "Test burst requests: {} concurrent requests, total {} requests",
            burst_count, total_requests
        );

        let semaphore = Arc::new(tokio::sync::Semaphore::new(burst_count));
        let success_counter = Arc::new(AtomicUsize::new(0));
        let failure_counter = Arc::new(AtomicUsize::new(0));

        let start_time = Instant::now();

        let mut tasks = Vec::new();

        for i in 0..total_requests {
            let pool_clone = pool.clone();
            let semaphore_clone = semaphore.clone();
            let success_counter_clone = success_counter.clone();
            let failure_counter_clone = failure_counter.clone();

            let task = tokio::spawn(async move {
                // Acquire semaphores to control concurrency.
                let _permit = semaphore_clone.acquire().await.unwrap();

                if let Some(client) = pool_clone.malloc().await {
                    // Simulated random request time
                    let sleep_time = rand::random::<u64>() % 200 + 50;
                    tokio::time::sleep(Duration::from_millis(sleep_time)).await;

                    match simulate_http_request(&client).await {
                        Ok(_) => {
                            success_counter_clone.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(_) => {
                            failure_counter_clone.fetch_add(1, Ordering::Relaxed);
                        }
                    }

                    // client automatically drops
                } else {
                    failure_counter_clone.fetch_add(1, Ordering::Relaxed);
                }
            });

            tasks.push(task);

            // Control request generation speed
            if i % 10 == 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }

        // Waiting for all tasks to complete
        for task in tasks {
            let _ = task.await;
        }

        let total_time = start_time.elapsed().as_secs_f32();
        let success = success_counter.load(Ordering::Relaxed);
        let failure = failure_counter.load(Ordering::Relaxed);

        println!("Burst request test complete:");
        println!("  Successful: {}", success);
        println!("  Failures: {}", failure);
        println!("  Total time: {:.2} seconds", total_time);
        println!("  Average QPS: {:.1}", total_requests as f32 / total_time);
    }

    /// Test long-term operation
    async fn test_long_running(pool: &ClientPool, duration_secs: u64) {
        println!("Long-running test {} seconds...", duration_secs);

        let start_time = Instant::now();
        let end_time = start_time + Duration::from_secs(duration_secs);

        let mut request_count = 0;

        while Instant::now() < end_time {
            request_count += 1;

            if let Some(client) = pool.malloc().await {
                // Simulated random request time
                let sleep_time = rand::random::<u64>() % 100 + 50;
                tokio::time::sleep(Duration::from_millis(sleep_time)).await;

                let _result = simulate_http_request(&client).await;

                // random interval
                let interval = rand::random::<u64>() % 50;
                tokio::time::sleep(Duration::from_millis(interval)).await;
            }

            // The status is printed once every 100 requests.
            if request_count % 100 == 0 {
                let elapsed = start_time.elapsed().as_secs();
                println!("  [T+{}s] {} requests have been processed.", elapsed, request_count);
            }
        }

        println!("The long-running test has completed, processing a total of {} requests.", request_count);
    }

    /// Simulate HTTP requests
    async fn simulate_http_request(_client: &PooledClientInner) -> Result<(), String> {
        // This uses a simulated HTTP request.
        // In practical use, you can replace it with the actual request code.
        // let one = _client.client.clone();
        // if one.get("http://127.0.0.1:9002").send().await.is_ok() {
        //     Ok(())
        // } else {
        //     Err("Simulated request failed".to_string())
        // }
        // Simulate a 90% success rate
        if rand::random::<f32>() < 0.9 {
            Ok(())
        } else {
            Err("Simulated request failed".to_string())
        }
    }
    #[tokio::test]
    async fn test() {
        println!("=== Start ClientPool Test ===");

        // Create a connection pool
        let pool = ClientPool::new();

        println!("✓ Connection pool created successfully, default size: {}", CLIENT_POOL_DEFAULT_SIZE);

        // Monitoring task - Print pool status once per second
        let pool_monitor = pool.clone();
        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_secs(1));
            let mut tick_count = 0;

            loop {
                interval.tick().await;
                tick_count += 1;

                let pool_guard = pool_monitor.inner.read().await;
                let used_count = pool_guard.iter().filter(|(_, h)| h.used_flag).count();
                let idle_count = pool_guard.len() - used_count;
                let total_count = pool_guard.len();

                // Find the largest ID to understand the pool's expansion.
                let max_id = pool_guard.keys().max().unwrap_or(&0);

                println!(
                    "[Monitoring {}s] Pool size: {} (In use: {}, Idle: {}), Maximum ID: {}, Next ID: {}",
                    tick_count,
                    total_count,
                    used_count,
                    idle_count,
                    max_id,
                    pool_monitor.update_id.load(Ordering::Relaxed)
                );

                // Detailed information is printed every 10 seconds.
                if tick_count % 10 == 0 {
                    println!("=== Detailed Status ===");
                    for (id, client) in pool_guard.iter() {
                        let age = client.idle_tick.elapsed().as_secs();
                        println!(
                            "  Client ID: {}, In Use: {}, Created: {} seconds ago",
                            id, client.used_flag, age
                        );
                    }
                    println!("================");
                }
            }
        });

        // Wait 1 second to start the monitoring task.
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Test 1: Normal requests, not exceeding the pool size
        println!("\n=== Test 1: Normal requests (not exceeding the pool size) ===");
        test_normal_requests(&pool, 3).await;

        // Test 2: Concurrent requests exceeding pool size
        println!("\n=== Test 2: Concurrent requests exceeding pool size ===");
        test_concurrent_requests(&pool, 15).await;

        // Wait 10 seconds to observe whether the pool shrinks.
        println!("\n=== Wait 10 seconds to observe whether the pool shrinks. ===");
        tokio::time::sleep(Duration::from_secs(10)).await;

        // Test 3: Sudden surge in requests
        println!("\n=== Test 3: Sudden surge in requests ===");
        test_burst_requests(&pool, 30, 100).await;

        // Test 4: Long-run test
        println!("\n=== Test 4: Long-duration test (30 seconds) ===");
        test_long_running(&pool, 30).await;

        println!("\n=== Test completed ===");
    }
}
