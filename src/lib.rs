//! # ReqwestManagementPool
//!
//! A client management pool based on Reqwest and Tokio ensures
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
//!
//! ## Configuration
use reqwest::Client;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore, mpsc};
use tokio::time::{self, Duration, Instant};

// Configuration constants
const MIN_IDLE_SIZE: usize = 8; // Minimum number of clients to keep in the pool
const MAX_POOL_SIZE: usize = 50; // Maximum total clients (active + idle)
const IDLE_TIMEOUT: Duration = Duration::from_secs(120); // Time before an idle client is retired
const MONITOR_INTERVAL: Duration = Duration::from_secs(180); // Time between pool metrics reports
/// Internal wrapper for an idle client with a timestamp
struct IdleClient {
    client: Client,
    last_used: Instant,
}

/// The main structure for the HTTP client management pool
#[derive(Clone)]
pub struct ClientPool {
    semaphore: Arc<Semaphore>,
    idle_rx: Arc<Mutex<mpsc::Receiver<IdleClient>>>,
    release_tx: mpsc::Sender<IdleClient>,

    // Thread-safe counters for monitoring
    idle_count: Arc<AtomicUsize>,  // Current clients sitting in the pool
    total_count: Arc<AtomicUsize>, // Total clients managed (idle + working)
}

/// A smart wrapper for a borrowed client. Returns to pool automatically on Drop.
pub struct PooledClient {
    client: Option<Client>,
    _permit: OwnedSemaphorePermit, // Holds the slot in the semaphore
    release_tx: mpsc::Sender<IdleClient>,
    idle_count: Arc<AtomicUsize>,
}

impl PooledClient {
    /// Get a reference to the inner reqwest Client
    pub fn get(&self) -> &Client {
        self.client.as_ref().expect("Client should be present")
    }
}

impl Drop for PooledClient {
    /// When the user is done with the client, send it back to the idle queue
    fn drop(&mut self) {
        if let Some(client) = self.client.take() {
            let tx = self.release_tx.clone();
            let count_ref = self.idle_count.clone();
            let idle = IdleClient {
                client,
                last_used: Instant::now(),
            };

            tokio::spawn(async move {
                if let Ok(_) = tx.send(idle).await {
                    // Successfully returned to pool, increment idle count
                    count_ref.fetch_add(1, Ordering::Relaxed);
                }
            });
        }
    }
}

impl ClientPool {
    /// Create a new ClientPool and initialize core clients
    pub fn new() -> Self {
        let (tx, rx) = mpsc::channel(MAX_POOL_SIZE);
        let idle_count = Arc::new(AtomicUsize::new(0));
        let total_count = Arc::new(AtomicUsize::new(0));

        // Pre-fill the pool with the minimum idle clients
        for _ in 0..MIN_IDLE_SIZE {
            let _ = tx.try_send(IdleClient {
                client: Client::new(),
                last_used: Instant::now(),
            });
            idle_count.fetch_add(1, Ordering::Relaxed);
            total_count.fetch_add(1, Ordering::Relaxed);
        }

        let pool = Self {
            semaphore: Arc::new(Semaphore::new(MAX_POOL_SIZE)),
            idle_rx: Arc::new(Mutex::new(rx)),
            release_tx: tx,
            idle_count,
            total_count,
        };

        // Start background maintenance tasks
        pool.start_cleanup_task();
        pool.start_monitor_reporter(MONITOR_INTERVAL);

        pool
    }

    /// Background task to clean up expired idle clients (Scale Down)
    fn start_cleanup_task(&self) {
        let idle_rx = self.idle_rx.clone();
        let release_tx = self.release_tx.clone();
        let idle_count = self.idle_count.clone();
        let total_count = self.total_count.clone();

        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_secs(5));
            loop {
                interval.tick().await;
                let mut rx = idle_rx.lock().await;
                let mut kept = Vec::new();

                // Drain the channel to inspect all idle clients
                while let Ok(idle) = rx.try_recv() {
                    idle_count.fetch_sub(1, Ordering::Relaxed);

                    if idle.last_used.elapsed() < IDLE_TIMEOUT || kept.len() < MIN_IDLE_SIZE {
                        kept.push(idle);
                    } else {
                        // Retired: Decrement the total count of clients in the system
                        total_count.fetch_sub(1, Ordering::Relaxed);
                    }
                }

                // Push kept clients back into the channel
                for item in kept {
                    let _ = release_tx.send(item).await;
                    idle_count.fetch_add(1, Ordering::Relaxed);
                }
            }
        });
    }

    /// Periodically prints pool metrics to the console
    pub fn start_monitor_reporter(&self, interval_dur: Duration) {
        let pool = self.clone();
        tokio::spawn(async move {
            let mut interval = time::interval(interval_dur);
            loop {
                interval.tick().await;
                let total = pool.total_count();
                let idle = pool.idle_count();
                let working = pool.working_count();

                println!(
                    "[POOL MONITOR] Total: {}, Idle: {}, Working: {}, Load: {:.1}%",
                    total,
                    idle,
                    working,
                    if total > 0 {
                        (working as f64 / total as f64) * 100.0
                    } else {
                        0.0
                    }
                );
            }
        });
    }

    /// Acquire a client from the pool or create a new one if permitted
    pub async fn malloc(&self) -> Option<PooledClient> {
        // Wait for a permit from the semaphore (limit global concurrency)
        let permit = self.semaphore.clone().acquire_owned().await.ok()?;

        let mut rx = self.idle_rx.lock().await;
        let client = match rx.try_recv() {
            Ok(idle) => {
                // Reuse existing client
                self.idle_count.fetch_sub(1, Ordering::Relaxed);
                idle.client
            }
            Err(_) => {
                // Pool is empty but semaphore allowed it: create new client
                self.total_count.fetch_add(1, Ordering::Relaxed);
                Client::new()
            }
        };

        Some(PooledClient {
            client: Some(client),
            _permit: permit,
            release_tx: self.release_tx.clone(),
            idle_count: self.idle_count.clone(),
        })
    }

    // --- Metric API ---

    /// Returns the number of clients currently available in the pool
    pub fn idle_count(&self) -> usize {
        self.idle_count.load(Ordering::Relaxed)
    }

    /// Returns the total number of clients managed by this pool
    pub fn total_count(&self) -> usize {
        self.total_count.load(Ordering::Relaxed)
    }

    /// Returns the number of clients currently borrowed and in use
    pub fn working_count(&self) -> usize {
        self.total_count().saturating_sub(self.idle_count())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // mod client_pool; // 假设上面的代码在 client_pool.rs
    // use client_pool::ClientPool;
    use std::time::Duration;
    use tokio::time::sleep;
    #[tokio::test]
    async fn test() {
        // 1. Initialize the pool (English logs enabled by reporter)
        let pool = ClientPool::new();
        println!(">>> Test Started: Simulating High Load for 15s <<<");

        // 2. Spawn concurrent tasks to consume clients
        for i in 0..40 {
            let p = pool.clone();
            tokio::spawn(async move {
                if let Some(_client) = p.malloc().await {
                    // Use the client for a simulated request
                    sleep(Duration::from_secs(2)).await;
                    if i % 10 == 0 {
                        println!("   [Task {}] Request completed.", i);
                    }
                }
            });
            // Stagger task starts
            sleep(Duration::from_millis(50)).await;
        }

        // 3. Observe the Scaling Down phase
        println!(">>> Load Phase Ended: Waiting for Scale-Down (15s) <<<");
        for sec in 1..=240 {
            sleep(Duration::from_secs(1)).await;
            if sec % 5 == 0 {
                println!(
                    "   Snapshot at {}s: Working={}, Idle={}",
                    sec,
                    pool.working_count(),
                    pool.idle_count()
                );
            }
        }

        println!(">>> Test Finished <<<");
    }
}
