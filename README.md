# Reqwest Management Pool

[English](#english) | [中文](#中文)

---

## English

### Overview

`reqwest-management-pool` is a high-performance HTTP client connection pool library for Rust, built on top of `reqwest` and `tokio`. It automatically manages a pool of HTTP clients, enabling connection reuse and reducing memory consumption while supporting concurrent access.

### Features

- ✅ **Automatic Pool Management**: Automatically manages the reqwest client connection pool
- ✅ **Connection Reuse**: Supports connection reuse, significantly improving performance
- ✅ **Automatic Cleanup**: Automatically releases and cleans up idle connections
- ✅ **Thread-Safe**: Fully thread-safe, supporting concurrent access from multiple tasks
- ✅ **Dynamic Scaling**: Automatically expands and shrinks the pool size based on demand
- ✅ **Zero Configuration**: Works out of the box with sensible defaults

### Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
reqwest-management-pool = "0.1.3"
tokio = { version = "1.49.0", features = ["full"] }
reqwest = "0.13.2"
```

### Quick Start

```rust
use reqwest_management_pool::ClientPool;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create and initialize the connection pool
    let pool = ClientPool::new();
    
    // Get a client from the pool
    if let Some(client) = pool.malloc().await {
        // Use the client to send HTTP requests
        let response = client.get()
            .get("https://example.com")
            .send()
            .await?;
        
        println!("Status: {}", response.status());
        
        // The client is automatically released back into the pool when dropped
    }
    
    Ok(())
}
```

### Usage Examples

#### Basic Usage

```rust
use reqwest_management_pool::ClientPool;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pool = ClientPool::new();
    
    // Get a client from the pool
    let client = pool.malloc().await.unwrap();
    
    // Make a GET request
    let response = client.get()
        .get("https://api.example.com/data")
        .send()
        .await?;
    
    // Client is automatically returned to pool when dropped
    Ok(())
}
```

#### Concurrent Requests

```rust
use reqwest_management_pool::ClientPool;

#[tokio::main]
async fn main() {
    let pool = ClientPool::new();
    
    let mut handles = Vec::new();
    
    // Spawn multiple concurrent requests
    for i in 0..10 {
        let pool_clone = pool.clone();
        let handle = tokio::spawn(async move {
            if let Some(client) = pool_clone.malloc().await {
                let url = format!("https://api.example.com/endpoint/{}", i);
                let _response = client.get().get(&url).send().await;
                // Client automatically returned to pool
            }
        });
        handles.push(handle);
    }
    
    // Wait for all requests to complete
    for handle in handles {
        let _ = handle.await;
    }
}
```

#### Pool Statistics

```rust
use reqwest_management_pool::ClientPool;

#[tokio::main]
async fn main() {
    let pool = ClientPool::new();
    
    // Get pool statistics
    let total_size = pool.total_count().await;
    let working_count = pool.working_count().await;
    let idle_count = pool.idle_count().await;
    
    println!("Pool size: {}", total_size);
    println!("Clients in use: {}", working_count);
    println!("Idle clients: {}", idle_count);
}
```

### API Reference

#### `ClientPool`

The main structure for managing the HTTP client pool.

##### Methods

- **`new() -> ClientPool`**
  
  Creates a new client pool with default settings:
  - Default pool size: 8 clients
  - Maximum pool size: 100 clients
  - Idle timeout: 120 seconds
  - Cleanup interval: 90 seconds

- **`malloc() -> Option<PooledClientInner>`**
  
  Acquires a client from the pool. Returns `None` if the pool is exhausted (though the pool will automatically expand).
  
  The returned client is automatically returned to the pool when dropped.

- **`total_count() -> usize`**
  
  Returns the current total number of clients in the pool.

- **`working_count() -> usize`**
  
  Returns the number of clients currently in use.

- **`idle_count() -> usize`**
  
  Returns the number of currently idle clients.

#### `PooledClientInner`

A wrapper around a `reqwest::Client` that automatically returns to the pool when dropped.

### Configuration

The pool uses the following default constants (defined in `src/lib.rs`):

- `MIN_IDLE_SIZE`: 8 (default pool size)
- `MAX_SIZE`: 100 (maximum pool size)
- `IDLE_TIMEOUT`: 120 seconds (idle client timeout)
- `CLEANUP_INTERVAL`: 90 seconds (cleanup task interval)

To customize these values, you can modify the constants in the source code or fork the repository.

### How It Works

1. **Initialization**: The pool pre-creates a default number of clients (8 by default).

2. **Client Acquisition**: When you call `malloc()`, the pool:
   - First tries to find an idle client
   - If no idle client is available, creates a new one
   - Marks the client as "in use"

3. **Client Release**: When a `PooledClientInner` is dropped:
   - It automatically sends a release message back to the pool
   - The pool marks the client as idle and resets its idle timer

4. **Automatic Cleanup**: A background task periodically:
   - Checks for idle clients that have exceeded the idle timeout
   - Removes them from the pool (but keeps at least the default pool size)
   - This prevents memory bloat during low-traffic periods

### License

This project is licensed under the MIT License.

---

## 中文

### 概述

`reqwest-management-pool` 是一个基于 Rust 的高性能 HTTP 客户端连接池库，构建在 `reqwest` 和 `tokio` 之上。它自动管理 HTTP 客户端池，支持连接复用，减少内存消耗，同时支持并发访问。

### 特性

- ✅ **自动池管理**: 自动管理 reqwest 客户端连接池
- ✅ **连接复用**: 支持连接复用，显著提升性能
- ✅ **自动清理**: 自动释放和清理空闲连接
- ✅ **线程安全**: 完全线程安全，支持多任务并发访问
- ✅ **动态扩展**: 根据需求自动扩展和收缩池大小
- ✅ **零配置**: 开箱即用，提供合理的默认值

### 安装

在 `Cargo.toml` 中添加以下依赖：

```toml
[dependencies]
reqwest-management-pool = "0.1.3"
tokio = { version = "1.49.0", features = ["full"] }
reqwest = "0.13.2"
```

### 快速开始

```rust
use reqwest_management_pool::ClientPool;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 创建并初始化连接池
    let pool = ClientPool::new();
    
    // 从池中获取客户端
    if let Some(client) = pool.malloc().await {
        // 使用客户端发送 HTTP 请求
        let response = client.get()
            .get("https://example.com")
            .send()
            .await?;
        
        println!("状态码: {}", response.status());
        
        // 客户端在销毁时会自动释放回池中
    }
    
    Ok(())
}
```

### 使用示例

#### 基本用法

```rust
use reqwest_management_pool::ClientPool;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pool = ClientPool::new();
    
    // 从池中获取客户端
    let client = pool.malloc().await.unwrap();
    
    // 发送 GET 请求
    let response = client.get()
        .get("https://api.example.com/data")
        .send()
        .await?;
    
    // 客户端在销毁时自动返回池中
    Ok(())
}
```

#### 并发请求

```rust
use reqwest_management_pool::ClientPool;

#[tokio::main]
async fn main() {
    let pool = ClientPool::new();
    
    let mut handles = Vec::new();
    
    // 启动多个并发请求
    for i in 0..10 {
        let pool_clone = pool.clone();
        let handle = tokio::spawn(async move {
            if let Some(client) = pool_clone.malloc().await {
                let url = format!("https://api.example.com/endpoint/{}", i);
                let _response = client.get().get(&url).send().await;
                // 客户端自动返回池中
            }
        });
        handles.push(handle);
    }
    
    // 等待所有请求完成
    for handle in handles {
        let _ = handle.await;
    }
}
```

#### 池统计信息

```rust
use reqwest_management_pool::ClientPool;

#[tokio::main]
async fn main() {
    let pool = ClientPool::new();
    
    // 获取池统计信息
    let total_size = pool.total_count().await;
    let working_count = pool.working_count().await;
    let idle_count = pool.idle_count().await;
    
    println!("池大小: {}", total_size);
    println!("使用中的客户端: {}", working_count);
    println!("空闲客户端: {}", idle_count);
}
```

### API 参考

#### `ClientPool`

管理 HTTP 客户端池的主要结构体。

##### 方法

- **`new() -> ClientPool`**
  
  创建一个具有默认设置的新客户端池：
  - 默认池大小：8 个客户端
  - 空闲超时：120 秒
  - 清理间隔：90 秒

- **`malloc() -> Option<PooledClientInner>`**
  
  从池中获取一个客户端。如果池已耗尽则返回 `None`（尽管池会自动扩展）。
  
  返回的客户端在销毁时会自动返回池中。

- **`total_count() -> usize`**
  
  返回池中当前客户端的总数。

- **`working_count() -> usize`**
  
  返回当前正在使用的客户端数量。

- **`idle_count() -> usize`**
  
  返回当前空闲的客户端数量。

#### `PooledClientInner`

一个包装了 `reqwest::Client` 的结构体，在销毁时自动返回池中。

##### 方法

- **`get() -> &Client`**
  
  返回底层 `reqwest::Client` 的引用。

- **`id() -> usize`**
  
  返回此客户端在池中的唯一 ID。

### 配置

池使用以下默认常量（在 `src/lib.rs` 中定义）：

- `MIN_IDLE_SIZE`: 8（默认池大小）
- `MAX_SIZE`: 100（最大池大小）
- `IDLE_TIMEOUT`: 120 秒（空闲客户端超时）
- `CLEANUP_INTERVAL`: 90 秒（清理任务间隔）

要自定义这些值，您可以修改源代码中的常量或 fork 仓库。

### 工作原理

1. **初始化**: 池会预创建默认数量的客户端（默认为 8 个）。

2. **客户端获取**: 当您调用 `malloc()` 时，池会：
   - 首先尝试找到一个空闲的客户端
   - 如果没有可用的空闲客户端，则创建一个新的
   - 将客户端标记为"使用中"

3. **客户端释放**: 当 `PooledClientInner` 被销毁时：
   - 它会自动发送释放消息回池中
   - 池将客户端标记为空闲并重置其空闲计时器

4. **自动清理**: 后台任务定期：
   - 检查超过空闲超时的空闲客户端
   - 将它们从池中移除（但至少保留默认池大小）
   - 这可以防止在低流量期间内存膨胀
   
### 许可证

本项目采用 MIT 许可证。

---

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

欢迎贡献！请随时提交 Pull Request。
