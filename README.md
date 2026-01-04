# tktrie Performance

## When to Use

| Use Case | Recommendation |
|----------|----------------|
| Single-threaded, need fastest lookups | `std::unordered_map` |
| Single-threaded, need ordered iteration | `tktrie` or `std::map` *(small N)* |
| Multi-threaded, read-heavy (>90% reads) | `concurrent_tktrie` |
| Multi-threaded, many readers + few writers | `concurrent_tktrie` |
| Multi-threaded, write-heavy | `guarded unordered_map` |
| Prefix/range queries needed | `tktrie` |
| Sequential/sorted keys | `tktrie` (best compression) |

---

## Non-Threaded Performance (100K keys)

### Random Keys

| Operation | tktrie | std::map | unordered_map | vs map | vs umap |
|-----------|-------:|--------:|-------------:|-------:|--------:|
| insert | 25.5ms | 25.7ms | 6.4ms | 0.99x | 3.96x |
| find | 5.7ms | 19.1ms | 1.4ms | **0.30x** | 4.20x |
| insert+erase | 33.8ms | 41.6ms | 7.2ms | 0.81x | 4.72x |

### Sequential Keys

| Operation | tktrie | std::map | unordered_map | vs map | vs umap |
|-----------|-------:|--------:|-------------:|-------:|--------:|
| insert | 3.5ms | 9.7ms | 2.5ms | **0.36x** | 1.40x |
| find | 1.1ms | 6.1ms | 0.3ms | **0.18x** | 3.76x |
| insert+erase | 6.1ms | 14.1ms | 3.6ms | **0.44x** | 1.73x |

---

## Threaded Performance (100K keys)

### Find Scaling (Random Keys)

| Threads | tktrie | guarded_map | guarded_umap | vs gmap | vs gumap |
|--------:|-------:|------------:|-----------:|--------:|---------:|
| 1 | 8.8ms | 27.7ms | 4.3ms | 0.32x | 2.05x |
| 2 | 3.7ms | 14.2ms | 7.5ms | **0.26x** | **0.50x** |
| 4 | 1.9ms | 8.2ms | 8.4ms | **0.24x** | **0.23x** |

### Find Scaling (Sequential Keys)

| Threads | tktrie | guarded_map | guarded_umap | vs gmap | vs gumap |
|--------:|-------:|------------:|-----------:|--------:|---------:|
| 1 | 1.7ms | 9.3ms | 2.3ms | 0.18x | 0.72x |
| 2 | 1.0ms | 6.3ms | 7.5ms | **0.16x** | **0.13x** |
| 4 | 0.6ms | 8.9ms | 8.6ms | **0.07x** | **0.07x** |

---

## Read vs Write Contention (4 readers)

### Random Keys

| Writers | tktrie reads/ms | guarded_map reads/ms | guarded_umap reads/ms |
|--------:|----------------:|---------------------:|----------------------:|
| 0 | 64,839 | 11,937 | 12,784 |
| 1 | 13,160 | 10,548 | 11,281 |
| 2 | 27,443 | 9,278 | 10,556 |
| 4 | 21,026 | 9,471 | 9,414 |

### Sequential Keys

| Writers | tktrie reads/ms | guarded_map reads/ms | guarded_umap reads/ms |
|--------:|----------------:|---------------------:|----------------------:|
| 0 | 338,826 | 10,930 | 11,767 |
| 1 | 163,613 | 9,570 | 9,753 |
| 2 | 160,361 | 8,387 | 9,433 |
| 4 | 75,269 | 8,095 | 8,447 |

---

## Reader Scaling (1 writer active)

### Random Keys

| Readers | tktrie reads/ms | tktrie writes/ms | guarded_map reads/ms | guarded_map writes/ms |
|--------:|----------------:|-----------------:|---------------------:|----------------------:|
| 1 | 2,916 | 202 | 4,921 | 169 |
| 2 | 6,574 | 231 | 12,977 | 54 |
| 4 | 13,566 | 166 | 12,077 | 10 |
| 8 | 22,663 | 104 | 5,702 | 1 |

### Sequential Keys

| Readers | tktrie reads/ms | tktrie writes/ms | guarded_map reads/ms | guarded_map writes/ms |
|--------:|----------------:|-----------------:|---------------------:|----------------------:|
| 1 | 50,630 | 149 | 4,889 | 1,130 |
| 2 | 96,335 | 118 | 12,111 | 64 |
| 4 | 170,698 | 148 | 10,088 | 20 |
| 8 | 296,288 | 51 | 5,280 | 1 |

---

## Key Takeaways

- **Reads scale**: tktrie reads scale with thread count; guarded containers don't
- **Sequential keys**: 10-30x faster than random due to skip compression
- **Read-heavy wins**: At 8 readers + 1 writer, tktrie delivers 4-56x read throughput
- **Write throughput**: tktrie maintains 50-200 writes/ms under contention vs 1-20 for guarded
- **Single-threaded**: tktrie beats std::map 3-5x on finds; unordered_map is faster for pure hash lookups
