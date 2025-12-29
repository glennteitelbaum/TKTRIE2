# TKTRIE
Thread optimized vs stp::map and std::unordered map with global mutex

## tktrie Benchmark Results (10 Runs Averaged)

## Strings (970 words)

| Threads | tktrie | ± | std::map | ± | std::unordered_map | ± | trie/map | trie/umap |
|--------:|-------:|--:|---------:|--:|-------------------:|--:|---------:|----------:|
| 1 | 3.68M | 1.20M | 7.17M | 726K | 10.98M | 1.29M | 0.51x | 0.34x |
| 2 | 4.14M | 2.36M | 2.14M | 150K | 4.09M | 354K | 1.93x | 1.01x |
| 4 | 7.50M | 1.52M | 996K | 156K | 1.59M | 160K | 7.53x | 4.72x |
| 8 | 7.63M | 799K | 834K | 112K | 1.23M | 165K | 9.15x | 6.21x |
| 16 | 7.57M | 1.55M | 738K | 141K | 1.02M | 103K | 10.25x | 7.41x |

## uint64_t (100,000 random keys)

| Threads | tktrie | ± | std::map | ± | std::unordered_map | ± | trie/map | trie/umap |
|--------:|-------:|--:|---------:|--:|-------------------:|--:|---------:|----------:|
| 1 | 317K | 35K | 10.05M | 889K | 15.90M | 1.30M | 0.03x | 0.02x |
| 2 | 3.20M | 1.75M | 1.41M | 208K | 3.14M | 621K | 2.28x | 1.02x |
| 4 | 5.74M | 664K | 720K | 53K | 1.42M | 247K | 7.98x | 4.04x |
| 8 | 3.89M | 470K | 540K | 34K | 1.03M | 179K | 7.20x | 3.78x |
| 16 | 2.36M | 163K | 528K | 9K | 963K | 40K | 4.46x | 2.44x |

### Single-Threaded Performance (No Locks)
| Container | ops/sec | vs tktrie |
|-----------|--------:|----------:|
| tktrie | 14,760,839 | 100% |
| std::map | 14,086,580 | 95% |
| std::unordered_map | 37,268,632 | 252% |

