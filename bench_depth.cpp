#include <iostream>
#include <iomanip>
#include <vector>
#include <random>
#include <algorithm>
#include <set>
#include <chrono>

#include "tktrie.h"

using namespace gteitelbaum;

// Tree statistics by actual traversal
template <typename Trie>
struct TreeStats {
    using ptr_t = typename Trie::ptr_t;
    
    size_t total_nodes = 0;
    size_t leaf_nodes = 0;
    size_t interior_nodes = 0;
    size_t skip_nodes = 0;
    size_t list_nodes = 0;
    size_t full_nodes = 0;
    size_t max_depth = 0;
    size_t total_leaf_depth = 0;
    size_t total_skip_bytes = 0;
    std::vector<size_t> depth_histogram;
    
    void visit(ptr_t n, size_t depth) {
        if (!n) return;
        
        total_nodes++;
        total_skip_bytes += n->skip_str().size();
        
        if (n->is_leaf()) {
            leaf_nodes++;
            max_depth = std::max(max_depth, depth);
            total_leaf_depth += depth;
            if (depth >= depth_histogram.size()) depth_histogram.resize(depth + 1, 0);
            depth_histogram[depth]++;
            
            if (n->is_skip()) skip_nodes++;
            else if (n->is_list()) list_nodes++;
            else full_nodes++;
        } else {
            interior_nodes++;
            if (n->is_list()) {
                list_nodes++;
                auto* ln = n->template as_list<false>();
                int cnt = ln->count();
                for (int i = 0; i < cnt; ++i) {
                    visit(ln->children[i].load(), depth + 1);
                }
            } else {
                full_nodes++;
                auto* fn = n->template as_full<false>();
                fn->valid.for_each_set([this, fn, depth](unsigned char c) {
                    visit(fn->children[c].load(), depth + 1);
                });
            }
        }
    }
    
    void print() {
        std::cout << "\n### Tree Structure\n\n";
        std::cout << "| Metric | Value |\n";
        std::cout << "|--------|-------|\n";
        std::cout << "| Total nodes | " << total_nodes << " |\n";
        std::cout << "| Interior nodes | " << interior_nodes << " |\n";
        std::cout << "| Leaf nodes | " << leaf_nodes << " |\n";
        std::cout << "| SKIP leaves | " << skip_nodes << " |\n";
        std::cout << "| LIST nodes | " << list_nodes << " |\n";
        std::cout << "| FULL nodes | " << full_nodes << " |\n";
        std::cout << "| Max depth | " << max_depth << " |\n";
        std::cout << "| Avg leaf depth | " << std::fixed << std::setprecision(2) 
                  << (leaf_nodes > 0 ? static_cast<double>(total_leaf_depth) / leaf_nodes : 0) << " |\n";
        std::cout << "| Avg skip bytes | " << std::fixed << std::setprecision(2)
                  << (total_nodes > 0 ? static_cast<double>(total_skip_bytes) / total_nodes : 0) << " |\n";
        
        std::cout << "\n### Depth Histogram\n\n";
        std::cout << "| Depth | Leaves | % |\n";
        std::cout << "|-------|--------|---|\n";
        for (size_t d = 0; d < depth_histogram.size(); ++d) {
            if (depth_histogram[d] > 0) {
                double pct = 100.0 * depth_histogram[d] / leaf_nodes;
                std::cout << "| " << d << " | " << depth_histogram[d] 
                          << " | " << std::fixed << std::setprecision(1) << pct << "% |\n";
            }
        }
    }
};

int main() {
    std::cout << "# Tree Depth Analysis\n\n";
    
    // Random keys (same as benchmark)
    std::vector<uint64_t> random_keys(100000);
    std::mt19937_64 rng(12345);
    for (auto& k : random_keys) k = rng();
    
    // Sequential keys
    std::vector<uint64_t> seq_keys(100000);
    std::iota(seq_keys.begin(), seq_keys.end(), 0);
    
    {
        std::cout << "## Random Keys (100K)\n";
        int64_trie<int> trie;
        for (auto k : random_keys) {
            trie.insert({static_cast<int64_t>(k), static_cast<int>(k)});
        }
        
        TreeStats<int64_trie<int>> stats;
        stats.visit(trie.test_root(), 0);
        stats.print();
        
        // Timing
        auto start = std::chrono::high_resolution_clock::now();
        size_t found = 0;
        for (auto k : random_keys) {
            found += trie.contains(static_cast<int64_t>(k)) ? 1 : 0;
        }
        auto end = std::chrono::high_resolution_clock::now();
        double ns_per_op = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count() 
                          / static_cast<double>(random_keys.size());
        std::cout << "\n### Timing\n\n";
        std::cout << "- Lookup time: " << std::fixed << std::setprecision(1) << ns_per_op << " ns\n";
        std::cout << "- ns per depth level: " << std::setprecision(1) 
                  << ns_per_op / stats.total_leaf_depth * stats.leaf_nodes << " ns\n";
    }
    
    std::cout << "\n";
    
    {
        std::cout << "## Sequential Keys (100K)\n";
        int64_trie<int> trie;
        for (auto k : seq_keys) {
            trie.insert({static_cast<int64_t>(k), static_cast<int>(k)});
        }
        
        TreeStats<int64_trie<int>> stats;
        stats.visit(trie.test_root(), 0);
        stats.print();
        
        // Timing
        auto start = std::chrono::high_resolution_clock::now();
        size_t found = 0;
        for (auto k : seq_keys) {
            found += trie.contains(static_cast<int64_t>(k)) ? 1 : 0;
        }
        auto end = std::chrono::high_resolution_clock::now();
        double ns_per_op = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count() 
                          / static_cast<double>(seq_keys.size());
        std::cout << "\n### Timing\n\n";
        std::cout << "- Lookup time: " << std::fixed << std::setprecision(1) << ns_per_op << " ns\n";
        std::cout << "- ns per depth level: " << std::setprecision(1) 
                  << ns_per_op / stats.total_leaf_depth * stats.leaf_nodes << " ns\n";
    }
    
    return 0;
}
