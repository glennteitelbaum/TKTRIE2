#pragma once

// This file contains retry sentinel storage and node_builder class
// It should only be included from tktrie_node_types.h

namespace gteitelbaum {

// =============================================================================
// RETRY SENTINEL STORAGE - constinit compatible
// =============================================================================

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
struct retry_storage : node_with_skip<T, THREADED, Allocator, FIXED_LEN> {
    bitmap256 valid{};
    std::array<void*, 256> dummy_children{};
    
    constexpr retry_storage() noexcept 
        : node_with_skip<T, THREADED, Allocator, FIXED_LEN>(RETRY_SENTINEL_HEADER) {}
};

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
struct sentinel_holder {
    static constinit retry_storage<T, THREADED, Allocator, FIXED_LEN> retry;
};

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
constinit retry_storage<T, THREADED, Allocator, FIXED_LEN> 
    sentinel_holder<T, THREADED, Allocator, FIXED_LEN>::retry{};

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
node_base<T, THREADED, Allocator, FIXED_LEN>* get_retry_sentinel() noexcept {
    return reinterpret_cast<node_base<T, THREADED, Allocator, FIXED_LEN>*>(
        &sentinel_holder<T, THREADED, Allocator, FIXED_LEN>::retry);
}

// =============================================================================
// NODE_BUILDER - allocation and type-safe construction
// =============================================================================

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
class node_builder {
public:
    using base_t = node_base<T, THREADED, Allocator, FIXED_LEN>;
    using ptr_t = base_t*;
    using skip_t = skip_node<T, THREADED, Allocator, FIXED_LEN>;
    using leaf_binary_t = binary_node<T, THREADED, Allocator, FIXED_LEN, true>;
    using interior_binary_t = binary_node<T, THREADED, Allocator, FIXED_LEN, false>;
    using leaf_list_t = list_node<T, THREADED, Allocator, FIXED_LEN, true>;
    using interior_list_t = list_node<T, THREADED, Allocator, FIXED_LEN, false>;
    using leaf_pop_t = pop_node<T, THREADED, Allocator, FIXED_LEN, true>;
    using interior_pop_t = pop_node<T, THREADED, Allocator, FIXED_LEN, false>;
    using leaf_full_t = full_node<T, THREADED, Allocator, FIXED_LEN, true>;
    using interior_full_t = full_node<T, THREADED, Allocator, FIXED_LEN, false>;
    
    static constexpr bool is_retry_sentinel(ptr_t n) noexcept {
        if constexpr (!THREADED) {
            (void)n;
            return false;
        } else {
            return n == get_retry_sentinel<T, THREADED, Allocator, FIXED_LEN>();
        }
    }
    
    // is_sentinel is now just is_retry_sentinel (no more not_found sentinel)
    static constexpr bool is_sentinel(ptr_t n) noexcept {
        return is_retry_sentinel(n);
    }
    
    static void delete_node(ptr_t n) {
        if (!n || is_sentinel(n)) return;
        if (n->is_skip()) {
            delete n->as_skip();
        } else if (n->is_binary()) {
            if (n->is_leaf()) delete n->template as_binary<true>();
            else delete n->template as_binary<false>();
        } else if (n->is_list()) [[likely]] {
            if (n->is_leaf()) delete n->template as_list<true>();
            else delete n->template as_list<false>();
        } else if (n->is_pop()) {
            if (n->is_leaf()) delete n->template as_pop<true>();
            else delete n->template as_pop<false>();
        } else {
            if (n->is_leaf()) delete n->template as_full<true>();
            else delete n->template as_full<false>();
        }
    }
    
    ptr_t make_leaf_skip(std::string_view sk, const T& value) {
        auto* n = new skip_t();
        n->set_header(make_header(true, FLAG_SKIP));
        n->skip.assign(sk);
        n->value.set(value);
        return n;
    }
    
    ptr_t make_leaf_binary(std::string_view sk) {
        auto* n = new leaf_binary_t();
        n->set_header(make_header(true, FLAG_BINARY));
        n->skip.assign(sk);
        return n;
    }
    
    ptr_t make_leaf_list(std::string_view sk) {
        auto* n = new leaf_list_t();
        n->set_header(make_header(true, FLAG_LIST));
        n->skip.assign(sk);
        return n;
    }
    
    ptr_t make_leaf_pop(std::string_view sk) {
        auto* n = new leaf_pop_t();
        n->set_header(make_header(true, FLAG_POP));
        n->skip.assign(sk);
        return n;
    }
    
    ptr_t make_leaf_full(std::string_view sk) {
        auto* n = new leaf_full_t();
        n->set_header(make_header(true, 0));
        n->skip.assign(sk);
        return n;
    }
    
    ptr_t make_interior_binary(std::string_view sk) {
        auto* n = new interior_binary_t();
        n->set_header(make_header(false, FLAG_BINARY));
        n->skip.assign(sk);
        return n;
    }
    
    ptr_t make_interior_list(std::string_view sk) {
        auto* n = new interior_list_t();
        n->set_header(make_header(false, FLAG_LIST));
        n->skip.assign(sk);
        return n;
    }
    
    ptr_t make_interior_pop(std::string_view sk) {
        auto* n = new interior_pop_t();
        n->set_header(make_header(false, FLAG_POP));
        n->skip.assign(sk);
        return n;
    }
    
    ptr_t make_interior_full(std::string_view sk) {
        auto* n = new interior_full_t();
        n->set_header(make_header(false, 0));
        n->skip.assign(sk);
        return n;
    }
    
    void dealloc_node(ptr_t n) {
        if (!n || is_sentinel(n)) return;
        
        // If poisoned, this is a speculative node with borrowed children - don't recurse
        if (n->is_poisoned()) {
            delete_node(n);
            return;
        }
        
        if (!n->is_leaf()) {
            if (n->is_binary()) {
                auto* bn = n->template as_binary<false>();
                int cnt = bn->count();
                for (int i = 0; i < cnt; ++i) {
                    dealloc_node(bn->children[i].load());
                }
            } else if (n->is_list()) [[likely]] {
                auto* ln = n->template as_list<false>();
                int cnt = ln->count();
                for (int i = 0; i < cnt; ++i) {
                    dealloc_node(ln->children[i].load());
                }
            } else if (n->is_pop()) {
                auto* pn = n->template as_pop<false>();
                int cnt = pn->count();
                for (int i = 0; i < cnt; ++i) {
                    dealloc_node(pn->children[i].load());
                }
            } else {
                auto* fn = n->template as_full<false>();
                fn->valid.for_each_set([this, fn](unsigned char c) {
                    dealloc_node(fn->children[c].load());
                });
            }
        }
        delete_node(n);
    }
    
    ptr_t deep_copy(ptr_t src) {
        if (!src || is_sentinel(src)) return nullptr;
        
        if (src->is_leaf()) {
            if (src->is_skip()) {
                auto* s = src->as_skip();
                auto* d = new skip_t();
                d->set_header(s->header());
                d->skip = s->skip;
                d->value.deep_copy_from(s->value);
                return d;
            }
            if (src->is_binary()) {
                auto* s = src->template as_binary<true>();
                auto* d = new leaf_binary_t();
                d->set_header(s->header());
                d->skip = s->skip;
                s->copy_values_to(d);
                return d;
            }
            if (src->is_list()) [[likely]] {
                auto* s = src->template as_list<true>();
                auto* d = new leaf_list_t();
                d->set_header(s->header());
                d->skip = s->skip;
                s->copy_values_to(d);
                return d;
            }
            if (src->is_pop()) {
                auto* s = src->template as_pop<true>();
                auto* d = new leaf_pop_t();
                d->set_header(s->header());
                d->skip = s->skip;
                s->copy_values_to(d);
                return d;
            }
            auto* s = src->template as_full<true>();
            auto* d = new leaf_full_t();
            d->set_header(s->header());
            d->skip = s->skip;
            s->copy_values_to(d);
            return d;
        }
        
        // Interior
        if (src->is_binary()) {
            auto* s = src->template as_binary<false>();
            auto* d = new interior_binary_t();
            d->set_header(s->header());
            d->skip = s->skip;
            if constexpr (FIXED_LEN == 0) {
                d->eos.deep_copy_from(s->eos);
            }
            s->copy_children_to(d);
            for (int i = 0; i < d->count_; ++i) {
                d->children[i].store(deep_copy(d->children[i].load()));
            }
            return d;
        }
        if (src->is_list()) [[likely]] {
            auto* s = src->template as_list<false>();
            auto* d = new interior_list_t();
            d->set_header(s->header());
            d->skip = s->skip;
            d->chars = s->chars;
            if constexpr (FIXED_LEN == 0) {
                d->eos.deep_copy_from(s->eos);
            }
            int cnt = s->count();
            for (int i = 0; i < cnt; ++i) {
                d->children[i].store(deep_copy(s->children[i].load()));
            }
            return d;
        }
        if (src->is_pop()) {
            auto* s = src->template as_pop<false>();
            auto* d = new interior_pop_t();
            d->set_header(s->header());
            d->skip = s->skip;
            d->valid = s->valid;
            if constexpr (FIXED_LEN == 0) {
                d->eos.deep_copy_from(s->eos);
            }
            int cnt = s->count();
            for (int i = 0; i < cnt; ++i) {
                d->children[i].store(deep_copy(s->children[i].load()));
            }
            return d;
        }
        auto* s = src->template as_full<false>();
        auto* d = new interior_full_t();
        d->set_header(s->header());
        d->skip = s->skip;
        d->valid = s->valid;
        if constexpr (FIXED_LEN == 0) {
            d->eos.deep_copy_from(s->eos);
        }
        s->valid.for_each_set([this, s, d](unsigned char c) {
            d->children[c].store(deep_copy(s->children[c].load()));
        });
        return d;
    }
};

}  // namespace gteitelbaum
