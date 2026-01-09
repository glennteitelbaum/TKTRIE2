#pragma once

// This file contains retry sentinel storage and node_builder class
// It should only be included from tktrie_node_types.h

namespace gteitelbaum {

// =============================================================================
// RETRY SENTINEL STORAGE
// =============================================================================

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
struct retry_storage : node_with_skip<T, THREADED, Allocator, FIXED_LEN> {
    bitmap256<THREADED> valid{};
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
// NODE_BUILDER
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
        bool skip_used = !sk.empty();
        n->set_header(make_header(true, FLAG_SKIP, skip_used, true, false));
        n->skip.assign(sk);
        n->value.set(value);
        return n;
    }
    
    ptr_t make_leaf_binary(std::string_view sk) {
        auto* n = new leaf_binary_t();
        bool skip_used = !sk.empty();
        n->set_header(make_header(true, FLAG_BINARY, skip_used, true, false));
        n->skip.assign(sk);
        return n;
    }
    
    ptr_t make_leaf_list(std::string_view sk) {
        auto* n = new leaf_list_t();
        bool skip_used = !sk.empty();
        n->set_header(make_header(true, FLAG_LIST, skip_used, false, false));
        n->skip.assign(sk);
        return n;
    }
    
    ptr_t make_leaf_pop(std::string_view sk) {
        auto* n = new leaf_pop_t();
        bool skip_used = !sk.empty();
        n->set_header(make_header(true, FLAG_POP, skip_used, false, false));
        n->skip.assign(sk);
        return n;
    }
    
    ptr_t make_leaf_full(std::string_view sk) {
        auto* n = new leaf_full_t();
        bool skip_used = !sk.empty();
        n->set_header(make_header(true, FLAG_FULL, skip_used, false, false));
        n->skip.assign(sk);
        return n;
    }
    
    ptr_t make_interior_binary(std::string_view sk) {
        auto* n = new interior_binary_t();
        bool skip_used = !sk.empty();
        n->set_header(make_header(false, FLAG_BINARY, skip_used, true, false));
        n->skip.assign(sk);
        return n;
    }
    
    ptr_t make_interior_list(std::string_view sk) {
        auto* n = new interior_list_t();
        bool skip_used = !sk.empty();
        n->set_header(make_header(false, FLAG_LIST, skip_used, false, false));
        n->skip.assign(sk);
        return n;
    }
    
    ptr_t make_interior_pop(std::string_view sk) {
        auto* n = new interior_pop_t();
        bool skip_used = !sk.empty();
        n->set_header(make_header(false, FLAG_POP, skip_used, false, false));
        n->skip.assign(sk);
        return n;
    }
    
    ptr_t make_interior_full(std::string_view sk) {
        auto* n = new interior_full_t();
        bool skip_used = !sk.empty();
        n->set_header(make_header(false, FLAG_FULL, skip_used, false, false));
        n->skip.assign(sk);
        return n;
    }
    
    template <bool IS_LEAF>
    ptr_t make_binary(std::string_view sk) {
        if constexpr (IS_LEAF) return make_leaf_binary(sk);
        else return make_interior_binary(sk);
    }
    
    template <bool IS_LEAF>
    ptr_t make_list(std::string_view sk) {
        if constexpr (IS_LEAF) return make_leaf_list(sk);
        else return make_interior_list(sk);
    }
    
    template <bool IS_LEAF>
    ptr_t make_pop(std::string_view sk) {
        if constexpr (IS_LEAF) return make_leaf_pop(sk);
        else return make_interior_pop(sk);
    }
    
    template <bool IS_LEAF>
    ptr_t make_full(std::string_view sk) {
        if constexpr (IS_LEAF) return make_leaf_full(sk);
        else return make_interior_full(sk);
    }
    
    void dealloc_node(ptr_t n) {
        if (!n || is_sentinel(n)) return;
        
        if (n->is_poisoned()) {
            delete_node(n);
            return;
        }
        
        if (!n->is_leaf()) {
            if (n->is_binary()) {
                auto* bn = n->template as_binary<false>();
                int cnt = bn->count();
                for (int i = 0; i < cnt; ++i) {
                    dealloc_node(bn->child_at_slot(i));
                }
            } else if (n->is_list()) [[likely]] {
                auto* ln = n->template as_list<false>();
                int cnt = ln->count();
                for (int i = 0; i < cnt; ++i) {
                    dealloc_node(ln->child_at_slot(i));
                }
            } else if (n->is_pop()) {
                auto* pn = n->template as_pop<false>();
                int cnt = pn->count();
                for (int i = 0; i < cnt; ++i) {
                    dealloc_node(pn->child_at_slot(i));
                }
            } else {
                auto* fn = n->template as_full<false>();
                fn->valid().for_each_set([this, fn](unsigned char c) {
                    dealloc_node(fn->get_child(c));
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
        
        if (src->is_binary()) {
            auto* s = src->template as_binary<false>();
            auto* d = new interior_binary_t();
            d->set_header(s->header());
            d->skip = s->skip;
            s->copy_interior_to(d);
            int cnt = d->count();
            for (int i = 0; i < cnt; ++i) {
                ptr_t child = d->child_at_slot(i);
                d->get_child_slot(d->char_at(i))->store(deep_copy(child));
            }
            return d;
        }
        if (src->is_list()) [[likely]] {
            auto* s = src->template as_list<false>();
            auto* d = new interior_list_t();
            d->set_header(s->header());
            d->skip = s->skip;
            s->copy_interior_to(d);
            int cnt = d->count();
            for (int i = 0; i < cnt; ++i) {
                ptr_t child = d->child_at_slot(i);
                d->get_child_slot(d->char_at(i))->store(deep_copy(child));
            }
            return d;
        }
        if (src->is_pop()) {
            auto* s = src->template as_pop<false>();
            auto* d = new interior_pop_t();
            d->set_header(s->header());
            d->skip = s->skip;
            s->copy_interior_to(d);
            d->valid().for_each_set([this, d](unsigned char c) {
                ptr_t child = d->get_child(c);
                d->get_child_slot(c)->store(deep_copy(child));
            });
            return d;
        }
        auto* s = src->template as_full<false>();
        auto* d = new interior_full_t();
        d->set_header(s->header());
        d->skip = s->skip;
        s->copy_interior_to(d);
        d->valid().for_each_set([this, d](unsigned char c) {
            ptr_t child = d->get_child(c);
            d->get_child_slot(c)->store(deep_copy(child));
        });
        return d;
    }
};

}  // namespace gteitelbaum
