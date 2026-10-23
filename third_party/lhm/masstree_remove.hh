/* Masstree
 * Eddie Kohler, Yandong Mao, Robert Morris
 * Copyright (c) 2012-2019 President and Fellows of Harvard College
 * Copyright (c) 2012-2016 Massachusetts Institute of Technology
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, subject to the conditions
 * listed in the Masstree LICENSE file. These conditions include: you must
 * preserve this copyright notice, and you cannot mention the copyright
 * holders in advertising related to the Software without their permission.
 * The Software is provided WITHOUT ANY WARRANTY, EXPRESS OR IMPLIED. This
 * notice is a summary of the Masstree LICENSE file; the license in that file
 * is legally binding.
 */
#ifndef MASSTREE_REMOVE_HH
#define MASSTREE_REMOVE_HH
#include "masstree_get.hh"
#include "btree_leaflink.hh"
#include "circular_int.hh"
namespace Masstree {

template <typename P>
bool tcursor<P>::gc_layer(threadinfo& ti)
{
    find_locked(ti);
    masstree_precondition(!n_->deleted() && !n_->deleted_layer());

    // find_locked might return early if another gc_layer attempt has
    // succeeded at removing multiple tree layers. So check that the whole
    // key has been consumed
    if (ka_.has_suffix()) {
        return false;
    }

    // find the slot for the child tree
    // ka_ is a multiple of ikey_size bytes long. We are looking for the entry
    // for the next tree layer, which has keylenx_ corresponding to ikey_size+1.
    // So if has_value(), then we found an entry for the same ikey, but with
    // length ikey_size; we need to adjust ki_.
    kx_.i += has_value();
    if (kx_.i >= n_->size()) {
        return false;
    }
    permuter_type perm(n_->permutation_);
    kx_.p = perm[kx_.i];
    if (n_->ikey0_[kx_.p] != ka_.ikey() || !n_->is_layer(kx_.p)) {
        return false;
    }

    // remove redundant internode layers
    node_type* layer;
    while (true) {
        layer = n_->lv_[kx_.p].layer();
        if (!layer->is_root()) {
            n_->lv_[kx_.p] = layer->maybe_parent();
            continue;
        }

        if (layer->isleaf()) {
            break;
        }

        internode_type *in = static_cast<internode_type *>(layer);
        // 对带目录语义的 root 来说，删除大量孩子后可能会残留一种
        // “size()==1、child_[0] 已空、唯一有效子树挂在 child_[1]” 的中间态。
        // 这里先把 root 归一化成“size()==0、child_[0] 为唯一有效子树”，
        // 再复用下面已有的 root 收缩与 meta 迁移逻辑。
        if (in->size() > 1) {
            return false;
        }
        in->lock(*layer, ti.lock_fence(tc_internode_lock));
        if (!in->is_root() || in->size() > 1) {
            goto unlock_layer;
        }

        if (in->size() == 1 && in->child_[0] && in->child_[1]) {
            node_type* left = in->child_[0];
            if (!left->isleaf()) {
                goto unlock_layer;
            }

            leaf_type* left_leaf = static_cast<leaf_type*>(left);
            left_leaf->lock(*left_leaf, ti.lock_fence(tc_leaf_lock));
            if (left_leaf->size() == 0 && !left_leaf->prev_ && !left_leaf->next_.ptr) {
                node_type* only_child = in->child_[1];
                if (!only_child->try_lock(ti.lock_fence(tc_internode_lock))) {
                    left_leaf->unlock();
                    in->unlock();
                    continue;
                }

                // child_[0] 没有独立分隔键槽位；把唯一有效子树左移到 child_[0]，
                // 并把 nkeys_ 归零，就把 root 规整成了现有收缩逻辑可处理的形态。
                left_leaf->mark_deleted();
                in->child_[0] = only_child;
                in->child_[1] = nullptr;
                in->nkeys_ = 0;
                only_child->set_parent(in);
                only_child->unlock();
                left_leaf->unlock();
                left_leaf->deallocate_rcu(ti);
            } else {
                left_leaf->unlock();
                goto unlock_layer;
            }
        }

        node_type *child = in->child_[0];
        if (!child) {
            goto unlock_layer;
        }
        if (!child->try_lock(ti.lock_fence(tc_internode_lock))) {
            in->unlock();
            continue;
        }
        if (in->has_directory_meta()) {
            // 在 LHMFS 语义里，这一层对应真实目录对象。
            // 当 root internode 已经冗余到只剩唯一 child 时，不能直接把 old root 丢掉，
            // 而要让真正被提升的新 root 接住目录元数据。
            if (child->isleaf()) {
                leaf_type* child_leaf = static_cast<leaf_type*>(child);
                // 如果 leaf 仍和其他叶子有链表相连，说明它并不是这层唯一叶子，
                // 不能在这里复制替换。
                if (child_leaf->prev_ || child_leaf->next_.ptr) {
                    child->unlock();
                    in->unlock();
                    return false;
                }

                leaf_type* new_root =
                    leaf_type::clone_root_with_meta(child_leaf, *in->directory_meta(), ti);
                n_->lv_[kx_.p] = new_root;

                child->unlock();
                in->mark_split();
                in->set_parent(new_root);  // 让并发读者从 old root 修正到新 root
                in->unlock();
                child_leaf->deallocate_rcu(ti);
                in->deallocate_rcu(ti);
                return false;
            }

            // 唯一 child 仍然是 internode 时，同样采用“复制新 root”的办法：
            // 新 root 接住目录元数据，旧 root 与旧 child 通过 RCU 延迟回收。
            internode_type* child_in = static_cast<internode_type*>(child);
            internode_type* new_root =
                internode_type::clone_root_with_meta(child_in, *in->directory_meta(), ti);
            n_->lv_[kx_.p] = new_root;

            child->unlock();
            in->mark_split();
            in->set_parent(new_root);  // 让并发读者从 old root 修正到新 root
            in->unlock();
            child_in->deallocate_rcu(ti);
            in->deallocate_rcu(ti);
            return false;
        }
        child->make_layer_root();
        n_->lv_[kx_.p] = child;
        child->unlock();
        in->mark_split();
        in->set_parent(child);  // ensure concurrent reader finds true root
        // NB: now in->parent() might weirdly be a LEAF!
        in->unlock();
        in->deallocate_rcu(ti);
    }

    {
        leaf_type* lf = static_cast<leaf_type*>(layer);
        if (lf->size() > 0) {
            return false;
        }
        lf->lock(*lf, ti.lock_fence(tc_leaf_lock));
        if (!lf->is_root() || lf->size() > 0) {
            goto unlock_layer;
        }

        // 在 LHMFS 语义下，带 directory_meta 的空 leaf root 代表“空目录”，
        // 而不是 Masstree 里的无意义空技术层，因此这里不能直接回收。
        if (lf->has_directory_meta()) {
            lf->unlock();
            return false;
        }

        // child is an empty leaf: kill it
        masstree_invariant(!lf->prev_ && !lf->next_.ptr);
        masstree_invariant(!lf->deleted());
        masstree_invariant(!lf->deleted_layer());
        if (P::need_phantom_epoch
            && circular_int<typename P::phantom_epoch_type>::less(n_->phantom_epoch_[0], lf->phantom_epoch_[0])) {
            n_->phantom_epoch_[0] = lf->phantom_epoch_[0];
        }
        lf->mark_deleted_layer();   // NB DO NOT mark as deleted (see above)
        lf->unlock();
        lf->deallocate_rcu(ti);
        return true;
    }

 unlock_layer:
    layer->unlock();
    return false;
}

template <typename P>
struct gc_layer_rcu_callback : public P::threadinfo_type::mrcu_callback {
    typedef typename P::threadinfo_type threadinfo;
    node_base<P>* root_;
    int len_;
    char s_[0];
    gc_layer_rcu_callback(node_base<P>* root, Str prefix)
        : root_(root), len_(prefix.length()) {
        memcpy(s_, prefix.data(), len_);
    }
    void operator()(threadinfo& ti);
    size_t size() const {
        return len_ + sizeof(*this);
    }
    static void make(node_base<P>* root, Str prefix, threadinfo& ti);
};

template <typename P>
void gc_layer_rcu_callback<P>::operator()(threadinfo& ti)
{
    while (!root_->is_root()) {
        root_ = root_->maybe_parent();
    }
    if (!root_->deleted()) {    // if not destroying tree...
        tcursor<P> lp(root_, s_, len_);
        bool do_remove = lp.gc_layer(ti);
        if (!do_remove || !lp.finish_remove(ti)) {
            lp.n_->unlock();
        }
        ti.deallocate(this, size(), memtag_masstree_gc);
    }
}

template <typename P>
void gc_layer_rcu_callback<P>::make(node_base<P>* root, Str prefix,
                                    threadinfo& ti)
{
    size_t sz = prefix.len + sizeof(gc_layer_rcu_callback<P>);
    void *data = ti.allocate(sz, memtag_masstree_gc);
    gc_layer_rcu_callback<P> *cb =
        new(data) gc_layer_rcu_callback<P>(root, prefix);
    ti.rcu_register(cb);
}

template <typename P>
bool tcursor<P>::finish_remove(threadinfo& ti) {
    if (n_->modstate_ == leaf<P>::modstate_insert) {
        n_->mark_insert();
        n_->modstate_ = leaf<P>::modstate_remove;
    }

    permuter_type perm(n_->permutation_);
    perm.remove(kx_.i);
    n_->permutation_ = perm.value();
    if (perm.size()) {
        return false;
    } else {
        return remove_leaf(n_, root_, ka_.prefix_string(), ti);
    }
}

template <typename P>
bool tcursor<P>::remove_layer_edge(threadinfo& ti) {
    masstree_precondition(state_ < 0);
    masstree_precondition(n_->is_layer(kx_.p));
    bool removed_leaf = finish_remove(ti);
    if (!removed_leaf) {
        n_->unlock();
    }
    return true;
}

template <typename P>
bool tcursor<P>::remove_leaf(leaf_type* leaf, node_type* root,
                             Str prefix, threadinfo& ti)
{
    if (!leaf->prev_) {
        if (!leaf->next_.ptr && !prefix.empty()) {
            gc_layer_rcu_callback<P>::make(root, prefix, ti);
        }
        return false;
    }

    // mark leaf deleted, RCU-free
    leaf->mark_deleted();
    leaf->deallocate_rcu(ti);

    // Ensure node that becomes responsible for our keys has its phantom epoch
    // kept up to date
    while (P::need_phantom_epoch) {
        leaf_type *prev = leaf->prev_;
        typename P::phantom_epoch_type prev_ts = prev->phantom_epoch();
        while (circular_int<typename P::phantom_epoch_type>::less(prev_ts, leaf->phantom_epoch())
               && !bool_cmpxchg(&prev->phantom_epoch_[0], prev_ts, leaf->phantom_epoch())) {
            prev_ts = prev->phantom_epoch();
        }
        fence();
        if (prev == leaf->prev_) {
            break;
        }
    }

    // Unlink leaf from doubly-linked leaf list
    btree_leaflink<leaf_type>::unlink(leaf);

    // Remove leaf from tree, collapse trivial chains, and rewrite
    // ikey bounds.
    ikey_type ikey = leaf->ikey_bound();
    node_type* n = leaf;
    node_type* replacement = nullptr;

    while (true) {
        internode_type *p = n->locked_parent(ti);
        p->mark_insert();
        masstree_invariant(!p->deleted());

        int kp = internode_type::bound_type::upper(ikey, *p);
        masstree_invariant(kp == 0 || p->ikey0_[kp - 1] <= ikey); // NB ikey might not equal!
        masstree_invariant(p->child_[kp] == n);

        p->child_[kp] = replacement;

        if (replacement) {
            replacement->set_parent(p);
        } else if (kp > 0) {
            p->shift_down(kp - 1, kp, p->nkeys_ - kp);
            --p->nkeys_;
        }

        if (kp <= 1 && p->nkeys_ > 0 && !p->child_[0]) {
            redirect(p, ikey, p->ikey0_[0], ti);
            ikey = p->ikey0_[0];
        }

        n->unlock();
        n = p;

        if (p->nkeys_ || p->is_root()) {
            break;
        }

        p->mark_deleted();
        p->deallocate_rcu(ti);
        replacement = p->child_[0];
        p->child_[0] = nullptr;
    }

    n->unlock();
    return true;
}

template <typename P>
void tcursor<P>::redirect(internode_type* n, ikey_type ikey,
                          ikey_type replacement_ikey, threadinfo& ti)
{
    int kp = -1;
    do {
        internode_type* p = n->locked_parent(ti);
        if (kp >= 0) {
            n->unlock();
        }
        kp = internode_type::bound_type::upper(ikey, *p);
        masstree_invariant(p->child_[kp] == n);
        if (kp > 0) {
            // NB p->ikey0_[kp - 1] might not equal ikey
            p->ikey0_[kp - 1] = replacement_ikey;
        }
        n = p;
    } while (kp == 0 || (kp == 1 && !n->child_[0]));
    n->unlock();
}

template <typename P>
struct destroy_rcu_callback : public P::threadinfo_type::mrcu_callback {
    typedef typename P::threadinfo_type threadinfo;
    typedef typename node_base<P>::leaf_type leaf_type;
    typedef typename node_base<P>::internode_type internode_type;
    node_base<P>* root_;
    int count_;
    destroy_rcu_callback(node_base<P>* root)
        : root_(root), count_(0) {
    }
    void operator()(threadinfo& ti);
    static void make(node_base<P>* root, Str prefix, threadinfo& ti);
  private:
    static inline node_base<P>** link_ptr(node_base<P>* n);
    static inline void enqueue(node_base<P>* n, node_base<P>**& tailp);
};

template <typename P>
inline node_base<P>** destroy_rcu_callback<P>::link_ptr(node_base<P>* n) {
    if (n->isleaf())
        return &static_cast<leaf_type*>(n)->parent_;
    else
        return &static_cast<internode_type*>(n)->parent_;
}

template <typename P>
inline void destroy_rcu_callback<P>::enqueue(node_base<P>* n,
                                             node_base<P>**& tailp) {
    *tailp = n;
    tailp = link_ptr(n);
}

template <typename P>
void destroy_rcu_callback<P>::operator()(threadinfo& ti) {
    if (++count_ == 1) {
        while (!root_->is_root()) {
            root_ = root_->maybe_parent();
        }
        root_->lock();
        root_->mark_deleted_tree(); // i.e., deleted but not splitting
        root_->unlock();
        ti.rcu_register(this);
        return;
    }

    node_base<P>* workq;
    node_base<P>** tailp = &workq;
    enqueue(root_, tailp);

    while (node_base<P>* n = workq) {
        node_base<P>** linkp = link_ptr(n);
        if (linkp != tailp) {
            workq = *linkp;
        } else {
            workq = 0;
            tailp = &workq;
        }

        if (n->isleaf()) {
            leaf_type* l = static_cast<leaf_type*>(n);
            typename leaf_type::permuter_type perm = l->permutation();
            for (int i = 0; i != l->size(); ++i) {
                int p = perm[i];
                if (l->is_layer(p))
                    enqueue(l->lv_[p].layer(), tailp);
            }
            l->deallocate(ti);
        } else {
            internode_type* in = static_cast<internode_type*>(n);
            for (int i = 0; i != in->size() + 1; ++i) {
                if (in->child_[i])
                    enqueue(in->child_[i], tailp);
            }
            in->deallocate(ti);
        }
    }
    ti.deallocate(this, sizeof(this), memtag_masstree_gc);
}

template <typename P>
void basic_table<P>::destroy(threadinfo& ti) {
    if (root_) {
        void* data = ti.allocate(sizeof(destroy_rcu_callback<P>), memtag_masstree_gc);
        destroy_rcu_callback<P>* cb = new(data) destroy_rcu_callback<P>(root_);
        ti.rcu_register(cb);
        root_ = 0;
    }
}

} // namespace Masstree
#endif
