/* Masstree
 * Eddie Kohler, Yandong Mao, Robert Morris
 * Copyright (c) 2012-2014 President and Fellows of Harvard College
 * Copyright (c) 2012-2014 Massachusetts Institute of Technology
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
#ifndef MASSTREE_TCURSOR_HH
#define MASSTREE_TCURSOR_HH 1
#include "small_vector.hh"
#include "masstree_key.hh"
#include "masstree_struct.hh"
namespace Masstree {
template <typename P> struct gc_layer_rcu_callback;

template <typename P>
class unlocked_tcursor {
  public:
    typedef node_base<P> node_type;
    typedef leaf<P> leaf_type;
    typedef typename P::value_type value_type;
    typedef key<typename P::ikey_type> key_type;
    typedef typename P::threadinfo_type threadinfo;
    typedef typename leaf<P>::nodeversion_type nodeversion_type;
    typedef typename nodeversion_type::value_type nodeversion_value_type;
    typedef typename leaf<P>::permuter_type permuter_type;

    inline unlocked_tcursor(const basic_table<P>& table, Str str)
        : ka_(str), lv_(leafvalue<P>::make_empty()),
          root_(table.root()) {
    }
    inline unlocked_tcursor(basic_table<P>& table, Str str)
        : ka_(str), lv_(leafvalue<P>::make_empty()),
          root_(table.fix_root()) {
    }
    inline unlocked_tcursor(const basic_table<P>& table,
                            const char* s, int len)
        : ka_(s, len), lv_(leafvalue<P>::make_empty()),
          root_(table.root()) {
    }
    inline unlocked_tcursor(basic_table<P>& table,
                            const char* s, int len)
        : ka_(s, len), lv_(leafvalue<P>::make_empty()),
          root_(table.fix_root()) {
    }
    inline unlocked_tcursor(const basic_table<P>& table,
                            const unsigned char* s, int len)
        : ka_(reinterpret_cast<const char*>(s), len),
          lv_(leafvalue<P>::make_empty()), root_(table.root()) {
    }
    inline unlocked_tcursor(basic_table<P>& table,
                            const unsigned char* s, int len)
        : ka_(reinterpret_cast<const char*>(s), len),
          lv_(leafvalue<P>::make_empty()), root_(table.fix_root()) {
    }
    inline unlocked_tcursor(const node_base<P>* root,
                            const char* s, int len)
        : ka_(s, len), lv_(leafvalue<P>::make_empty()), root_(root) {
    }
    inline unlocked_tcursor(node_base<P>* root,
                            const char* s, int len)
        : ka_(s, len), lv_(leafvalue<P>::make_empty()), root_(root) {
    }
    inline unlocked_tcursor(const node_base<P>* root,
                            const unsigned char* s, int len)
        : ka_(reinterpret_cast<const char*>(s), len),
          lv_(leafvalue<P>::make_empty()), root_(root) {
    }
    inline unlocked_tcursor(node_base<P>* root,
                            const unsigned char* s, int len)
        : ka_(reinterpret_cast<const char*>(s), len),
          lv_(leafvalue<P>::make_empty()), root_(root) {
    }

    bool find_unlocked(threadinfo& ti);
    bool find_unlocked_edge(threadinfo& ti);

    inline value_type value() const {
        return lv_.value();
    }
    inline leaf<P>* node() const {
        return n_;
    }
    inline permuter_type permutation() const {
        return perm_;
    }
    inline int compare_key(const key_type& a, int bp) const {
        return n_->compare_key(a, bp);
    }
    inline nodeversion_value_type full_version_value() const {
        static_assert(int(nodeversion_type::traits_type::top_stable_bits) >= int(leaf<P>::permuter_type::size_bits), "not enough bits to add size to version");
        return (v_.version_value() << leaf<P>::permuter_type::size_bits) + perm_.size();
    }
    inline int state() const {
        return state_;
    }
    inline bool is_layer() const {
        return state_ < 0;
    }
    inline node_type* layer_root() const {
        return lv_.layer();
    }

  private:
    leaf<P>* n_;
    key_type ka_;
    typename leaf<P>::nodeversion_type v_;
    permuter_type perm_;
    leafvalue<P> lv_;
    const node_base<P>* root_;
    int state_;
};

template <typename P>
class tcursor {
  public:
    typedef node_base<P> node_type;
    typedef leaf<P> leaf_type;
    typedef internode<P> internode_type;
    typedef typename P::value_type value_type;
    typedef leafvalue<P> leafvalue_type;
    typedef typename leaf_type::permuter_type permuter_type;
    typedef typename P::ikey_type ikey_type;
    typedef key<ikey_type> key_type;
    typedef typename leaf<P>::nodeversion_type nodeversion_type;
    typedef typename nodeversion_type::value_type nodeversion_value_type;
    typedef typename P::threadinfo_type threadinfo;
    static constexpr int new_nodes_size = 1; // unless we make a new trie newnodes will have at most 1 item
    typedef small_vector<std::pair<leaf_type*, nodeversion_value_type>, new_nodes_size> new_nodes_type;

    tcursor(basic_table<P>& table, Str str)
        : ka_(str), root_(table.fix_root()) {
    }
    tcursor(basic_table<P>& table, const char* s, int len)
        : ka_(s, len), root_(table.fix_root()) {
    }
    tcursor(basic_table<P>& table, const unsigned char* s, int len)
        : ka_(reinterpret_cast<const char*>(s), len), root_(table.fix_root()) {
    }
    tcursor(node_base<P>* root, const char* s, int len)
        : ka_(s, len), root_(root) {
    }
    tcursor(node_base<P>* root, const unsigned char* s, int len)
        : ka_(reinterpret_cast<const char*>(s), len), root_(root) {
    }

    inline bool has_value() const {
        return kx_.p >= 0;
    }
    inline value_type& value() const {
        return n_->lv_[kx_.p].value();
    }

    inline bool is_first_layer() const {
        return !ka_.is_shifted();
    }

    inline leaf<P>* node() const {
        return n_;
    }

    inline leaf_type* original_node() const {
        return original_n_;
    }

    inline nodeversion_value_type original_version_value() const {
        return original_v_;
    }

    inline nodeversion_value_type updated_version_value() const {
        return updated_v_;
    }

    inline const new_nodes_type &new_nodes() const {
        return new_nodes_;
    }

    inline bool find_locked(threadinfo& ti);
    inline bool find_insert(threadinfo& ti);
    // find_locked_edge 会停在当前层的匹配槽位上。
    // 与 find_locked 不同，如果命中的是 layer 入口，它不会自动下钻到子树，
    // 而是把调用者留在“父目录边”所在的叶子节点上。
    inline bool find_locked_edge(threadinfo& ti);

    inline void finish(int answer, threadinfo& ti);
    inline void finish_read() {
        n_->unlock();
    }

    inline nodeversion_value_type previous_full_version_value() const;
    inline nodeversion_value_type next_full_version_value(int state) const;
    inline int state() const {
        return state_;
    }
    inline bool is_layer() const {
        return state_ < 0;
    }
    inline node_type* layer_root() const {
        return n_->lv_[kx_.p].layer();
    }
    // create_layer 在当前层为一个不存在的精确 key 主动创建空子树入口。
    // 这为“空目录也必须真实存在”提供最小的内核原语。
    inline bool create_layer(node_type*& layer_root, threadinfo& ti);
    // create_layer_with_meta 是 leaf root special allocation 的第一步：
    // 新建的 layer root 会额外尾随一份 directory_meta。
    inline bool create_layer_with_meta(node_type*& layer_root,
                                       const MasstreeLHM::directory_meta& meta,
                                       threadinfo& ti);
    // attach_existing_layer 在当前层插入一个新的 layer edge，但不新建目录子树；
    // 而是把已有的 subtree root 直接挂到新父目录名下。
    inline bool attach_existing_layer(node_type* layer_root, threadinfo& ti);
    // remove_layer_edge 专门删除当前层命中的 layer 槽位。
    // 与普通 value 删除不同，这条路径由 find_locked_edge() 停在父目录边上，
    // 再显式调用 finish_remove() 进入 Masstree 原生的叶子删除/层压缩链。
    inline bool remove_layer_edge(threadinfo& ti);

  private:
    leaf_type* n_;
    key_type ka_;
    key_indexed_position kx_;
    node_base<P>* root_;
    int state_;

    leaf_type* original_n_;
    nodeversion_value_type original_v_;
    nodeversion_value_type updated_v_;
    new_nodes_type new_nodes_;

    inline node_type* reset_retry() {
        ka_.unshift_all();
        return root_;
    }

    bool make_new_layer(threadinfo& ti);
    bool make_split(threadinfo& ti);
    friend class leaf<P>;
    inline void finish_insert();
    inline bool finish_remove(threadinfo& ti);

    static void redirect(internode_type* n, ikey_type ikey,
                         ikey_type replacement, threadinfo& ti);
    /** Remove @a leaf from the Masstree rooted at @a rootp.
     * @param prefix String defining the path to the tree containing this leaf.
     *   If removing a leaf in layer 0, @a prefix is empty.
     *   If removing, for example, the node containing key "01234567ABCDEF" in the layer-1 tree
     *   rooted at "01234567", then @a prefix should equal "01234567". */
    static bool remove_leaf(leaf_type* leaf, node_type* root,
                            Str prefix, threadinfo& ti);

    bool gc_layer(threadinfo& ti);
    friend struct gc_layer_rcu_callback<P>;
};

template <typename P>
inline typename tcursor<P>::nodeversion_value_type
tcursor<P>::previous_full_version_value() const {
    static_assert(int(nodeversion_type::traits_type::top_stable_bits) >= int(leaf<P>::permuter_type::size_bits), "not enough bits to add size to version");
    return (n_->unlocked_version_value() << leaf<P>::permuter_type::size_bits) + n_->size();
}

template <typename P>
inline typename tcursor<P>::nodeversion_value_type
tcursor<P>::next_full_version_value(int state) const {
    static_assert(int(nodeversion_type::traits_type::top_stable_bits) >= int(leaf<P>::permuter_type::size_bits), "not enough bits to add size to version");
    typename node_base<P>::nodeversion_type v(*n_);
    v.unlock();
    nodeversion_value_type result = (v.version_value() << leaf<P>::permuter_type::size_bits) + n_->size();
    if (state < 0 && (state_ & 1))
        return result - 1;
    else if (state > 0 && state_ == 2)
        return result + 1;
    else
        return result;
}

} // namespace Masstree
#endif
