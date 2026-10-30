#ifndef MASSTREELHM_MASSTREE_DIRECTORY_INDEX_HH
#define MASSTREELHM_MASSTREE_DIRECTORY_INDEX_HH

#include <cstdint>
#include <cstring>
#include <string>
#include <utility>
#include <vector>

#include "compiler.hh"
#include "masstree.hh"
#include "masstree_get.hh"
#include "masstree_insert.hh"
#include "masstree_remove.hh"
#include "masstree_tcursor.hh"
#include "namespace_types.hh"

namespace MasstreeLHM {

#if defined(__GNUC__) || defined(__clang__)
#define LHM_ALWAYS_INLINE inline __attribute__((always_inline))
#else
#define LHM_ALWAYS_INLINE inline
#endif

// MasstreeDirectoryIndex owns the in-memory directory route tree.
//
// This class is intentionally small in the first refactoring step: it only
// owns table lifetime and exposes the same low-level table/root handles that
// LhmNamespace currently uses. Later steps will move directory route
// primitives such as locate, lookup child, create layer, and attach/detach
// into this class.
class MasstreeDirectoryIndex {
  public:
    using table_type = Masstree::basic_table<namespace_table_params>;
    using unlocked_cursor_type = typename table_type::unlocked_cursor_type;
    using cursor_type = Masstree::tcursor<namespace_table_params>;
    using node_type = typename table_type::node_type;
    using table_value_type = typename table_type::value_type;
    using value_type = namespace_entry;

    struct directory_edge_lookup {
        bool found = false;
        node_type* child_root = nullptr;
        value_type entry{};
    };

    struct child_slot_lookup {
        bool found = false;
        bool is_layer = false;
        node_type* child_root = nullptr;
        value_type value{};
    };

    struct single_component_key {
        char bytes[sizeof(uint64_t)];

        lcdf::Str as_str() const {
            return lcdf::Str(bytes, sizeof(bytes));
        }
    };

    void initialize(threadinfo& ti) {
        table_.initialize(ti);
    }

    void destroy(threadinfo& ti) {
        table_.destroy(ti);
    }

    // Explicit layer insertions can split the root without updating table_.
    // Direct traversal must start at the current root, not the old left leaf.
    node_type* root() {
        return canonicalize_directory_root(table_.root());
    }

    const node_type* root() const {
        return canonicalize_directory_root(table_.root());
    }

    table_type& table() {
        return table_;
    }

    const table_type& table() const {
        return table_;
    }

    LHM_ALWAYS_INLINE static single_component_key make_single_component_key(
        uint64_t component_hash) {
        single_component_key key;
        uint64_t be = host_to_net_order(component_hash);
        memcpy(key.bytes, &be, sizeof(be));
        return key;
    }

    LHM_ALWAYS_INLINE static node_type* canonicalize_directory_root(node_type* root) {
        while (true) {
            node_type* next = root->maybe_parent();
            if (next == root) {
                return root;
            }
            root = next;
        }
    }

    LHM_ALWAYS_INLINE static const node_type* canonicalize_directory_root(
        const node_type* root) {
        while (true) {
            const node_type* next = root->maybe_parent();
            if (next == root) {
                return root;
            }
            root = next;
        }
    }

    LHM_ALWAYS_INLINE bool locate_directory_root(const ParsedPath& parsed,
                                                 const node_type*& out,
                                                 threadinfo& ti) const {
        out = root();
        if (parsed.normalized_path == "/") {
            return true;
        }

        for (uint64_t component_hash : parsed.hashes) {
            single_component_key edge_key = make_single_component_key(component_hash);
            unlocked_cursor_type cursor(const_cast<node_type*>(out), edge_key.as_str().data(),
                                        edge_key.as_str().length());
            cursor.find_unlocked_edge(ti);
            int state = cursor.state();
            if (state >= 0 || !cursor.is_layer()) {
                return false;
            }
            out = canonicalize_directory_root(cursor.layer_root());
        }
        return true;
    }

    LHM_ALWAYS_INLINE bool lookup_child_slot_from_parent_root(node_type* parent_root,
                                                              uint64_t child_hash,
                                                              child_slot_lookup& out,
                                                              threadinfo& ti) const {
        out = child_slot_lookup{};
        single_component_key child_key = make_single_component_key(child_hash);
        unlocked_cursor_type cursor(parent_root, child_key.as_str().data(),
                                    child_key.as_str().length());
        cursor.find_unlocked_edge(ti);
        int state = cursor.state();
        if (state < 0 && cursor.is_layer()) {
            out.found = true;
            out.is_layer = true;
            out.child_root = canonicalize_directory_root(cursor.layer_root());
            return true;
        }
        if (state <= 0) {
            return false;
        }
        out.found = true;
        out.value = make_namespace_entry(cursor.value(), std::string());
        return true;
    }

    LHM_ALWAYS_INLINE bool lookup_child_from_parent_root(node_type* parent_root,
                                                         const std::string& child_name,
                                                         uint64_t child_hash,
                                                         value_type& out,
                                                         threadinfo& ti) const {
        child_slot_lookup slot;
        if (!lookup_child_slot_from_parent_root(parent_root, child_hash, slot, ti)) {
            return false;
        }

        if (slot.is_layer) {
            const node_type* child_root = slot.child_root;
            if (child_root->has_directory_meta()) {
                const directory_meta* meta = child_root->directory_meta();
                if (meta != nullptr) {
                    out = make_namespace_entry(entry_kind::directory,
                                               make_inode_ref(meta->head_block_id, 0),
                                               child_name);
                    return true;
                }
            }
            return false;
        }

        const value_type& raw = slot.value;
        if (entry_name_equals(raw, child_name)) {
            out = raw;
            return true;
        }
        return false;
    }

    LHM_ALWAYS_INLINE bool lookup_directory_edge_from_parent_root(
        node_type* parent_root,
        const std::string& child_name,
        uint64_t child_hash,
        directory_edge_lookup& out,
        threadinfo& ti) const {
        single_component_key child_key = make_single_component_key(child_hash);
        cursor_type cursor(parent_root, child_key.as_str().data(), child_key.as_str().length());
        cursor.find_locked_edge(ti);
        int state = cursor.state();
        if (state < 0 && cursor.is_layer()) {
            node_type* child_root = canonicalize_directory_root(cursor.layer_root());
            cursor.finish_read();
            if (!child_root->has_directory_meta()) {
                return false;
            }
            const directory_meta* meta = child_root->directory_meta();
            if (meta == nullptr) {
                return false;
            }
            out.found = true;
            out.child_root = child_root;
            out.entry = make_namespace_entry(entry_kind::directory,
                                             make_inode_ref(meta->head_block_id, 0),
                                             child_name);
            return true;
        }
        cursor.finish_read();
        return false;
    }

    LHM_ALWAYS_INLINE bool create_directory_layer(node_type* parent_root,
                                                  uint64_t child_hash,
                                                  const directory_meta& meta,
                                                  node_type*& directory_root,
                                                  threadinfo& ti) const {
        single_component_key edge_key = make_single_component_key(child_hash);
        cursor_type cursor(parent_root, edge_key.as_str().data(), edge_key.as_str().length());
        return cursor.create_layer_with_meta(directory_root, meta, ti);
    }

    LHM_ALWAYS_INLINE bool attach_existing_directory_layer(node_type* parent_root,
                                                           uint64_t child_hash,
                                                           node_type* child_root,
                                                           threadinfo& ti) const {
        single_component_key edge_key = make_single_component_key(child_hash);
        cursor_type cursor(parent_root, edge_key.as_str().data(), edge_key.as_str().length());
        return cursor.attach_existing_layer(child_root, ti);
    }

    LHM_ALWAYS_INLINE bool remove_directory_layer(node_type* parent_root,
                                                  uint64_t child_hash,
                                                  node_type* expected_child_root,
                                                  threadinfo& ti) const {
        single_component_key edge_key = make_single_component_key(child_hash);
        cursor_type cursor(parent_root, edge_key.as_str().data(), edge_key.as_str().length());
        cursor.find_locked_edge(ti);
        int state = cursor.state();
        node_type* actual_root = (state < 0 && cursor.is_layer())
            ? canonicalize_directory_root(cursor.layer_root())
            : nullptr;
        if (state >= 0 || !cursor.is_layer() || actual_root != expected_child_root) {
            cursor.finish_read();
            return false;
        }
        return cursor.remove_layer_edge(ti);
    }

    bool directory_root_has_children(const node_type* node) const {
        if (node->isleaf()) {
            const typename node_type::leaf_type* leaf =
                static_cast<const typename node_type::leaf_type*>(node);
            return leaf->size() != 0;
        }

        const typename node_type::internode_type* in =
            static_cast<const typename node_type::internode_type*>(node);
        for (int i = 0; i != in->size() + 1; ++i) {
            if (in->child_[i] && directory_root_has_children(in->child_[i])) {
                return true;
            }
        }
        return false;
    }

    LHM_ALWAYS_INLINE bool force_remove_child_slot(node_type* parent_root,
                                                   uint64_t child_hash,
                                                   threadinfo& ti) const {
        single_component_key key = make_single_component_key(child_hash);
        cursor_type cursor(parent_root, key.as_str().data(), key.as_str().length());
        cursor.find_locked_edge(ti);
        int state = cursor.state();
        if (state < 0 && cursor.is_layer()) {
            return cursor.remove_layer_edge(ti);
        }
        if (state > 0) {
            cursor.finish(-1, ti);
            return true;
        }
        cursor.finish_read();
        return true;
    }

    LHM_ALWAYS_INLINE bool remove_child_slot_fast(node_type* parent_root,
                                                  uint64_t child_hash,
                                                  value_type* removed_out,
                                                  threadinfo& ti) const {
        single_component_key key = make_single_component_key(child_hash);
        cursor_type cursor(parent_root, key.as_str().data(), key.as_str().length());
        cursor.find_locked_edge(ti);
        int state = cursor.state();

        if (state < 0 && cursor.is_layer()) {
            if (removed_out != nullptr) {
                node_type* child_root = canonicalize_directory_root(cursor.layer_root());
                if (!child_root->has_directory_meta()) {
                    cursor.finish_read();
                    return false;
                }
                const directory_meta* meta = child_root->directory_meta();
                if (meta == nullptr) {
                    cursor.finish_read();
                    return false;
                }
                *removed_out = make_namespace_entry(entry_kind::directory,
                                                    make_inode_ref(meta->head_block_id, 0),
                                                    std::string());
            }
            return cursor.remove_layer_edge(ti);
        }
        if (state <= 0) {
            cursor.finish_read();
            return false;
        }

        if (removed_out != nullptr) {
            *removed_out = make_namespace_entry(cursor.value(), std::string());
        }
        cursor.finish(-1, ti);
        return true;
    }

    bool remove_child_slot_checked(node_type* parent_root, uint64_t child_hash,
                                   const std::string& child_name,
                                   value_type* removed_out, threadinfo& ti) const {
        single_component_key key = make_single_component_key(child_hash);
        cursor_type cursor(parent_root, key.as_str().data(), key.as_str().length());
        cursor.find_locked_edge(ti);
        int state = cursor.state();

        if (state < 0 && cursor.is_layer()) {
            node_type* child_root = canonicalize_directory_root(cursor.layer_root());
            if (!child_root->has_directory_meta()) {
                cursor.finish_read();
                return false;
            }
            const directory_meta* meta = child_root->directory_meta();
            if (meta == nullptr) {
                cursor.finish_read();
                return false;
            }
            if (directory_root_has_children(child_root)) {
                cursor.finish_read();
                return false;
            }
            if (removed_out != nullptr) {
                *removed_out = make_namespace_entry(entry_kind::directory,
                                                    make_inode_ref(meta->head_block_id, 0),
                                                    child_name);
            }
            return cursor.remove_layer_edge(ti);
        }

        if (state <= 0) {
            cursor.finish_read();
            return false;
        }

        value_type slot_value = make_namespace_entry(cursor.value(), child_name);
        if (!entry_is_file(slot_value)) {
            cursor.finish_read();
            return false;
        }
        if (removed_out != nullptr) {
            *removed_out = slot_value;
        }
        cursor.finish(-1, ti);
        return true;
    }

    void append_directory_children(const node_type* parent_root,
                                   const std::vector<uint64_t>& parent_hashes,
                                   std::vector<readdir_record>& out) const {
        append_directory_children_from_node(parent_root, parent_hashes, out);
    }

    // Seek one slot in this layer, never descend into child directory layers.
    // Resume with an exclusive hash, not a leaf pointer across RCU jobs. Internal
    // separator keys prune all subtrees before the cursor. Calls are serialized
    // by the ZB worker; selected subtree structure must remain unchanged.
    bool next_child_slot(const node_type* node, bool have_after, uint64_t after,
                         uint64_t& hash, value_type& entry, bool& found) const {
        found = false;
        if (!node) return false;
        if (node->isleaf()) {
            const auto* leaf = static_cast<const typename node_type::leaf_type*>(node);
            const auto perm = leaf->permutation();
            for (int i = 0; i < leaf->size(); ++i) {
                const int p = perm[i];
                const uint64_t key = static_cast<uint64_t>(leaf->ikey(p));
                if (have_after && key <= after) continue;
                hash = key;
                found = true;
                if (!leaf->is_layer(p)) {
                    entry = make_namespace_entry(leaf->lv_[p].value(), std::string());
                    return true;
                }
                const auto* child = canonicalize_directory_root(leaf->lv_[p].layer());
                if (!child->has_directory_meta() || !child->directory_meta()) return false;
                entry = make_namespace_entry(entry_kind::directory,
                    make_inode_ref(child->directory_meta()->head_block_id, 0), std::string());
                return true;
            }
            return true;
        }
        const auto* in = static_cast<const typename node_type::internode_type*>(node);
        int start = 0;
        if (have_after)
            while (start < in->size() && static_cast<uint64_t>(in->ikey(start)) <= after) ++start;
        for (int i = start; i <= in->size(); ++i) {
            if (!next_child_slot(in->child_[i], have_after, after, hash, entry, found)) return false;
            if (found) return true;
        }
        return true;
    }

    std::vector<subtree_record> scan_subtree(const ParsedPath& root, threadinfo& ti) const {
        subtree_scanner scanner;
        std::vector<subtree_record> results;
        scanner.root_hashes = &root.hashes;
        scanner.out = &results;
        table_.scan(lcdf::Str(""), false, scanner, ti);
        return results;
    }

    LHM_ALWAYS_INLINE bool insert_file_value(node_type* parent_root,
                                             uint64_t child_hash,
                                             const value_type& entry,
                                             bool overwrite_existing_value,
                                             threadinfo& ti) const {
        single_component_key key = make_single_component_key(child_hash);
        cursor_type cursor(parent_root, key.as_str().data(), key.as_str().length());
        bool already_present = cursor.find_insert(ti);
        if (already_present) {
            if (cursor.is_layer()) {
                cursor.finish_read();
                return false;
            }
            if (!overwrite_existing_value) {
                cursor.finish(0, ti);
                return false;
            }
            cursor.value() = make_route_value(entry);
            fence();
            cursor.finish(0, ti);
            return true;
        }

        cursor.value() = make_route_value(entry);
        fence();
        cursor.finish(1, ti);
        return true;
    }

  private:
    struct subtree_scanner {
        const std::vector<uint64_t>* root_hashes;
        std::vector<subtree_record>* out;

        template <typename SS, typename K>
        void visit_leaf(const SS&, const K&, threadinfo&) {
        }

        bool visit_value(lcdf::Str key, table_value_type value, threadinfo&) {
            std::vector<uint64_t> decoded = PathKey::decode(key);
            if (decoded.size() < root_hashes->size()) {
                return true;
            }
            for (size_t i = 0; i < root_hashes->size(); ++i) {
                if (decoded[i] != (*root_hashes)[i]) {
                    return true;
                }
            }

            subtree_record record;
            record.full_hashes = std::move(decoded);
            record.entry = make_namespace_entry(value, std::string());
            out->push_back(std::move(record));
            return true;
        }
    };

    void append_directory_children_from_node(const node_type* node,
                                             const std::vector<uint64_t>& parent_hashes,
                                             std::vector<readdir_record>& out) const {
        if (node->isleaf()) {
            const typename node_type::leaf_type* leaf =
                static_cast<const typename node_type::leaf_type*>(node);
            typename node_type::leaf_type::permuter_type perm = leaf->permutation();
            for (int i = 0; i != leaf->size(); ++i) {
                int p = perm[i];
                uint64_t child_hash = static_cast<uint64_t>(leaf->ikey(p));

                if (!leaf->is_layer(p)) {
                    value_type slot_value =
                        make_namespace_entry(leaf->lv_[p].value(), std::string());
                    readdir_record record;
                    record.full_hashes = parent_hashes;
                    record.full_hashes.push_back(child_hash);
                    record.child_component_hash = child_hash;
                    record.child_name = entry_name(slot_value);
                    record.entry = slot_value;
                    out.push_back(std::move(record));
                    continue;
                }

                // 命中目录 edge 时只恢复当前目录项，不下钻子目录。
                const node_type* child_root =
                    canonicalize_directory_root(leaf->lv_[p].layer());
                if (!child_root->has_directory_meta()) {
                    continue;
                }
                const directory_meta* meta = child_root->directory_meta();
                if (meta == nullptr) {
                    continue;
                }
                readdir_record record;
                record.full_hashes = parent_hashes;
                record.full_hashes.push_back(child_hash);
                record.child_component_hash = child_hash;
                record.child_name = std::string();
                record.entry = make_namespace_entry(entry_kind::directory,
                                                    make_inode_ref(meta->head_block_id, 0),
                                                    record.child_name);
                out.push_back(std::move(record));
            }
            return;
        }

        const typename node_type::internode_type* in =
            static_cast<const typename node_type::internode_type*>(node);
        for (int i = 0; i != in->size() + 1; ++i) {
            if (in->child_[i]) {
                append_directory_children_from_node(in->child_[i], parent_hashes, out);
            }
        }
    }

    table_type table_;
};

}  // namespace MasstreeLHM

#undef LHM_ALWAYS_INLINE

#endif
