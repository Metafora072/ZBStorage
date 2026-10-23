#include "mds/lhm/LhmBridge.h"
#include "masstree_wrapper.h"
#include <filesystem>
#include <memory>
#include <stdexcept>

int main(int argc, char** argv) {
    try {
        if (argc != 2 || std::filesystem::exists(argv[1])) throw std::runtime_error("fresh directory required");
        std::filesystem::create_directories(argv[1]);
        MasstreeWrapper old_tree;
        old_tree.insert(MasstreeWrapper::Str("old-key"), 42);
        const auto old_epoch = globalepoch;
        const auto old_active = active_epoch;
        auto path = std::string(argv[1]) + "/coexist.meta";
        std::unique_ptr<zb_lhm_context, decltype(&zb_lhm_close)> ctx(zb_lhm_open(path.c_str()), zb_lhm_close);
        zb_lhm_inode inode{}, by_id{};
        if (!ctx || !zb_lhm_create(ctx.get(), "/file", 0) ||
            !zb_lhm_lookup(ctx.get(), "/file", &inode) ||
            !zb_lhm_read_inode(ctx.get(), inode.inode_id, &by_id) ||
            inode.inode_id != by_id.inode_id) throw std::runtime_error("LHM failed alongside original Masstree");
        ctx.reset();
        uint64_t value = 0;
        if (!old_tree.search(MasstreeWrapper::Str("old-key"), value) || value != 42 ||
            globalepoch != old_epoch || active_epoch != old_active)
            throw std::runtime_error("original Masstree corrupted by LHM");
        old_tree.insert(MasstreeWrapper::Str("after-close"), 99);
        if (!old_tree.search(MasstreeWrapper::Str("after-close"), value) || value != 99)
            throw std::runtime_error("original Masstree unusable after bridge close");
        std::cout << "PASS: original ZB Masstree and LHM coexist in one process\n";
    } catch (const std::exception& ex) { std::cerr << ex.what() << '\n'; return 1; }
}
