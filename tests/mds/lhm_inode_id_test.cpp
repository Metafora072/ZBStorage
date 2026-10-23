#include <iostream>
#include <stdexcept>
#include "mds/storage/LhmInodeId.h"

using ID = zb::mds::LhmInodeId;
static void Check(bool value) {
    if (!value) throw std::runtime_error("inode ID contract failed");
}
int main() {
    try {
        Check(ID::Decode(1)->kind == ID::Kind::Root);
        // Known values independently taken from the existing LHM encoding.
        Check(ID::File(7, 62).value() == 0x000000070000003eULL);
        Check(ID::Directory(7).value() == 0x8000000000000007ULL);
        for (uint32_t block : {1U, 7U, ID::kMaxBlock}) {
            for (uint32_t slot = 0; slot < 63; ++slot) {
                auto id = ID::File(block, slot).value();
                auto address = ID::Decode(id).value();
                Check(address.kind == ID::Kind::File && address.block == block && address.slot == slot);
                Check(id != ID::Directory(block).value() && id != ID::kRoot);
            }
            auto dir = ID::Decode(ID::Directory(block).value()).value();
            Check(dir.kind == ID::Kind::Directory && dir.block == block);
        }
        Check(!ID::File(0, 0) && !ID::File(1, 63) && !ID::File(0x80000000U, 0));
        Check(!ID::Directory(0) && !ID::Directory(0x80000000U));
        for (uint64_t invalid : {0ULL, 2ULL, 0x000000010000003fULL,
                                 0x8000000000000000ULL, 0x8000000100000001ULL,
                                 0xffffffffffffffffULL}) Check(!ID::Decode(invalid));
        std::cout << "PASS: LHM inode ID layout, root, all slots, boundaries and malformed IDs\n";
    } catch (const std::exception& ex) {
        std::cerr << ex.what() << '\n';
        return 1;
    }
}
