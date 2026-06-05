#include "mds/masstree_meta/MasstreeOpticalLayoutTranslator.h"

#include <iostream>
#include <string>

namespace {

bool Expect(bool condition, const std::string& message) {
    if (!condition) {
        std::cerr << message << "\n";
        return false;
    }
    return true;
}

} // namespace

int main() {
    std::string error;
    zb::mds::MasstreeOpticalLayoutTranslation translation;
    if (!zb::mds::MasstreeOpticalLayoutTranslation::BuildForLegacyCursor({0, 8999, 9, 1},
                                                                         &translation,
                                                                         &error)) {
        std::cerr << error << "\n";
        return 1;
    }
    if (!Expect(translation.cutover_global_image_id == 90000, "unexpected cutover global image id") ||
        !Expect(translation.cutover_disc_ordinal == 4500, "unexpected sealed disc count")) {
        return 1;
    }

    const zb::mds::MasstreeOpticalLayoutTranslator translator(translation);
    zb::mds::MasstreeOpticalClusterCursor next;
    if (!translator.NextWritableCursor(&next, &error) ||
        !Expect(next.node_index == 0 && next.disk_index == 4500 &&
                    next.image_index_in_disk == 0 && next.image_used_bytes == 0,
                "unexpected next writable cursor")) {
        std::cerr << error << "\n";
        return 1;
    }

    uint32_t node = 0;
    uint32_t disc = 0;
    uint32_t image = 0;
    if (!translator.TranslateLegacyToActive(0, 9000, 0, &node, &disc, &image, &error) ||
        !Expect(node == 0 && disc == 4500 && image == 0, "legacy to active translation mismatch")) {
        std::cerr << error << "\n";
        return 1;
    }

    bool uses_legacy_alias = false;
    if (!translator.ResolveActiveReadAlias(0, 4499, 19,
                                           &uses_legacy_alias,
                                           &node,
                                           &disc,
                                           &image,
                                           &error) ||
        !Expect(uses_legacy_alias && node == 0 && disc == 8999 && image == 9,
                "legacy read alias mismatch")) {
        std::cerr << error << "\n";
        return 1;
    }
    if (!translator.ResolveActiveReadAlias(0, 4500, 0,
                                           &uses_legacy_alias,
                                           &node,
                                           &disc,
                                           &image,
                                           &error) ||
        !Expect(!uses_legacy_alias && node == 0 && disc == 4500 && image == 0,
                "uniform read location mismatch")) {
        std::cerr << error << "\n";
        return 1;
    }

    zb::mds::MasstreeOpticalLayoutTranslation decoded;
    if (!zb::mds::MasstreeOpticalLayoutTranslation::Decode(translation.Encode(), &decoded, &error) ||
        !Expect(decoded.cutover_global_image_id == translation.cutover_global_image_id &&
                    decoded.cutover_disc_ordinal == translation.cutover_disc_ordinal,
                "translation record round trip mismatch")) {
        std::cerr << error << "\n";
        return 1;
    }
    return 0;
}
