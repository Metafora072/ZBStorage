#include "storage_model/NamespaceModel.h"
#include "storage_model/NodeModel.h"
#include "storage_model/OpticalModel.h"
#include "storage_model/WorkflowModel.h"

#include <iostream>
#include <string>

namespace {

bool Expect(bool value, const std::string& message) {
    if (!value) {
        std::cerr << "FAIL: " << message << '\n';
    }
    return value;
}

zb::storage_model::FileId MakeFileId(uint8_t seed) {
    zb::storage_model::FileId id;
    id.user_id = 1;
    id.namespace_id = 2;
    id.directory_path_hash.fill(seed);
    id.file_name_hash.fill(static_cast<uint8_t>(seed + 1));
    return id;
}

bool TestIdentifiersAndNodeInventory() {
    using namespace zb::storage_model;
    const FileId id = MakeFileId(3);
    if (!Expect(!IsZeroFileId(id), "non-zero FileId") ||
        !Expect(FileIdHex(id).size() == 48, "FileId has a 24-byte/48-hex representation") ||
        !Expect(IsValidOpticalLibraryId(kMaxOpticalLibraryId), "24-bit library id") ||
        !Expect(!IsValidOpticalLibraryId(kMaxOpticalLibraryId + 1), "reject oversized library id")) {
        return false;
    }

    NodeInventorySnapshot inventory;
    inventory.node_id = "storage-1";
    inventory.devices.push_back({"disk-1", DeviceKind::kHdd, 1000, 400, true});
    inventory.devices.push_back({"disk-2", DeviceKind::kHdd, 2000, 500, true});
    std::string error;
    OpticalLibraryStatus optical;
    optical.observed = true;
    optical.total_slots = 10000;
    optical.local_disc_count = 2;
    optical.recorded_disc_count = 1;
    optical.blank_disc_count = 1;
    const bool valid_optical = ValidateOpticalLibraryStatus(optical, &error);
    optical.recording_disc_count = 1;
    const bool invalid_optical = !ValidateOpticalLibraryStatus(optical, &error);
    return Expect(kStorageModelVersion == 2, "public model version") &&
           Expect(ValidateNodeInventorySnapshot(inventory, &error), error) &&
           Expect(DeviceUsedBytes(inventory.devices.front()) == 600, "per-device used bytes") &&
           Expect(InventoryCapacityBytes(inventory) == 3000, "inventory capacity") &&
           Expect(InventoryFreeBytes(inventory) == 900, "inventory free bytes") &&
           Expect(InventoryUsedBytes(inventory) == 2100, "inventory used bytes") &&
           Expect(InventoryHealthyCapacityBytes(inventory) == 3000,
                  "healthy inventory capacity") &&
           Expect(InventoryHealthyFreeBytes(inventory) == 900,
                  "healthy inventory free bytes") &&
           Expect(valid_optical, "valid optical aggregate state") &&
           Expect(invalid_optical, "optical state counts must equal local discs");
}

bool TestDiscSlotIndexAndLibrary() {
    using namespace zb::storage_model;
    DiscSlotIndex index;
    std::string error;
    if (!Expect(index.Insert(100, 4, &error), error) ||
        !Expect(index.Insert(101, 5, &error), error) ||
        !Expect(index.Insert(103, 8, &error), error) ||
        !Expect(!index.Insert(104, 8, &error), "reject occupied slot")) {
        return false;
    }
    OpticalSlotIndex slot = 0;
    const auto segments = index.SegmentSnapshot();
    if (!Expect(index.GetSlot(101, &slot) && slot == 5, "reverse slot lookup") ||
        !Expect(segments.size() == 2, "contiguous mappings are compressed") ||
        !Expect(index.PointSnapshot().size() == 1, "isolated mapping snapshot")) {
        return false;
    }

    OpticalLibraryInventory library;
    library.library_id = 7;
    library.node_id = "optical-7";
    library.slots = {{4, 100}, {5, 101}};
    OpticalDisc first;
    first.disc_id = 100;
    first.library_id = 7;
    first.slot_index = 4;
    OpticalDisc second = first;
    second.disc_id = 101;
    second.slot_index = 5;
    library.discs = {first, second};
    return Expect(ValidateOpticalLibraryInventory(library, &error), error);
}

bool TestFileAndWorkflow() {
    using namespace zb::storage_model;
    FileRecord file;
    file.file_id = MakeFileId(4);
    file.attributes.inode_id = 99;
    file.attributes.size_bytes = 10;
    file.state = FileLifecycleState::kArchived;
    file.placement.tier = StorageTier::kOptical;
    file.placement.optical_extents.push_back({7, 100, 8, 0, 4096, 10});
    std::string error;
    if (!Expect(ValidateFileRecord(file, &error), error)) {
        return false;
    }

    SegmentManifest manifest;
    manifest.migration_id = "migration-1";
    manifest.target_library_id = 7;
    TransferChunk chunk;
    chunk.chunk_id = "chunk-1";
    chunk.source = {"storage-1", 1000, 100};
    chunk.files.push_back({file.file_id, 0, 0, 10});
    manifest.chunks.push_back(chunk);
    if (!Expect(ValidateSegmentManifest(manifest, &error), error)) {
        return false;
    }

    BatchReadPlan plan;
    plan.request_id = "read-1";
    ReadOperation op;
    op.file_id = file.file_id;
    op.image_id = 8;
    op.image_offset = 4096;
    op.length = 10;
    plan.libraries.push_back({7, {{100, 4, {op}}}});
    return Expect(ValidateBatchReadPlan(plan, &error), error);
}

} // namespace

int main() {
    if (!TestIdentifiersAndNodeInventory() ||
        !TestDiscSlotIndexAndLibrary() ||
        !TestFileAndWorkflow()) {
        return 1;
    }
    std::cout << "storage_model_test: PASS\n";
    return 0;
}
