#pragma once
#include <functional>
#include "mds/storage/NamespaceStore.h"

namespace zb::mds {
// Storage-independent location query. Resolve reads identity/attributes first;
// complete string locations take precedence over compact numeric inode fields.
class FileLocationResolver {
public:
    virtual ~FileLocationResolver() = default;
    virtual NamespaceStatus Resolve(uint64_t inode_id, zb::rpc::FileLocationView*) const = 0;
};

struct FileLocationSources {
    // false + empty error means absent; nonempty error means storage failure.
    std::function<bool(uint64_t, zb::rpc::InodeAttr*, std::string*)> read_inode;
    std::function<bool(uint64_t, bool optical, std::string*, std::string*)> read_record;
    std::function<bool(const std::string&, uint32_t, std::string*)> resolve_disk;
    std::function<void(const std::string&, std::string*, uint64_t*)> resolve_node;
    std::function<bool(zb::rpc::OpticalFileLocation*, bool packed, std::string*)> resolve_optical;
};

class RecordFileLocationResolver final : public FileLocationResolver {
public:
    explicit RecordFileLocationResolver(FileLocationSources sources) : sources_(std::move(sources)) {}
    NamespaceStatus Resolve(uint64_t, zb::rpc::FileLocationView*) const override;
    NamespaceStatus ResolveKnownInode(const zb::rpc::InodeAttr&, zb::rpc::FileLocationView*) const;
private:
    FileLocationSources sources_;
};
}
