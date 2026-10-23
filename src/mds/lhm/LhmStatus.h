#pragma once
#include "LhmBridge.h"
#include "mds/storage/NamespaceStore.h"
namespace zb::mds {
inline NamespaceStatus LhmStatus(int code) {
    switch (code) {
    case ZB_LHM_OK: return {};
    case ZB_LHM_MISSING: return {zb::rpc::MDS_NOT_FOUND,"path or inode not found"};
    case ZB_LHM_EXISTS: return {zb::rpc::MDS_ALREADY_EXISTS,"path exists or is reserved"};
    case ZB_LHM_INVALID: return {zb::rpc::MDS_INVALID_ARGUMENT,"invalid namespace argument"};
    case ZB_LHM_NOT_EMPTY: return {zb::rpc::MDS_NOT_EMPTY,"directory not empty"};
    default: return {zb::rpc::MDS_INTERNAL_ERROR,"LHM storage or namespace operation failed"};
    }
}
inline bool LhmReadResult(int code,std::string* error) {
    if (error) *error=(code==ZB_LHM_OK || code==ZB_LHM_MISSING)?"":LhmStatus(code).message;
    return code==ZB_LHM_OK;
}
}
