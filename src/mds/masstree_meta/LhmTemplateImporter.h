#pragma once
#include "MasstreeImportService.h"
#include "../storage/NamespaceStore.h"
namespace zb::mds {
// Metadata template materialization only; never reuses source IDs or publishes legacy routes.
bool ImportTemplateIntoNamespace(NamespaceStore& target,
    const MasstreeImportService::TemplateImportRequest& request,
    MasstreeImportService::Result* result, std::string* error);
}
