#pragma once

#include "sim/masstree/SimMasstreeTypes.h"

namespace zb::sim::masstree {

bool SaveManifest(const SimMasstreeManifest& manifest, std::string* error);
bool LoadManifest(const std::filesystem::path& path, SimMasstreeManifest* manifest, std::string* error);

} // namespace zb::sim::masstree
