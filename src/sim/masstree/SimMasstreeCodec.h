#pragma once

#include "sim/masstree/SimMasstreeTypes.h"

#include <istream>
#include <ostream>

namespace zb::sim::masstree {

bool WriteInodeRecord(std::ostream& out, const SimInodeRecord& record, std::string* error);
bool ReadInodeRecord(std::istream& in, SimInodeRecord* record, std::string* error);
bool WriteDentryRecord(std::ostream& out, const SimDentryRecord& record, std::string* error);
bool ReadDentryRecord(std::istream& in, SimDentryRecord* record, std::string* error);

} // namespace zb::sim::masstree
