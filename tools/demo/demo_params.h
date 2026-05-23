#pragma once

#include <string>
#include <unordered_map>
#include <vector>

namespace zb::demo {

struct ParsedCommand {
    bool ok{false};
    std::string raw;
    std::string action;
    std::vector<std::string> tokens;
    std::unordered_map<std::string, std::string> args;
    std::vector<std::string> positionals;
    std::string error;
};

ParsedCommand ParseCommandLine(const std::string& line);

} // namespace zb::demo
