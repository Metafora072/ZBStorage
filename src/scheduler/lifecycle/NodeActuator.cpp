#include "NodeActuator.h"

#include <cstdlib>
#include <utility>

namespace zb::scheduler {

namespace {

std::string ReplaceAll(std::string text, const std::string& from, const std::string& to) {
    if (from.empty()) {
        return text;
    }
    size_t pos = 0;
    while ((pos = text.find(from, pos)) != std::string::npos) {
        text.replace(pos, from.size(), to);
        pos += to.size();
    }
    return text;
}

std::string ShellQuote(const std::string& value) {
    std::string out{"'"};
    for (char ch : value) out += ch == '\'' ? "'\\''" : std::string(1, ch);
    out += "'";
    return out;
}

} // namespace

ShellNodeActuator::ShellNodeActuator(std::string start_template,
                                     std::string stop_template,
                                     std::string reboot_template)
    : start_template_(std::move(start_template)),
      stop_template_(std::move(stop_template)),
      reboot_template_(std::move(reboot_template)) {}

ActuatorResult ShellNodeActuator::StartNode(const std::string& node_id, const std::string& address) {
    return ExecuteTemplate(start_template_, node_id, address, false);
}

ActuatorResult ShellNodeActuator::StopNode(const std::string& node_id, const std::string& address, bool force) {
    return ExecuteTemplate(stop_template_, node_id, address, force);
}

ActuatorResult ShellNodeActuator::RebootNode(const std::string& node_id, const std::string& address) {
    return ExecuteTemplate(reboot_template_, node_id, address, false);
}

ActuatorResult ShellNodeActuator::ExecuteTemplate(const std::string& command_template,
                                                  const std::string& node_id,
                                                  const std::string& address,
                                                  bool force) {
    if (command_template.empty()) {
        return {true, "No command template configured, operation accepted"};
    }
    std::string cmd = command_template;
    cmd = ReplaceAll(cmd, "{node_id}", ShellQuote(node_id));
    cmd = ReplaceAll(cmd, "{address}", ShellQuote(address));
    cmd = ReplaceAll(cmd, "{force}", force ? "true" : "false");

    int ret = std::system(cmd.c_str());
    if (ret != 0) {
        return {false, "Command failed with exit code: " + std::to_string(ret)};
    }
    return {true, "Command executed: " + cmd};
}

} // namespace zb::scheduler
