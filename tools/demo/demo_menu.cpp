#include "demo_menu.h"

#include <algorithm>
#include <iostream>

namespace zb::demo {

void RenderMenu(const std::string& title, const std::vector<MenuActionSpec>& actions) {
    std::cout << "\n========================================\n";
    std::cout << " " << title << '\n';
    std::cout << "========================================\n";
    for (const auto& action : actions) {
        std::cout << action.id << ") " << action.title;
        if (!action.description.empty()) {
            std::cout << "  - " << action.description;
        }
        std::cout << '\n';
    }
    std::cout << "\n\u8f93\u5165\u683c\u5f0f: <\u5e8f\u53f7> key=value key=value\n";
    std::cout << "\u793a\u4f8b: 6 script=scripts/import_50yi.sh\n";
}

const MenuActionSpec* FindAction(const std::vector<MenuActionSpec>& actions, const std::string& token) {
    auto equals = [&](const MenuActionSpec& action) {
        if (action.id == token) {
            return true;
        }
        return std::find(action.aliases.begin(), action.aliases.end(), token) != action.aliases.end();
    };

    auto it = std::find_if(actions.begin(), actions.end(), equals);
    return it == actions.end() ? nullptr : &(*it);
}

} // namespace zb::demo
