#include "demo_params.h"

#include <cctype>

namespace zb::demo {

namespace {

std::vector<std::string> Tokenize(const std::string& line, std::string* error) {
    std::vector<std::string> tokens;
    std::string current;
    bool in_quotes = false;
    char quote_char = '\0';

    for (size_t i = 0; i < line.size(); ++i) {
        const char ch = line[i];
        if (in_quotes) {
            if (ch == quote_char) {
                in_quotes = false;
                quote_char = '\0';
            } else if (ch == '\\' && i + 1 < line.size()) {
                current.push_back(line[++i]);
            } else {
                current.push_back(ch);
            }
            continue;
        }
        if (ch == '"' || ch == '\'') {
            in_quotes = true;
            quote_char = ch;
            continue;
        }
        if (std::isspace(static_cast<unsigned char>(ch)) != 0) {
            if (!current.empty()) {
                tokens.push_back(current);
                current.clear();
            }
            continue;
        }
        current.push_back(ch);
    }

    if (in_quotes) {
        if (error) {
            *error = "unterminated quoted argument";
        }
        return {};
    }
    if (!current.empty()) {
        tokens.push_back(current);
    }
    if (error) {
        error->clear();
    }
    return tokens;
}

} // namespace

ParsedCommand ParseCommandLine(const std::string& line) {
    ParsedCommand parsed;
    parsed.raw = line;

    std::string error;
    const std::vector<std::string> tokens = Tokenize(line, &error);
    if (!error.empty()) {
        parsed.error = error;
        return parsed;
    }
    if (tokens.empty()) {
        parsed.error = "empty input";
        return parsed;
    }

    parsed.tokens = tokens;
    parsed.action = tokens.front();
    for (size_t i = 1; i < tokens.size(); ++i) {
        const std::string& token = tokens[i];
        const size_t eq = token.find('=');
        if (eq == std::string::npos) {
            parsed.positionals.push_back(token);
            continue;
        }
        const std::string key = token.substr(0, eq);
        const std::string value = token.substr(eq + 1);
        if (key.empty()) {
            parsed.error = "invalid argument: " + token;
            return parsed;
        }
        parsed.args[key] = value;
    }

    parsed.ok = true;
    return parsed;
}

} // namespace zb::demo
