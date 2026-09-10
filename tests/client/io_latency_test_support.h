#pragma once
#include <chrono>
#include <filesystem>
#include <fstream>
#include <functional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>
#include <cstdlib>

#ifndef ZBSTORAGE_TEST_RESULTS
#define ZBSTORAGE_TEST_RESULTS "tests/client/results"
#endif
namespace latency_test {
namespace fs = std::filesystem;
inline void Check(bool ok, const std::string& message) {
    if (!ok) throw std::runtime_error(message);
}
struct TempDir {
    fs::path path;
    TempDir() {
        fs::create_directories(ZBSTORAGE_TEST_RESULTS);
        auto pattern = (fs::absolute(ZBSTORAGE_TEST_RESULTS) / "latency-XXXXXX").string();
        std::vector<char> name(pattern.begin(), pattern.end());
        name.push_back('\0');
        Check(::mkdtemp(name.data()) != nullptr, "mkdtemp failed");
        path = name.data();
    }
    ~TempDir() { std::error_code ec; fs::remove_all(path, ec); }
};
inline void Write(const fs::path& path, const std::string& text) {
    std::ofstream out(path);
    out << text;
    Check(bool(out), "cannot write test fixture");
}
inline std::vector<std::vector<std::string>> ReadCsv(const std::string& path) {
    std::ifstream in(path);
    Check(bool(in), "CSV missing: " + path);
    std::vector<std::vector<std::string>> rows;
    std::string line;
    while (std::getline(in, line)) {
        std::stringstream stream(line);
        std::vector<std::string> row;
        std::string value;
        while (std::getline(stream, value, ',')) row.push_back(value);
        Check(row.size() == 14, "CSV should have exactly 14 columns: " + line);
        rows.push_back(row);
    }
    Check(!rows.empty(), "CSV header missing");
    Check(rows[0][11] == "mds_us" && rows[0][12] == "data_node_us" && rows[0][13] == "total_us", "unexpected latency columns");
    return rows;
}
inline bool WaitUntil(const std::function<bool()>& predicate) {
    const auto end = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    do {
        if (predicate()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    } while (std::chrono::steady_clock::now() < end);
    return false;
}
} // namespace latency_test
