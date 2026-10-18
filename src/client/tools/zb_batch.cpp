#include "client/sdk/BatchIoClient.h"
#include <fstream>
#include <iostream>
#include <sstream>

int main(int argc, char** argv) {
    if (argc < 2 || argc > 3) {
        std::cerr << "Usage: zb_batch TASKS.tsv [parallel_files]\n"
                     "Rows: id<TAB>read|write<TAB>mount_path<TAB>offset<TAB>size<TAB>local_path<TAB>dependency_ids(comma separated, or -)\n";
        return 2;
    }
    try {
        zb::client::BatchIoOptions options;
        if (argc == 3) options.max_parallel_files = std::stoul(argv[2]);
        zb::client::BatchIoClient client(options);
        std::ifstream in(argv[1]);
        if (!in) throw std::runtime_error("cannot open task manifest");
        std::vector<zb::client::BatchIoTask> tasks;
        std::string line;
        while (std::getline(in, line)) {
            if (line.empty() || line[0] == '#') continue;
            std::istringstream row(line); std::vector<std::string> fields; std::string f;
            while (std::getline(row, f, '\t')) fields.push_back(f);
            if (fields.size() != 7 || (fields[1] != "read" && fields[1] != "write"))
                throw std::runtime_error("invalid TSV row");
            zb::client::BatchIoTask t;
            t.request_id = fields[0]; t.write = fields[1] == "write"; t.path = fields[2];
            auto number = [](const std::string& s) {
                size_t n = 0; auto v = std::stoull(s, &n);
                if (s.empty() || s[0] == '-' || n != s.size()) throw std::runtime_error("invalid number");
                return v;
            };
            t.offset = number(fields[3]); t.size = number(fields[4]); t.local_path = fields[5];
            if (fields[6] != "-") {
                std::istringstream deps(fields[6]);
                while (std::getline(deps, f, ',')) t.after.push_back(f);
            }
            tasks.push_back(std::move(t));
        }
        auto handle = client.submit_batch(std::move(tasks), [](const zb::client::BatchIoResult& r) {
            std::cout << r.request_id << '\t' << (r.error ? (r.skipped ? "SKIPPED" : "FAIL") : "PASS")
                      << '\t' << r.bytes << '\t' << r.elapsed_us << '\t' << r.error
                      << '\t' << r.may_have_written << std::endl;
        });
        for (const auto& result : client.wait_batch(handle)) if (result.error) return 1;
        return 0;
    } catch (const std::exception& e) { std::cerr << e.what() << '\n'; return 2; }
}
