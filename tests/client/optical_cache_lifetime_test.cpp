// Exercise the production node's atomic idle-eviction gate without a two-hour sleep.
#include <optical_node_manager_classes.h>
#include <future>
#include <iostream>
#include <stdexcept>

using optical_node_manager::ReadSessionTable;
using Outcome = ReadSessionTable::AcquireOutcome;
void Check(bool ok) { if (!ok) throw std::runtime_error("cache lifetime assertion failed"); }
int main() {
    try {
        ReadSessionTable table;
        uint64_t first{}, second{};
        Check(table.Acquire(1, "artifact", 10, 4, &first) == Outcome::kAcquired);
        Check(!table.ExpireIfIdle(1));
        Check(table.Acquire(1, "artifact", 10, 4, &second) == Outcome::kAcquired);
        bool cleanable{};
        Check(table.Release(first, &cleanable) == 1 && !cleanable);
        Check(!table.ExpireIfIdle(1));
        Check(table.Release(second, &cleanable) == 1 && !cleanable);
        Check(table.ExpireIfIdle(1));
        Check(!table.ExpireIfIdle(1));
        Check(table.Acquire(1, "artifact", 10, 4, &first) == Outcome::kPrepareReleased);
        // If acquisition wins the race, eviction must defer; if eviction wins, open must fail.
        for (uint64_t task = 2; task < 102; ++task) {
            std::promise<void> go; auto start = go.get_future().share();
            uint64_t handle{};
            auto open = std::async(std::launch::async, [&] {
                start.wait(); return table.Acquire(task, "artifact", 10, 4, &handle);
            });
            auto expire = std::async(std::launch::async, [&] {
                start.wait(); return table.ExpireIfIdle(task);
            });
            go.set_value(); auto outcome = open.get(); bool removed = expire.get();
            if (outcome == Outcome::kAcquired) {
                Check(!removed);
                Check(table.Release(handle, &cleanable) == task && !cleanable);
                Check(table.ExpireIfIdle(task));
            } else {
                Check(outcome == Outcome::kPrepareReleased && removed);
            }
        }
        std::cout << "PASS: node-owned cache protects readers and atomically excludes new opens on eviction\n";
        return 0;
    } catch (const std::exception& e) { std::cerr << e.what() << '\n'; return 1; }
}
