#include "client/sdk/PrepareClient.h"
#include <cstdio>
#include <string>

int main(int argc, char** argv) {
    if (argc != 6 || std::string(argv[1]) != "--socket" || std::string(argv[3]) != "stage") {
        std::fprintf(stderr, "Usage: zbctl --socket PATH stage {submit FILE|wait HANDLE|release HANDLE}\n");
        return 2;
    }
    try {
        zb::client::PrepareClient client(argv[2]);
        const std::string command(argv[4]);
        if (command == "submit") std::puts(client.submit(argv[5]).value.c_str());
        else if (command == "wait") std::puts(client.wait({argv[5]}).path.c_str());
        else if (command == "release") client.release_prepared({argv[5]});
        else { std::fprintf(stderr, "Unknown stage command: %s\n", argv[4]); return 2; }
        return 0;
    } catch (const zb::client::ClientError& e) {
        std::fprintf(stderr, "zbctl: %s (errno=%d)\n", e.what(), e.code());
        return 1;
    } catch (const std::exception& e) {
        std::fprintf(stderr, "zbctl: %s\n", e.what());
        return 1;
    }
}
