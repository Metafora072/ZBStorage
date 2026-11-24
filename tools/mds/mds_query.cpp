// Synchronous, single-outstanding-request metadata query client.
#include <brpc/channel.h>
#include <brpc/controller.h>
#include <gflags/gflags.h>
#include <google/protobuf/util/json_util.h>
#include <openssl/sha.h>
#include <chrono>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include "mds.pb.h"

DEFINE_string(server, "127.0.0.1:19000", "MDS endpoint");
DEFINE_string(path, "", "One absolute namespace path");
DEFINE_string(database_id, "", "Database ID for lhm_multi_readonly queries");
DEFINE_bool(list_databases, false, "List multi-database availability and shared index cache counts");
DEFINE_string(paths_file, "", "UTF-8 paths, one per line, queried in order");
DEFINE_bool(interactive, false, "Read paths from stdin");
DEFINE_string(output, "", "JSONL output; default stdout");
DEFINE_int32(timeout_ms, 30000, "Per-request timeout; retries disabled");
DEFINE_string(index_source, "", "Build sparse file-ordinal index for a TSV manifest");
DEFINE_string(index_output, "", "Sparse index output (ordinal TAB byte offset)");
DEFINE_uint64(stride, 4096, "File records per index anchor");

static std::string Quote(const std::string& value) {
    std::ostringstream out; out << '"';
    for (unsigned char c : value) {
        if (c == '"' || c == '\\') out << '\\' << c;
        else if (c < 32) out << "\\u" << std::hex << std::setw(4) << std::setfill('0') << unsigned(c);
        else out << c;
    }
    out << '"'; return out.str();
}

static int Index() {
    if (FLAGS_index_output.empty() || !FLAGS_stride) throw std::runtime_error("index_output and positive stride required");
    std::ifstream src(FLAGS_index_source, std::ios::binary);
    std::ofstream dst(FLAGS_index_output, std::ios::binary | std::ios::trunc);
    if (!src || !dst) throw std::runtime_error("cannot open index input/output");
    SHA256_CTX sha; SHA256_Init(&sha);
    uint64_t offset = 0, count = 0; std::string line;
    while (std::getline(src, line)) {
        if (src.eof()) throw std::runtime_error("manifest must end with newline");
        SHA256_Update(&sha, line.data(), line.size()); SHA256_Update(&sha, "\n", 1);
        if (line.compare(0, 5, "file\t") == 0) {
            if (count % FLAGS_stride == 0) dst << count << '\t' << offset << '\n';
            ++count;
        }
        offset += line.size() + 1;
    }
    if (src.bad()) throw std::runtime_error("manifest read failed");
    dst.close(); if (!dst) throw std::runtime_error("index write failed");
    unsigned char digest[SHA256_DIGEST_LENGTH]; SHA256_Final(digest, &sha);
    std::ostringstream hex;
    for (auto c : digest) hex << std::hex << std::setw(2) << std::setfill('0') << unsigned(c);
    std::cout << "{\"files\":" << count << ",\"bytes\":" << offset << ",\"sha256\":" << Quote(hex.str()) << "}\n";
    return 0;
}

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    try {
        if (!FLAGS_index_source.empty()) return Index();
        if (int(!FLAGS_path.empty()) + int(!FLAGS_paths_file.empty()) + int(FLAGS_interactive) + int(FLAGS_list_databases) != 1)
            throw std::runtime_error("choose exactly one: --path, --paths_file, --interactive, --list_databases");
        if (FLAGS_timeout_ms <= 0) throw std::runtime_error("timeout_ms must be positive");
        brpc::Channel channel; brpc::ChannelOptions options;
        options.protocol = "baidu_std"; options.connection_type = "single";
        options.timeout_ms = FLAGS_timeout_ms; options.max_retry = 0;
        if (channel.Init(FLAGS_server.c_str(), &options)) throw std::runtime_error("channel init failed");
        zb::rpc::MdsService_Stub stub(&channel);
        if(FLAGS_list_databases) {
            zb::rpc::ListDatabasesRequest request;zb::rpc::ListDatabasesReply reply;brpc::Controller ctl;
            stub.ListDatabases(&ctl,&request,&reply,nullptr);
            if(ctl.Failed()) throw std::runtime_error(ctl.ErrorText());
            std::string body;google::protobuf::util::JsonPrintOptions print;
            print.preserve_proto_field_names=true;print.always_print_primitive_fields=true;
            if(!google::protobuf::util::MessageToJsonString(reply,&body,print).ok()) throw std::runtime_error("cannot serialize database list");
            if(FLAGS_output.empty()) std::cout<<body<<'\n';
            else {std::ofstream out(FLAGS_output);out<<body<<'\n';out.close();if(!out) throw std::runtime_error("cannot write database list");}
            return reply.status().code()==zb::rpc::MDS_OK?0:2;
        }
        // Establish transport with root lookup; no sampled file is pre-queried.
        zb::rpc::LookupRequest warm; warm.set_path("/");
        warm.set_database_id(FLAGS_database_id);
        zb::rpc::LookupReply warm_reply; brpc::Controller warm_ctl;
        stub.Lookup(&warm_ctl, &warm, &warm_reply, nullptr);
        if (warm_ctl.Failed() || warm_reply.status().code() != zb::rpc::MDS_OK || warm_reply.database_id()!=FLAGS_database_id)
            throw std::runtime_error("root readiness/connection warmup failed: " + warm_ctl.ErrorText()+" "+warm_reply.status().message());
        std::ofstream output; std::ostream* out = &std::cout;
        if (!FLAGS_output.empty()) {
            output.open(FLAGS_output); if (!output) throw std::runtime_error("cannot open output"); out = &output;
        }
        size_t seq = 0, failures = 0;
        auto query = [&](const std::string& path) {
            if (path.empty() || path.front() != '/' || path.find('\0') != std::string::npos)
                throw std::runtime_error("invalid absolute path");
            zb::rpc::LookupRequest req; req.set_path(path);
            req.set_database_id(FLAGS_database_id);
            zb::rpc::LookupReply reply; brpc::Controller ctl;
            const auto start = std::chrono::steady_clock::now();
            stub.Lookup(&ctl, &req, &reply, nullptr);
            const auto end = std::chrono::steady_clock::now();
            const auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
            const bool ok = !ctl.Failed() && reply.status().code() == zb::rpc::MDS_OK && reply.database_id()==FLAGS_database_id;
            if (!ok) ++failures;
            std::string body; google::protobuf::util::JsonPrintOptions print;
            print.preserve_proto_field_names = true; print.always_print_primitive_fields = true;
            if (!google::protobuf::util::MessageToJsonString(reply, &body, print).ok())
                throw std::runtime_error("response JSON serialization failed");
            *out << "{\"sequence\":" << ++seq << ",\"path\":" << Quote(path)
                 << ",\"elapsed_ns\":" << ns << ",\"ok\":" << (ok ? "true" : "false")
                 << ",\"rpc_error\":" << Quote(ctl.ErrorText()) << ",\"response\":" << body << "}\n";
            if (!*out) throw std::runtime_error("result write failed");
            if (FLAGS_interactive) out->flush();
        };
        if (!FLAGS_path.empty()) query(FLAGS_path);
        else {
            std::ifstream file;
            if (!FLAGS_paths_file.empty()) { file.open(FLAGS_paths_file); if (!file) throw std::runtime_error("cannot open paths file"); }
            std::istream& in = FLAGS_interactive ? std::cin : file;
            std::string path; while (std::getline(in, path)) { if (!path.empty()) query(path); }
            if (in.bad()) throw std::runtime_error("paths input read failed");
        }
        out->flush(); if (!*out) throw std::runtime_error("result flush failed");
        return failures ? 2 : 0;
    } catch (const std::exception& e) { std::cerr << e.what() << '\n'; return 1; }
}
