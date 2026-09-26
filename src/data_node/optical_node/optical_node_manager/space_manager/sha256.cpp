#include "sha256.h"

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <unistd.h>
#include <vector>

namespace space_manager {

namespace {

// SHA-256 轮常量（FIPS 180-4 §4.2.2）。
constexpr uint32_t kRoundConstants[64] = {
    0x428a2f98u, 0x71374491u, 0xb5c0fbcfu, 0xe9b5dba5u, 0x3956c25bu, 0x59f111f1u, 0x923f82a4u, 0xab1c5ed5u,
    0xd807aa98u, 0x12835b01u, 0x243185beu, 0x550c7dc3u, 0x72be5d74u, 0x80deb1feu, 0x9bdc06a7u, 0xc19bf174u,
    0xe49b69c1u, 0xefbe4786u, 0x0fc19dc6u, 0x240ca1ccu, 0x2de92c6fu, 0x4a7484aau, 0x5cb0a9dcu, 0x76f988dau,
    0x983e5152u, 0xa831c66du, 0xb00327c8u, 0xbf597fc7u, 0xc6e00bf3u, 0xd5a79147u, 0x06ca6351u, 0x14292967u,
    0x27b70a85u, 0x2e1b2138u, 0x4d2c6dfcu, 0x53380d13u, 0x650a7354u, 0x766a0abbu, 0x81c2c92eu, 0x92722c85u,
    0xa2bfe8a1u, 0xa81a664bu, 0xc24b8b70u, 0xc76c51a3u, 0xd192e819u, 0xd6990624u, 0xf40e3585u, 0x106aa070u,
    0x19a4c116u, 0x1e376c08u, 0x2748774cu, 0x34b0bcb5u, 0x391c0cb3u, 0x4ed8aa4au, 0x5b9cca4fu, 0x682e6ff3u,
    0x748f82eeu, 0x78a5636fu, 0x84c87814u, 0x8cc70208u, 0x90befffau, 0xa4506cebu, 0xbef9a3f7u, 0xc67178f2u};

// SHA-256 初始哈希值（FIPS 180-4 §5.3.3）。
constexpr uint32_t kInitialState[8] = {
    0x6a09e667u, 0xbb67ae85u, 0x3c6ef372u, 0xa54ff53au,
    0x510e527fu, 0x9b05688cu, 0x1f83d9abu, 0x5be0cd19u};

// 32 位循环右移。
inline uint32_t Rotr(uint32_t value, uint32_t bits) {
    return (value >> bits) | (value << (32 - bits));
}

// 流式上下文：state 为 8 个 32 位寄存器；buffer 缓存不足一块的尾部数据。
struct Sha256Context {
    uint32_t state[8];
    uint64_t total_bytes;
    uint8_t buffer[64];
    size_t buffer_len;
};

void Sha256Init(Sha256Context* ctx) {
    std::memcpy(ctx->state, kInitialState, sizeof(kInitialState));
    ctx->total_bytes = 0;
    ctx->buffer_len = 0;
}

// 压缩函数：处理一个 64 字节数据块并更新 8 个寄存器。
void Sha256Transform(Sha256Context* ctx, const uint8_t* block) {
    uint32_t w[64];
    for (int i = 0; i < 16; ++i) {
        w[i] = (static_cast<uint32_t>(block[i * 4]) << 24) |
               (static_cast<uint32_t>(block[i * 4 + 1]) << 16) |
               (static_cast<uint32_t>(block[i * 4 + 2]) << 8) |
               static_cast<uint32_t>(block[i * 4 + 3]);
    }
    for (int i = 16; i < 64; ++i) {
        const uint32_t s0 = Rotr(w[i - 15], 7) ^ Rotr(w[i - 15], 18) ^ (w[i - 15] >> 3);
        const uint32_t s1 = Rotr(w[i - 2], 17) ^ Rotr(w[i - 2], 19) ^ (w[i - 2] >> 10);
        w[i] = w[i - 16] + s0 + w[i - 7] + s1;
    }

    uint32_t a = ctx->state[0];
    uint32_t b = ctx->state[1];
    uint32_t c = ctx->state[2];
    uint32_t d = ctx->state[3];
    uint32_t e = ctx->state[4];
    uint32_t f = ctx->state[5];
    uint32_t g = ctx->state[6];
    uint32_t h = ctx->state[7];

    for (int i = 0; i < 64; ++i) {
        const uint32_t big_s1 = Rotr(e, 6) ^ Rotr(e, 11) ^ Rotr(e, 25);
        const uint32_t choose = (e & f) ^ (~e & g);
        const uint32_t temp1 = h + big_s1 + choose + kRoundConstants[i] + w[i];
        const uint32_t big_s0 = Rotr(a, 2) ^ Rotr(a, 13) ^ Rotr(a, 22);
        const uint32_t majority = (a & b) ^ (a & c) ^ (b & c);
        const uint32_t temp2 = big_s0 + majority;

        h = g;
        g = f;
        f = e;
        e = d + temp1;
        d = c;
        c = b;
        b = a;
        a = temp1 + temp2;
    }

    ctx->state[0] += a;
    ctx->state[1] += b;
    ctx->state[2] += c;
    ctx->state[3] += d;
    ctx->state[4] += e;
    ctx->state[5] += f;
    ctx->state[6] += g;
    ctx->state[7] += h;
}

// 增量喂入数据；可任意次调用，长度不限。
void Sha256Update(Sha256Context* ctx, const uint8_t* data, size_t len) {
    ctx->total_bytes += len;

    // 先把上次残留的半个块补齐。
    if (ctx->buffer_len > 0) {
        const size_t need = 64 - ctx->buffer_len;
        const size_t take = (len < need) ? len : need;
        std::memcpy(ctx->buffer + ctx->buffer_len, data, take);
        ctx->buffer_len += take;
        data += take;
        len -= take;
        if (ctx->buffer_len == 64) {
            Sha256Transform(ctx, ctx->buffer);
            ctx->buffer_len = 0;
        }
    }

    while (len >= 64) {
        Sha256Transform(ctx, data);
        data += 64;
        len -= 64;
    }

    if (len > 0) {
        std::memcpy(ctx->buffer, data, len);
        ctx->buffer_len = len;
    }
}

// 收尾：追加 0x80 / 0 填充与大端 64 位比特长度，输出 32 字节摘要。
void Sha256Final(Sha256Context* ctx, uint8_t out_digest[32]) {
    const uint64_t bit_len = ctx->total_bytes * 8;

    // 0x80 与 0 填充的总字节数：使 (buffer_len + tail_len) % 64 == 56。
    const size_t zero_count =
        (ctx->buffer_len < 56) ? (55 - ctx->buffer_len) : (119 - ctx->buffer_len);
    const size_t tail_len = 1 + zero_count;
    uint8_t padding[72];
    std::memset(padding, 0, sizeof(padding));
    padding[0] = 0x80;

    uint8_t len_be[8];
    for (int i = 0; i < 8; ++i) {
        len_be[i] = static_cast<uint8_t>((bit_len >> (56 - i * 8)) & 0xFF);
    }

    // 复用 Update：它会自增 total_bytes，但此后不再读取该字段，无副作用。
    Sha256Update(ctx, padding, tail_len);
    Sha256Update(ctx, len_be, sizeof(len_be));

    for (int i = 0; i < 8; ++i) {
        out_digest[i * 4] = static_cast<uint8_t>(ctx->state[i] >> 24);
        out_digest[i * 4 + 1] = static_cast<uint8_t>(ctx->state[i] >> 16);
        out_digest[i * 4 + 2] = static_cast<uint8_t>(ctx->state[i] >> 8);
        out_digest[i * 4 + 3] = static_cast<uint8_t>(ctx->state[i]);
    }
}

}  // namespace

bool Sha256FileHex(const std::string& path, std::string* out_hex) {
    if (out_hex == nullptr) {
        return false;
    }
    out_hex->clear();

    const int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        return false;
    }

    Sha256Context ctx;
    Sha256Init(&ctx);

    // 1 MiB 缓冲：兼顾系统调用次数与内存占用，不整文件读入。
    constexpr size_t kBufferSize = 1u << 20;
    std::vector<uint8_t> buffer(kBufferSize);

    bool read_ok = true;
    for (;;) {
        const ssize_t n = ::read(fd, buffer.data(), buffer.size());
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            read_ok = false;
            break;
        }
        if (n == 0) {
            break;
        }
        Sha256Update(&ctx, buffer.data(), static_cast<size_t>(n));
    }
    ::close(fd);

    if (!read_ok) {
        return false;
    }

    uint8_t digest[32];
    Sha256Final(&ctx, digest);

    static const char kHexDigits[] = "0123456789abcdef";
    out_hex->reserve(64);
    for (size_t i = 0; i < sizeof(digest); ++i) {
        out_hex->push_back(kHexDigits[digest[i] >> 4]);
        out_hex->push_back(kHexDigits[digest[i] & 0x0F]);
    }
    return true;
}

}  // namespace space_manager