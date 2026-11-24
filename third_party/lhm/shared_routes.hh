#pragma once
// Immutable, instance-independent directory/bucket routing for read-only imports.
#include "compiled_template.hh"
#include "incremental_checkpoint.hh"
#include <numeric>
#include <sstream>
namespace MasstreeLHM {
constexpr uint64_t kSharedRouteMagic=0x315254534d484cULL;
struct SharedRoutes {
    std::vector<compiled::Route> routes;
    // One compact permutation, ordered by (parent ordinal, full component name).
    // Full names disambiguate directory hashes, including negative lookups.
    std::vector<uint64_t> children;
    void validate(const SharedTemplateInfo& info) {
        if(routes.size()!=info.directories) throw std::runtime_error("shared directory count mismatch");
        uint64_t files=0; std::vector<uint16_t> depths(routes.size()); std::vector<bool> seen(info.bucket_pages,false);
        for(size_t i=0;i<routes.size();++i) {
            const auto& r=routes[i];
            if(r.ordinal!=i+1 || r.parent_ordinal>=r.ordinal ||
               (i==0 ? r.parent_ordinal || r.name!="/" : !r.parent_ordinal))
                throw std::runtime_error("shared route order/parent mismatch");
            depths[i]=i?depths[r.parent_ordinal-1]+1:1;
            if(depths[i]>4096) throw std::runtime_error("shared path exceeds maximum depth");
            if(r.plan.directory.entry_count>info.files-files) throw std::runtime_error("shared file count overflow");
            files+=r.plan.directory.entry_count;
            for(const auto& b:r.plan.directory.buckets) {
                if(!b.block_id || b.block_id>info.bucket_pages || seen[b.block_id-1])
                    throw std::runtime_error("shared bucket range/uniqueness mismatch");
                seen[b.block_id-1]=true;
            }
        }
        if(files!=info.files || std::find(seen.begin(),seen.end(),false)!=seen.end())
            throw std::runtime_error("shared template totals mismatch");
        children.resize(routes.size()-1); std::iota(children.begin(),children.end(),uint64_t{2});
        auto less=[&](uint64_t a,uint64_t b) { const auto& x=routes[a-1]; const auto& y=routes[b-1];
            return x.parent_ordinal!=y.parent_ordinal?x.parent_ordinal<y.parent_ordinal:x.name<y.name; };
        std::sort(children.begin(),children.end(),less);
        for(size_t i=1;i<children.size();++i) if(!less(children[i-1],children[i]))
            throw std::runtime_error("duplicate shared directory name");
    }
    uint64_t child(uint64_t parent,const std::string& name) const {
        auto i=std::lower_bound(children.begin(),children.end(),std::make_pair(parent,name),
            [&](uint64_t n,const auto& key) { const auto& r=routes[n-1];
                return r.parent_ordinal!=key.first?r.parent_ordinal<key.first:r.name<key.second; });
        return i!=children.end() && routes[*i-1].parent_ordinal==parent && routes[*i-1].name==name?*i:0;
    }
    static uint64_t save(const std::string& file,const std::vector<compiled::Route>& routes) {
        SnapshotWriter out(file); out.u64(kSharedRouteMagic); out.u64(1); out.u64(routes.size());
        for(const auto& r:routes) {
            std::ostringstream bytes(std::ios::binary); compiled::WriteRoute(bytes,r);
            const auto s=bytes.str(); out.u64(s.size());
            for(size_t i=0;i<s.size();i+=8) { uint64_t word=0;
                for(size_t j=0;j<8 && i+j<s.size();++j) word|=uint64_t(uint8_t(s[i+j]))<<(8*j);
                out.u64(word); }
        }
        const auto checksum=out.checksum(); out.finish(); return checksum;
    }
    void load(const std::string& file,const IndexFile& identity,const SharedTemplateInfo& info) {
        SnapshotReader in(file);
        if(in.u64()!=kSharedRouteMagic || in.u64()!=1 || in.u64()!=info.directories ||
           info.directories>identity.bytes/24 || info.bucket_pages>identity.bytes/20) throw std::runtime_error("invalid shared route header");
        routes.reserve(info.directories);
        for(uint64_t i=0;i<info.directories;++i) {
            const auto n=in.u64();
            if(!n || n>(32ULL<<20)+256 || n>identity.bytes) throw std::runtime_error("shared route record too large");
            std::string data; data.reserve(n);
            for(uint64_t p=0;p<n;p+=8) { auto word=in.u64();
                for(uint64_t j=0;j<8 && p+j<n;++j) {data+=char(word&255);word>>=8;}
                if(word) throw std::runtime_error("noncanonical shared route padding"); }
            std::istringstream bytes(data,std::ios::binary); compiled::Route r;
            if(!compiled::ReadRoute(bytes,r) || bytes.peek()!=std::char_traits<char>::eof())
                throw std::runtime_error("invalid shared route record");
            routes.push_back(std::move(r));
        }
        if(in.checksum()!=identity.checksum) throw std::runtime_error("shared route checksum differs from checkpoint");
        in.finish(); validate(info);
    }
};
}
