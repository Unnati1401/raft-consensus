#pragma once

#include <cstdint>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <array>
#include <charconv> 
#include <memory>
#include <condition_variable>
#include <chrono>

#include "rafty/raft.hpp"
#include "kv.pb.h"
#include "kv.grpc.pb.h"

namespace kv {

static constexpr const char* OP_PUT    = "P";
static constexpr const char* OP_GET    = "G";
static constexpr const char* OP_APPEND = "A";
static constexpr size_t NUM_SHARDS     = 64; 

struct RiflEntry {
    uint64_t       seq_num = 0;
    kvpb::KvStatus status  = kvpb::KV_SUCCESS;
    std::string    value;
};

struct Shard {
    std::mutex mtx;
    std::unordered_map<std::string, std::string> store;
    std::unordered_map<uint64_t, RiflEntry> rifl_table;
};

struct PendingOp {
    std::mutex m;
    std::condition_variable cv;
    kvpb::KvStatus status = kvpb::KV_NOTLEADER;
    std::string value;
    bool done = false;
};

class KvServer final : public kvpb::KvService::Service {
public:
    explicit KvServer(rafty::Raft &raft) : raft_(raft) {}

    // --- gRPC Service Methods ---

    grpc::Status Put(grpc::ServerContext* context, const kvpb::PutRequest* request, kvpb::KvResponse* response) override {
        return execute_op<kvpb::KvResponse>(OP_PUT, request->key(), request->client_id(), request->seq_num(), request->value(), response);
    }

    grpc::Status Get(grpc::ServerContext* context, const kvpb::GetRequest* request, kvpb::GetResponse* response) override {
        // Optimized Read Path: check leader lease
        uint64_t read_idx = raft_.get_read_index();
        if (read_idx > 0) {
            auto& shard = get_shard(request->key());
            std::scoped_lock lock(shard.mtx);
            response->set_status(kvpb::KV_SUCCESS);
            auto it = shard.store.find(request->key());
            response->set_value(it != shard.store.end() ? it->second : "");
            return grpc::Status::OK;
        }
        return execute_op<kvpb::GetResponse>(OP_GET, request->key(), request->client_id(), request->seq_num(), "", response);
    }

    grpc::Status Append(grpc::ServerContext* context, const kvpb::AppendRequest* request, kvpb::KvResponse* response) override {
        return execute_op<kvpb::KvResponse>(OP_APPEND, request->key(), request->client_id(), request->seq_num(), request->value(), response);
    }

    // --- Internal Logic ---

    template <typename T>
    grpc::Status execute_op(const char* type, const std::string& key, uint64_t cid, uint64_t seq, const std::string& val, T* response) {
        std::string cmd;
        cmd.reserve(key.size() + val.size() + 32);
        cmd += type; cmd += '\0';
        cmd += key;  cmd += '\0';
        cmd += std::to_string(cid); cmd += '\0';
        cmd += std::to_string(seq); cmd += '\0';
        cmd += val;

        auto res = raft_.propose(cmd);
        if (!res.is_leader) {
            response->set_status(kvpb::KV_NOTLEADER);
            return grpc::Status::OK;
        }

        auto pending = std::make_shared<PendingOp>();
        {
            std::scoped_lock lock(pending_mtx_);
            pending_ops_[res.index] = pending;
        }

        std::unique_lock<std::mutex> wait_lock(pending->m);
        bool finished = pending->cv.wait_for(wait_lock, std::chrono::milliseconds(1000), [&] { 
            return pending->done || raft_.is_dead(); 
        });

        if (!finished) {
            response->set_status(kvpb::KV_TIMEOUT);
        } else {
            response->set_status(pending->status);
            // Only GetResponse has a 'value' field in your proto
            if constexpr (std::is_same_v<T, kvpb::GetResponse>) {
                response->set_value(pending->value);
            }
        }

        std::scoped_lock cleanup_lock(pending_mtx_);
        pending_ops_.erase(res.index);
        return grpc::Status::OK;
    }

    void on_apply(const rafty::ApplyResult &result) {
        if (!result.valid) return;

        std::string_view d = result.data;
        size_t pos = 0;
        auto next_tok = [&]() -> std::string_view {
            size_t end = d.find('\0', pos);
            if (end == std::string_view::npos) end = d.size();
            std::string_view tok = d.substr(pos, end - pos);
            pos = (end < d.size()) ? end + 1 : d.size();
            return tok;
        };

        std::string_view op = next_tok();
        std::string_view key = next_tok();
        std::string_view cid_s = next_tok();
        std::string_view seq_s = next_tok();
        std::string_view value = (pos < d.size()) ? d.substr(pos) : "";

        uint64_t client_id = 0, seq_num = 0;
        std::from_chars(cid_s.data(), cid_s.data() + cid_s.size(), client_id);
        std::from_chars(seq_s.data(), seq_s.data() + seq_s.size(), seq_num);

        auto& shard = get_shard(key);
        kvpb::KvStatus op_status = kvpb::KV_SUCCESS;
        std::string op_value;

        {
            std::unique_lock<std::mutex> lock(shard.mtx);
            auto &rifl = shard.rifl_table[client_id];
            
            if (seq_num != 0 && seq_num <= rifl.seq_num) {
                op_status = rifl.status;
                op_value  = rifl.value;
            } else {
                if (op == OP_PUT) shard.store[std::string(key)] = std::string(value);
                else if (op == OP_APPEND) shard.store[std::string(key)] += std::string(value);
                else if (op == OP_GET) {
                    auto it = shard.store.find(std::string(key));
                    op_value = (it != shard.store.end()) ? it->second : "";
                }
                rifl.seq_num = seq_num;
                rifl.status = op_status;
                rifl.value = op_value;
            }
        }

        std::shared_ptr<PendingOp> pending;
        {
            std::scoped_lock lock(pending_mtx_);
            auto it = pending_ops_.find(result.index);
            if (it != pending_ops_.end()) pending = it->second;
        }

        if (pending) {
            std::scoped_lock lock(pending->m);
            pending->status = op_status;
            pending->value = op_value;
            pending->done = true;
            pending->cv.notify_all();
        }
    }

private:
    Shard& get_shard(std::string_view key) {
        return shards_[std::hash<std::string_view>{}(key) % NUM_SHARDS];
    }

    rafty::Raft &raft_;
    std::array<Shard, NUM_SHARDS> shards_;
    std::mutex pending_mtx_;
    std::unordered_map<uint64_t, std::shared_ptr<PendingOp>> pending_ops_;
};

} // namespace kv