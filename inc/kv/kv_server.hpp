#pragma once

#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>
#include <memory>
#include <chrono>
#include <condition_variable>
#include <sstream>
#include <grpcpp/grpcpp.h>

#include "common/common.hpp"
#include "kv.grpc.pb.h"
#include "rafty/raft.hpp"

namespace kv {

static constexpr const char* OP_PUT    = "P";
static constexpr const char* OP_GET    = "G";
static constexpr const char* OP_APPEND = "A";

static constexpr int KV_TIMEOUT_MS = 2500;

struct RiflEntry {
    uint64_t       seq_num = 0;
    kvpb::KvStatus status  = kvpb::KV_SUCCESS;
    std::string    value;   
};

struct PendingOp {
    bool           done          = false;
    bool           wrong_leader  = false; 
    kvpb::KvStatus status        = kvpb::KV_TIMEOUT;
    std::string    value;
    std::condition_variable cv;
};

class KvServer : public kvpb::KvService::Service {
public:
  explicit KvServer(rafty::Raft &raft) : raft_(raft) {}
  ~KvServer() = default;

  void on_apply(const rafty::ApplyResult &result) {
    if (!result.valid) return;

    // Parse OUTSIDE the lock — string ops are expensive
    std::string op, key, value;
    uint64_t client_id = 0, seq_num = 0;

    {
        const std::string &d = result.data;
        auto next = [&](size_t &pos) -> std::string {
            size_t end = d.find('\0', pos);
            if (end == std::string::npos) end = d.size();
            std::string tok = d.substr(pos, end - pos);
            pos = (end < d.size()) ? end + 1 : d.size();
            return tok;
        };
        size_t pos = 0;
        op        = next(pos);
        key       = next(pos);
        std::string cid_s = next(pos);
        std::string seq_s = next(pos);
        if (!cid_s.empty()) client_id = std::stoull(cid_s);
        if (!seq_s.empty()) seq_num   = std::stoull(seq_s);
        if (pos < d.size()) value = d.substr(pos);
    }

    kvpb::KvStatus op_status = kvpb::KV_SUCCESS;
    std::string    op_value;
    std::shared_ptr<PendingOp> pending;

    {
        // Lock held for minimal time — only state mutations
        std::unique_lock<std::mutex> lock(mtx_);

        auto &rifl = rifl_table_[client_id];
        bool is_dup = (seq_num != 0 && seq_num <= rifl.seq_num);

        if (is_dup) {
            op_status = rifl.status;
            op_value  = rifl.value;
        } else {
            if (op == OP_PUT) {
                store_[key] = value;
            } else if (op == OP_APPEND) {
                store_[key] += value;
            } else if (op == OP_GET) {
                auto it = store_.find(key);
                op_value = (it != store_.end()) ? it->second : "";
            }
            rifl.seq_num = seq_num;
            rifl.status  = op_status;
            rifl.value   = op_value;
        }

        last_applied_index_ = result.index;

        // Grab the pending op pointer while under lock, notify outside
        auto it = pending_ops_.find(result.index);
        if (it != pending_ops_.end()) {
            pending = it->second;
            pending->done         = true;
            pending->wrong_leader = false;
            pending->status       = op_status;
            pending->value        = op_value;
        }
    } // Lock released here

    // Notify OUTSIDE the lock — avoids waking threads that immediately
    // block again trying to re-acquire mtx_
    if (pending) {
        pending->cv.notify_all();
    }
    apply_cv_.notify_all();
  }

private:
  static std::string serialize(const char *op,
                               const std::string &key,
                               const std::string &value,
                               uint64_t client_id,
                               uint64_t seq_num) {
    std::string out;
    out += op;              out += '\0';
    out += key;             out += '\0';
    out += std::to_string(client_id); out += '\0';
    out += std::to_string(seq_num);
    if (!value.empty() || op[0] == 'P' || op[0] == 'A') {
        out += '\0';
        out += value;
    }
    return out;
  }

  struct OpResult {
      kvpb::KvStatus status;
      std::string    value;
  };

  // Fast local read: wait until last_applied_index_ >= read_index, then
  // serve directly from store_. No Raft log entry needed.
  OpResult execute_get(const std::string &key,
                       uint64_t           client_id,
                       uint64_t           seq_num) {
    // Check RIFL cache first
    {
        std::unique_lock<std::mutex> lock(mtx_);
        auto it = rifl_table_.find(client_id);
        if (it != rifl_table_.end() &&
            seq_num != 0 &&
            seq_num <= it->second.seq_num) {
            return {it->second.status, it->second.value};
        }
    }

    // Get the read index from Raft (current commit index on leader)
    // This confirms we are leader and tells us how far we need to be applied
    uint64_t read_index = raft_.get_read_index();
    if (read_index == 0) {
        return {kvpb::KV_NOTLEADER, ""};
    }

    // Wait until our apply thread has caught up to read_index
    {
        std::unique_lock<std::mutex> lock(mtx_);
        bool ok = apply_cv_.wait_for(
            lock,
            std::chrono::milliseconds(KV_TIMEOUT_MS),
            [&]{ return last_applied_index_ >= read_index; }
        );
        if (!ok) {
            auto state = raft_.get_state();
            return {state.is_leader ? kvpb::KV_TIMEOUT : kvpb::KV_NOTLEADER, ""};
        }

        // Serve locally — no log entry written
        auto it = store_.find(key);
        std::string val = (it != store_.end()) ? it->second : "";

        // Update RIFL cache for this Get
        auto &rifl = rifl_table_[client_id];
        rifl.seq_num = seq_num;
        rifl.status  = kvpb::KV_SUCCESS;
        rifl.value   = val;

        return {kvpb::KV_SUCCESS, val};
    }
  }

  OpResult execute_op(const char       *op_tag,
                      const std::string &key,
                      const std::string &value,
                      uint64_t           client_id,
                      uint64_t           seq_num) {

    {
        std::unique_lock<std::mutex> lock(mtx_);
        auto it = rifl_table_.find(client_id);
        if (it != rifl_table_.end() &&
            seq_num != 0 &&
            seq_num <= it->second.seq_num) {
            return {it->second.status, it->second.value};
        }
    }

    std::string data = serialize(op_tag, key, value, client_id, seq_num);
    rafty::ProposalResult prop = raft_.propose(data);

    if (!prop.is_leader) {
        return {kvpb::KV_NOTLEADER, ""};
    }

    uint64_t idx = prop.index;

    auto pending = std::make_shared<PendingOp>();
    {
        std::unique_lock<std::mutex> lock(mtx_);
        auto old = pending_ops_.find(idx);
        if (old != pending_ops_.end()) {
            old->second->wrong_leader = true;
            old->second->done        = true;
            old->second->cv.notify_all();
        }
        pending_ops_[idx] = pending;
    }

    OpResult out{kvpb::KV_TIMEOUT, ""};
    {
        std::unique_lock<std::mutex> lock(mtx_);
        bool fired = pending->cv.wait_for(
            lock,
            std::chrono::milliseconds(KV_TIMEOUT_MS),
            [&]{ return pending->done; }
        );

        if (fired && !pending->wrong_leader) {
            out = {pending->status, pending->value};
        } else if (fired && pending->wrong_leader) {
            out = {kvpb::KV_NOTLEADER, ""};
        }
        
        pending_ops_.erase(idx);
    }

    if (out.status == kvpb::KV_TIMEOUT) {
        auto state = raft_.get_state();
        if (!state.is_leader) {
            out.status = kvpb::KV_NOTLEADER;
        }
    }

    return out;
  }

public:
  grpc::Status Put(grpc::ServerContext *,
                   const kvpb::PutRequest *request,
                   kvpb::KvResponse *response) override {
    auto res = execute_op(OP_PUT,
                          request->key(),
                          request->value(),
                          request->client_id(),
                          request->seq_num());
    response->set_status(res.status);
    return grpc::Status::OK;
  }

  grpc::Status Get(grpc::ServerContext *,
                   const kvpb::GetRequest *request,
                   kvpb::GetResponse *response) override {
    // Fast path: no Raft log entry for reads
    auto res = execute_get(request->key(),
                           request->client_id(),
                           request->seq_num());
    response->set_status(res.status);
    response->set_value(res.value);
    return grpc::Status::OK;
  }

  grpc::Status Append(grpc::ServerContext *,
                      const kvpb::AppendRequest *request,
                      kvpb::KvResponse *response) override {
    auto res = execute_op(OP_APPEND,
                          request->key(),
                          request->value(),
                          request->client_id(),
                          request->seq_num());
    response->set_status(res.status);
    return grpc::Status::OK;
  }

private:
  rafty::Raft &raft_;

  std::mutex mtx_;  

  std::unordered_map<std::string, std::string> store_;
  std::unordered_map<uint64_t, RiflEntry> rifl_table_;
  std::unordered_map<uint64_t, std::shared_ptr<PendingOp>> pending_ops_;

  // For read-index Gets
  uint64_t last_applied_index_ = 0;
  std::condition_variable apply_cv_;
};

} // namespace kv