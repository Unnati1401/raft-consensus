#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include <grpcpp/grpcpp.h>

#include "common/common.hpp"
#include "common/config.hpp"
#include "common/logger.hpp"
#include "toolings/msg_queue.hpp"

#include "raft.grpc.pb.h"
#include "rafty/raft_service_impl.hpp"

using namespace toolings;

namespace rafty {
using RaftServiceStub = std::unique_ptr<raftpb::RaftService::Stub>;
using grpc::Server;

class Raft {
public:
  Raft(const Config &config, MessageQueue<ApplyResult> &ready);
  ~Raft();

  // WARN: do not modify the signature
  // TODO: implement `run`, `propose` and `get_state`
  void run(); /* lab 1 */
  ProposalResult propose(const std::string &data); /* lab 1 */
  State get_state() const; /* lab 2 */
  // RPC result helpers and handlers (declared for definition in src/raft.cpp)
  struct RequestVoteResult {
    uint64_t term;
    bool vote_granted;
  };

  struct AppendEntriesResult {
    uint64_t term;
    bool success;
  };

  RequestVoteResult handle_request_vote(uint64_t term,
                                        uint64_t candidate_id,
                                        uint64_t last_log_index,
                                        uint64_t last_log_term);

  AppendEntriesResult handle_append_entries(const raftpb::AppendEntriesRequest &req);
  void send_heartbeats();
  void send_request_votes();
  // lab3: sync propose
  ProposalResult propose_sync(const std::string &data);

  // WARN: do not modify the signature
  void start_server();
  void stop_server();
  void connect_peers();
  bool is_dead() const;
  void kill();

private:
  // WARN: do not modify `create_context` and `apply`.

  // invoke `create_context` when creating context for rpc call.
  // args: the id of which raft instance the RPC will go to.
  std::unique_ptr<grpc::ClientContext> create_context(uint64_t to) const;
  void apply(const ApplyResult &result);

protected:
  // WARN: do not modify `mtx` and `logger`.
  mutable std::mutex mtx;
  std::unique_ptr<rafty::utils::logger> logger;

  // Log storage
  std::vector<raftpb::Entry> log_;
  
  // Volatile state on all servers
  uint64_t commit_index_;
  uint64_t last_applied_;
  
  // Volatile state on leaders
  std::unordered_map<uint64_t, uint64_t> next_index_;
  std::unordered_map<uint64_t, uint64_t> match_index_;
  
  void apply_committed_entries();
  void update_commit_index();
  std::chrono::steady_clock::time_point next_heartbeat_deadline;
  std::chrono::steady_clock::time_point election_deadline;
  void start_election();
  
private:
  // WARN: do not modify the declaration of
  // `id`, `listening_addr`, `peer_addrs`,
  // `dead`, `ready_queue`, `peers_`, and `server_`.
  uint64_t id;
  std::string listening_addr;
  std::map<uint64_t, std::string> peer_addrs;

  std::atomic<bool> dead;
  MessageQueue<ApplyResult> &ready_queue;

  std::unordered_map<uint64_t, RaftServiceStub> peers_;
  std::unique_ptr<Server> server_;
  std::unique_ptr<rafty::RaftServiceImpl> service_impl_;

  // current term known to this server
  uint64_t current_term = 0;
  // candidate ID that received vote in current_term (or -1 for none)
  int64_t voted_for = -1;
  // whether this node currently considers itself leader
  bool is_leader_ = false;
};
} // namespace rafty

#include "rafty/impl/raft.ipp" // IWYU pragma: keep
