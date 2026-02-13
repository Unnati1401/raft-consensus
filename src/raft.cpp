#include "common/utils/rand_gen.hpp"
#include "rafty/raft.hpp"
#ifdef TRACING
#include "common/utils/tracing.hpp"
#endif

namespace rafty {
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::experimental::ClientInterceptorFactoryInterface;
using grpc::experimental::CreateCustomChannelWithInterceptors;

//TODO: finish it
Raft::Raft(const Config &config, MessageQueue<ApplyResult> &ready)
    : logger(utils::logger::get_logger(config.id)),
      id(config.id),
      listening_addr(config.addr),
      peer_addrs(config.peer_addrs),
      dead(false),
      ready_queue(ready) {
}

Raft::~Raft() { this->stop_server(); }

void Raft::run() {
  // TODO: kick off the raft instance
  // Note: this function should be non-blocking

  // lab 1
}

State Raft::get_state() const {
  // TODO: lab 1
  State s;
  s.term = this->current_term;
  s.is_leader = this->is_leader_;
  return s;
}

ProposalResult Raft::propose(const std::string &data) {
  // TODO: lab 2
  return ProposalResult{0, this->current_term, false};
}

ProposalResult Raft::propose_sync(const std::string &data) {
  // TODO: lab 3
  return ProposalResult{0, this->current_term, false};
}

// TODO: add more functions if desired.
  rafty::Raft::RequestVoteResult
  rafty::Raft::handle_request_vote(uint64_t term,
                                  uint64_t candidate_id,
                                  uint64_t,
                                  uint64_t) {
    std::scoped_lock lock(this->mtx);

    RequestVoteResult result;
    result.term = this->current_term;
    result.vote_granted = false;

    if (term < this->current_term) {
      return result;
    }

    if (term > this->current_term) {
      this->current_term = term;
      this->voted_for = -1;
      this->is_leader_ = false;
    }

    if (this->voted_for == -1 ||
        this->voted_for == static_cast<int64_t>(candidate_id)) {
      this->voted_for = static_cast<int64_t>(candidate_id);
      result.vote_granted = true;
    }

    result.term = this->current_term;
    return result;
  }

  rafty::Raft::AppendEntriesResult
  rafty::Raft::handle_append_entries(
      const raftpb::AppendEntriesRequest &req) {

    std::scoped_lock lock(this->mtx);

    AppendEntriesResult result;
    result.term = this->current_term;
    result.success = false;

    if (req.term() < this->current_term) {
      return result;
    }

    if (req.term() > this->current_term) {
      this->current_term = req.term();
      this->voted_for = -1;
      this->is_leader_ = false;
    }

    result.term = this->current_term;
    result.success = true;
    return result;
  }
} // namespace rafty
