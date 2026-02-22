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
      ready_queue(ready),
      current_term(0),    // move these up
      voted_for(-1),
      is_leader_(false) {
}

Raft::~Raft() { this->stop_server(); }


void Raft::send_heartbeats() {
    // Note: Caller (run loop) already holds the lock, but since we 
    // are spawning threads, we capture the data we need.
    for (auto &peer : this->peers_) {
        uint64_t peer_id = peer.first;
        
        raftpb::AppendEntriesRequest req;
        req.set_term(this->current_term);
        req.set_leader_id(this->id);

        // Spawn a thread for each peer so one slow connection doesn't block the leader
        std::thread([this, peer_id, req]() {
            auto context = this->create_context(peer_id);
            raftpb::AppendEntriesResponse res;
            
            auto status = this->peers_[peer_id]->AppendEntries(context.get(), req, &res);

            if (status.ok()) {
                std::unique_lock<std::mutex> lock(this->mtx);
                // Rule: If response contains term T > currentTerm, step down
                if (res.term() > this->current_term) {
                  this->current_term = res.term();
                  this->is_leader_ = false;
                  this->voted_for = -1;
                  this->election_deadline =
                      std::chrono::steady_clock::now() +
                      std::chrono::milliseconds(150 + (std::rand() % 150));
              }
            }
        }).detach();
    }
}

void Raft::send_request_votes() {
    // Shared state for the threads to update
    auto votes_received = std::make_shared<std::atomic<int>>(1); // 1 for self
    int total_nodes = this->peers_.size() + 1;
    int majority = total_nodes / 2 + 1;
    uint64_t election_term = this->current_term;

    for (auto const& [peer_id, stub] : this->peers_) {
        std::thread([this, peer_id, election_term, votes_received, majority]() {
            raftpb::RequestVoteRequest req;
            req.set_term(election_term);
            req.set_candidate_id(this->id);
            // Lab 1 placeholders
            req.set_last_log_index(0); 
            req.set_last_log_term(0);

            auto context = this->create_context(peer_id);
            raftpb::RequestVoteResponse res;
            auto status = this->peers_[peer_id]->RequestVote(context.get(), req, &res);
            if (!status.ok()) {
                return;
            }

            std::unique_lock<std::mutex> lock(this->mtx);

            // If we see a higher term → step down immediately
            if (res.term() > this->current_term) {
                this->current_term = res.term();
                this->is_leader_ = false;
                this->voted_for = -1;
                this->election_deadline =
                    std::chrono::steady_clock::now() +
                    std::chrono::milliseconds(150 + (std::rand() % 150));
                return;
            }

            // Ignore stale replies
            if (res.term() != election_term) {
                return;
            }

            // Ignore if we are no longer candidate
            if (this->is_leader_) {
                return;
            }

            if (this->current_term != election_term) {
                return;
            }

            // Count vote
            if (res.vote_granted()) {
                if (++(*votes_received) >= majority) {
                    this->is_leader_ = true;
                    this->logger->info("Node {} became leader for term {}", id, current_term);
                    this->next_heartbeat_deadline = std::chrono::steady_clock::now();
                    lock.unlock();
                    this->send_heartbeats();  // send immediately
                }
            }
          }).detach();
      }
  }

void Raft::run() {
  // TODO: kick off the raft instance
  // Note: this function should be non-blocking

  // lab 1

  // 1. Initialize random seed based on node ID
  std::srand(static_cast<unsigned int>(this->id) + std::time(nullptr));

  // 2. Spawn a background thread so run() is non-blocking
  std::thread([this]() {
    this->logger->info("Raft node {} background loop started", id);

    auto now = std::chrono::steady_clock::now();
    
    // Initial deadlines
    {
        std::scoped_lock lock(this->mtx);
        this->next_heartbeat_deadline = now;
        // Random timeout between 150ms and 300ms
        this->election_deadline = now + std::chrono::milliseconds(150 + (std::rand() % 150));
    }

    while (!this->is_dead()) {
      now = std::chrono::steady_clock::now();
      std::unique_lock<std::mutex> lock(this->mtx);

      if (this->is_leader_) {
        // LEADER: Send heartbeats every 100ms
        if (now >= this->next_heartbeat_deadline) {
          this->send_heartbeats(); 
          this->next_heartbeat_deadline = now + std::chrono::milliseconds(100);
        }
      } else {
        // FOLLOWER/CANDIDATE: Check if leader has been silent too long
        if (now >= this->election_deadline) {
          this->logger->info("Node {} timeout! Starting election", id);
          
          this->is_leader_ = false;
          this->current_term++;
          this->voted_for = this->id;

          // Reset election timer for the campaign period
          this->election_deadline = now + std::chrono::milliseconds(150 + (std::rand() % 150));
          
          // Start the voting process (Part C)
          this->send_request_votes(); 
        }
      }
      lock.unlock();

      // Sleep a bit to prevent 100% CPU usage
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    
    this->logger->info("Raft node {} background loop exiting", id);
  }).detach(); // Detach allows the thread to run independently
}

State Raft::get_state() const {
  // TODO: lab 1
  std::scoped_lock lock(this->mtx);
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
      auto now = std::chrono::steady_clock::now();
      this->election_deadline = now + std::chrono::milliseconds(150 + (std::rand() % 150));
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
    }

    this->is_leader_ = false;

    auto now = std::chrono::steady_clock::now();
    this->election_deadline = now + std::chrono::milliseconds(150 + (std::rand() % 150));

    result.term = this->current_term;
    result.success = true;
    return result;
  }
} // namespace rafty
