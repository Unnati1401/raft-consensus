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
        // Initialize log with a dummy entry at index 0 to make indexing easier
        raftpb::Entry dummy;
        dummy.set_term(0);
        dummy.set_index(0);
        log_.push_back(dummy);
        
        // Initialize next_index and match_index for each peer
        for (const auto& peer : peer_addrs) {
            next_index_[peer.first] = 1;  // Next index starts at 1 (after dummy)
            match_index_[peer.first] = 0;
        }
}

Raft::~Raft() { this->stop_server(); }


void Raft::send_heartbeats() {
    // Note: Caller (run loop) already holds the lock
    
    // For each follower
    for (auto &peer : this->peers_) {
        uint64_t peer_id = peer.first;
        
        // Prepare the AppendEntries request
        raftpb::AppendEntriesRequest req;
        req.set_term(this->current_term);
        req.set_leader_id(this->id);
        req.set_leader_commit(this->commit_index_);
        
        // Set prev_log_index and prev_log_term based on next_index for this peer
        uint64_t next_idx = this->next_index_[peer_id];
        uint64_t prev_log_index = next_idx - 1;
        
        // Safety check: prev_log_index should be within log bounds
        if (prev_log_index < this->log_.size()) {
            req.set_prev_log_index(prev_log_index);
            req.set_prev_log_term(this->log_[prev_log_index].term());
        } else {
            // This shouldn't happen in normal operation
            req.set_prev_log_index(0);
            req.set_prev_log_term(0);
        }
        
        // Add any new entries that need to be sent to this follower
        // For heartbeats, we might send empty entries, but if there are pending entries,
        // we should send them
        for (uint64_t i = next_idx; i < this->log_.size(); i++) {
            raftpb::Entry* entry = req.add_entries();
            entry->set_term(this->log_[i].term());
            entry->set_index(this->log_[i].index());
            entry->set_data(this->log_[i].data());
        }

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
                    return;
                }
                
                // If we're still the leader and this is for the current term
                if (this->is_leader_ && res.term() == this->current_term) {
                  // In the response handling part of send_heartbeats()
                  if (res.success()) {
                      // Update next_index and match_index for this follower
                      if (req.entries_size() > 0) {
                          uint64_t last_sent_index = req.entries(req.entries_size() - 1).index();
                          this->next_index_[peer_id] = last_sent_index + 1;
                          this->match_index_[peer_id] = last_sent_index;
                          
                          this->logger->info("Follower {} successfully replicated entries up to index {}", 
                              peer_id, last_sent_index);
                          
                          // Check if we can commit more entries
                          this->update_commit_index();
                      }
                  } else {
                      // Append failed - log inconsistency
                      this->logger->debug("Follower {} rejected AppendEntries, decrementing next_index from {} to {}", 
                          peer_id, this->next_index_[peer_id], this->next_index_[peer_id] - 1);
                      if (this->next_index_[peer_id] > 1) {
                          this->next_index_[peer_id]--;
                      }
                  }
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
    
    // Get last log info for RequestVote
    uint64_t last_log_index = this->log_.empty() ? 0 : this->log_.back().index();
    uint64_t last_log_term = this->log_.empty() ? 0 : this->log_.back().term();

    for (auto const& [peer_id, stub] : this->peers_) {
        std::thread([this, peer_id, election_term, last_log_index, last_log_term, votes_received, majority]() {
            raftpb::RequestVoteRequest req;
            req.set_term(election_term);
            req.set_candidate_id(this->id);
            req.set_last_log_index(last_log_index);
            req.set_last_log_term(last_log_term);

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
                    
                    // Initialize leader state
                    uint64_t last_log_idx = this->log_.empty() ? 0 : this->log_.back().index();
                    for (auto& [pid, _] : this->peers_) {
                        this->next_index_[pid] = last_log_idx + 1;
                        this->match_index_[pid] = 0;
                    }
                    
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
  // Lock to ensure thread safety
  std::unique_lock<std::mutex> lock(this->mtx);
  
  // Check if this node is the leader
  if (!this->is_leader_) {
    // Not the leader, return with is_leader=false
    ProposalResult result;
    result.index = 0;
    result.term = this->current_term;
    result.is_leader = false;
    return result;
  }
  
  // We are the leader, append to our log
  uint64_t new_index = this->log_.size();  // Current size will be the new index
  
  // Create the new log entry
  raftpb::Entry new_entry;
  new_entry.set_term(this->current_term);
  new_entry.set_index(new_index);
  new_entry.set_data(data);
  
  // Append to log
  this->log_.push_back(new_entry);
  
  this->logger->info("Leader {} appended entry at index {}, term {}, data: {}", 
      this->id, new_index, this->current_term, data);
  
  // Unlock before sending heartbeats (which will send this new entry)
  lock.unlock();
  
  // Immediately trigger a heartbeat to replicate this entry
  // The next heartbeat will send this entry to followers
  this->send_heartbeats();
  
  // Return the proposal result
  ProposalResult result;
  result.index = new_index;
  result.term = this->current_term;
  result.is_leader = true;
  
  return result;
}

ProposalResult Raft::propose_sync(const std::string &data) {
  // TODO: lab 3
  return ProposalResult{0, this->current_term, false};
}

// TODO: add more functions if desired.
  rafty::Raft::RequestVoteResult
  rafty::Raft::handle_request_vote(uint64_t term,
                                  uint64_t candidate_id,
                                  uint64_t last_log_index,
                                  uint64_t last_log_term) {
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

      // If votedFor is null or candidateId, and candidate's log is at least as up-to-date as receiver's log
      if (this->voted_for == -1 || this->voted_for == static_cast<int64_t>(candidate_id)) {
          
          // Check if candidate's log is at least as up-to-date
          uint64_t my_last_log_index = this->log_.empty() ? 0 : this->log_.back().index();
          uint64_t my_last_log_term = this->log_.empty() ? 0 : this->log_.back().term();
          
          bool log_is_uptodate = false;
          
          if (last_log_term > my_last_log_term) {
              log_is_uptodate = true;
          } else if (last_log_term == my_last_log_term) {
              log_is_uptodate = (last_log_index >= my_last_log_index);
          }
          
          if (log_is_uptodate) {
              this->voted_for = static_cast<int64_t>(candidate_id);
              result.vote_granted = true;
              
              // Reset election timer when granting vote
              auto now = std::chrono::steady_clock::now();
              this->election_deadline = now + std::chrono::milliseconds(150 + (std::rand() % 150));
          }
      }

      result.term = this->current_term;
      return result;
  }

  rafty::Raft::AppendEntriesResult
  rafty::Raft::handle_append_entries(const raftpb::AppendEntriesRequest &req) {
      std::scoped_lock lock(this->mtx);

      AppendEntriesResult result;
      result.term = this->current_term;
      result.success = false;

      // 1. Reply false if term < currentTerm (Figure 2)
      if (req.term() < this->current_term) {
          this->logger->info("Node {} rejected AppendEntries: term {} < current term {}", 
              id, req.term(), this->current_term);
          return result;
      }

      // 2. If term > currentTerm, update to new term and become follower
      if (req.term() > this->current_term) {
          this->logger->info("Node {} updating term from {} to {}", 
              id, this->current_term, req.term());
          this->current_term = req.term();
          this->voted_for = -1;
      }

      // 3. We're hearing from a leader, so we're a follower
      this->is_leader_ = false;

      // 4. Reset election timer
      auto now = std::chrono::steady_clock::now();
      this->election_deadline = now + std::chrono::milliseconds(150 + (std::rand() % 150));

      // 5. Reply false if log doesn't contain an entry at prevLogIndex
      //    whose term matches prevLogTerm (Figure 2)
      uint64_t prev_log_index = req.prev_log_index();
      
      if (prev_log_index >= this->log_.size()) {
          this->logger->info("Node {} rejected AppendEntries: prevLogIndex {} beyond log size {}", 
              id, prev_log_index, this->log_.size());
          result.term = this->current_term;
          result.success = false;
          return result;
      }
      
      if (this->log_[prev_log_index].term() != req.prev_log_term()) {
          this->logger->info("Node {} rejected AppendEntries: term mismatch at index {} (expected {}, got {})", 
              id, prev_log_index, req.prev_log_term(), this->log_[prev_log_index].term());
          result.term = this->current_term;
          result.success = false;
          return result;
      }

      // 6. If an existing entry conflicts with a new one (same index but different term),
      //    delete the existing entry and all that follow it (Figure 2)
      uint64_t log_index = prev_log_index + 1;
      for (int i = 0; i < req.entries_size(); i++) {
          const auto& entry = req.entries(i);
          
          if (log_index < this->log_.size()) {
              // Check for conflict
              if (this->log_[log_index].term() != entry.term()) {
                  // Conflict: truncate from this point
                  this->logger->info("Node {} truncating log at index {} (term mismatch: {} vs {})", 
                      id, log_index, this->log_[log_index].term(), entry.term());
                  this->log_.resize(log_index);
                  this->log_.push_back(entry);
              }
              // No conflict, keep existing
          } else {
              // Beyond current log, append
              this->log_.push_back(entry);
          }
          log_index++;
      }

      // 7. Update commit index if leader commit > commitIndex
      if (req.leader_commit() > this->commit_index_) {
          uint64_t last_new_index = this->log_.back().index();
          this->commit_index_ = std::min(req.leader_commit(), last_new_index);
          
          this->logger->info("Node {} updated commit index to {}", id, this->commit_index_);
          
          // 8. Apply any newly committed entries (Part C)
          // We'll implement this fully in Part C
          this->apply_committed_entries();
      }

      result.term = this->current_term;
      result.success = true;
      
      this->logger->info("Node {} successfully processed AppendEntries from leader {}, log size now {}", 
          id, req.leader_id(), this->log_.size());
      
      return result;
  }

  void Raft::apply_committed_entries() {
    // This will be implemented fully in Part C
    // For now, just log that entries are committed
    while (this->last_applied_ < this->commit_index_) {
        this->last_applied_++;
        const auto& entry = this->log_[this->last_applied_];
        this->logger->info("Node {} would apply committed entry at index {}, term {}, data: {}", 
            id, this->last_applied_, entry.term(), entry.data());
        
        // In Part C, we'll actually call apply() here
        // ApplyResult apply_result;
        // apply_result.index = this->last_applied_;
        // apply_result.data = entry.data();
        // this->apply(apply_result);
    }
  }

  void Raft::update_commit_index() {
    // This implements the rule from Figure 2:
    // If there exists an N such that N > commitIndex, a majority
    // of matchIndex[i] >= N, and log[N].term == currentTerm:
    // set commitIndex = N
    
    // Start from the end of the log and work backwards
    for (uint64_t n = this->log_.back().index(); n > this->commit_index_; n--) {
        int replicated_count = 1; // Count self
        
        // Count how many followers have replicated this entry
        for (const auto& [follower_id, match] : this->match_index_) {
            if (match >= n) {
                replicated_count++;
            }
        }
        
        // Check if we have majority and the entry is from current term
        int majority = (this->peers_.size() + 1) / 2 + 1;
        if (replicated_count >= majority && this->log_[n].term() == this->current_term) {
            this->commit_index_ = n;
            this->logger->info("Leader {} advanced commit index to {}", id, n);
            
            // Apply committed entries (will be implemented in Part C)
            this->apply_committed_entries();
            break;
        }
    }
  }
} // namespace rafty
