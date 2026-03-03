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

// Constructor
Raft::Raft(const Config &config, MessageQueue<ApplyResult> &ready)
    : logger(utils::logger::get_logger(config.id)),
      id(config.id),
      listening_addr(config.addr),
      peer_addrs(config.peer_addrs),
      dead(false),
      ready_queue(ready),
      current_term(0),
      voted_for(-1),
      commit_index_(0),
      last_applied_(0),
      is_leader_(false) {

    raftpb::Entry dummy;
    dummy.set_term(0);
    dummy.set_index(0);
    dummy.set_data("");
    log_.push_back(dummy);

    this->logger->info("Raft node {} initialized", id);
}

Raft::~Raft() { this->stop_server(); }

void Raft::run() {
    std::srand(static_cast<unsigned int>(this->id) + std::time(nullptr));

    std::thread([this]() {
        this->logger->info("Raft node {} background loop started", id);

        {
            std::scoped_lock lock(this->mtx);
            auto now = std::chrono::steady_clock::now();
            this->next_heartbeat_deadline = now;
            this->election_deadline = now +
                std::chrono::milliseconds(150 + (std::rand() % 150));
        }

        while (!this->is_dead()) {
            auto now = std::chrono::steady_clock::now();
            bool should_heartbeat = false;
            bool should_elect     = false;

            {
                std::unique_lock<std::mutex> lock(this->mtx);
                if (this->is_leader_) {
                    if (now >= this->next_heartbeat_deadline) {
                        this->next_heartbeat_deadline =
                            now + std::chrono::milliseconds(25);
                        should_heartbeat = true;
                    }
                } else {
                    if (now >= this->election_deadline) {
                        should_elect = true;
                    }
                }
            } // lock released HERE — before any RPC

            if (should_heartbeat) this->send_heartbeats();
            if (should_elect)     this->start_election();
            this->apply_committed_entries(); 
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }

        this->logger->info("Raft node {} background loop exiting", id);
    }).detach();
}

void Raft::start_election() {
    uint64_t term_at_election;
    uint64_t last_log_index;
    uint64_t last_log_term;

    {
        std::scoped_lock lock(this->mtx);
        this->current_term++;
        this->voted_for = this->id;
        this->is_leader_ = false;
        
        // Reset election deadline
        this->election_deadline = std::chrono::steady_clock::now() +
            std::chrono::milliseconds(150 + (std::rand() % 150));

        term_at_election = this->current_term;
        last_log_index   = this->log_.back().index();
        last_log_term    = this->log_.back().term();
    }

    this->logger->info("Node {} starting election for term {}",
                       id, term_at_election);

    // Use atomic for thread-safe counting, though we increment under mutex anyway
    auto votes_received = std::make_shared<std::atomic<int>>(1); 
    int  majority       = (int)(this->peer_addrs.size() + 1) / 2 + 1;

    for (const auto& [peer_id, stub] : this->peers_) {
        std::thread([this, peer_id, term_at_election,
                     last_log_index, last_log_term,
                     votes_received, majority]() {

            raftpb::RequestVoteRequest req;
            req.set_term(term_at_election);
            req.set_candidate_id(this->id);
            req.set_last_log_index(last_log_index);
            req.set_last_log_term(last_log_term);

            auto context = this->create_context(peer_id);
            raftpb::RequestVoteResponse res;
            
            // RPC Call
            auto status = this->peers_[peer_id]->RequestVote(
                context.get(), req, &res);

            if (!status.ok()) return;

            std::scoped_lock lock(this->mtx);

            // 1. Step down if we see a higher term (Discovery of newer leader/candidate)
            if (res.term() > this->current_term) {
                this->current_term = res.term();
                this->voted_for    = -1;
                this->is_leader_   = false;
                this->election_deadline = std::chrono::steady_clock::now() +
                    std::chrono::milliseconds(150 + (std::rand() % 150));
                return;
            }

            // 2. Stale reply guards: 
            // If the term changed or we are already leader, ignore this vote.
            if (res.term() != term_at_election || this->current_term != term_at_election) return;
            if (this->is_leader_) return; 

            // 3. Process granted vote
            if (res.vote_granted()) {
                // CHANGE: Use == so initialization happens exactly once
                if (++(*votes_received) == majority) {
                    this->is_leader_ = true;
                    this->logger->info("Node {} became leader for term {}",
                                       id, current_term);

                    // Initialise leader volatile state (Figure 2)
                    uint64_t last_idx = this->log_.back().index();
                    for (auto const& [pid, _] : this->peers_) {
                        this->next_index_[pid]  = last_idx + 1;
                        this->match_index_[pid] = 0;
                    }
                    
                    // Trigger immediate heartbeat to assert dominance
                    this->next_heartbeat_deadline = std::chrono::steady_clock::now();
                }
            }
        }).detach();
    }
}

void Raft::send_heartbeats() {
    // -------- Phase 1: Build RPC requests under lock --------
    // We snapshot the state needed so we don't hold the lock during network I/O.
    std::vector<std::pair<uint64_t, raftpb::AppendEntriesRequest>> requests;

    {
        std::scoped_lock lock(this->mtx);

        if (!this->is_leader_) return;

        uint64_t current_term_snapshot = this->current_term;
        uint64_t last_index = this->log_.size() - 1;

        for (const auto& [peer_id, stub] : this->peers_) {
            raftpb::AppendEntriesRequest req;
            req.set_term(current_term_snapshot);
            req.set_leader_id(this->id);
            req.set_leader_commit(this->commit_index_);

            // 1. Determine where to start sending logs
            uint64_t next_idx = this->next_index_.count(peer_id) ? this->next_index_[peer_id] : 1;

            // Invariant: nextIndex must be within [1, last_index + 1]
            if (next_idx < 1) next_idx = 1;
            if (next_idx > last_index + 1) next_idx = last_index + 1;

            uint64_t prev_idx = next_idx - 1;
            req.set_prev_log_index(prev_idx);
            req.set_prev_log_term(this->log_[prev_idx].term());

            // 2. Only add entries that the follower doesn't have yet (Efficient Packing)
            for (uint64_t i = next_idx; i <= last_index; ++i) {
                *req.add_entries() = this->log_[i];
            }

            requests.emplace_back(peer_id, std::move(req));
        }
    }

    // -------- Phase 2: Dispatch RPCs without holding lock --------
    for (auto& [peer_id, req] : requests) {
        std::thread([this, peer_id, req]() {
            auto context = this->create_context(peer_id);
            raftpb::AppendEntriesResponse res;

            auto status = this->peers_[peer_id]->AppendEntries(context.get(), req, &res);
            if (!status.ok()) return;

            std::scoped_lock lock(this->mtx);

            // 1. Higher term discovered -> immediately step down
            if (res.term() > this->current_term) {
                this->current_term = res.term();
                this->voted_for = -1;
                this->is_leader_ = false;
                this->election_deadline = std::chrono::steady_clock::now() +
                    std::chrono::milliseconds(150 + (std::rand() % 150));
                return;
            }

            // 2. Ignore stale replies (we are no longer leader or the term has moved on)
            if (!this->is_leader_ || req.term() != this->current_term) return;

            if (res.success()) {
                // SUCCESS: Update indices only if this reply is more recent than what we know
                uint64_t match = req.prev_log_index() + req.entries_size();
                if (match > this->match_index_[peer_id]) {
                    this->match_index_[peer_id] = match;
                    this->next_index_[peer_id] = match + 1;
                    
                    // Check if we can commit new entries now
                    this->update_commit_index();
                }
            } else {
                // FAILURE: Log mismatch. Decrement and retry.
                // Only decrement if this reply matches our current expectation for this peer.
                if (this->next_index_[peer_id] == req.prev_log_index() + 1) {
                    if (this->next_index_[peer_id] > 1) {
                        this->next_index_[peer_id]--;
                        
                        // Wake up background loop for fast catch-up (Fixes RejoinB)
                        this->next_heartbeat_deadline = std::chrono::steady_clock::now();
                    }
                }
            }
        }).detach();
    }
}

rafty::Raft::AppendEntriesResult
rafty::Raft::handle_append_entries(const raftpb::AppendEntriesRequest &req) {
    AppendEntriesResult result;
    result.success = false;

    // Everything happens strictly inside the lock to maintain ordering
    std::scoped_lock lock(this->mtx);
    result.term = this->current_term;

    // 1. Reject stale leader
    if (req.term() < this->current_term)
        return result;

    // 2. Update term if newer
    if (req.term() > this->current_term) {
        this->current_term = req.term();
        this->voted_for    = -1;
    }

    // 3. Valid leader — step down and reset timer
    this->is_leader_ = false;
    this->election_deadline = std::chrono::steady_clock::now() +
        std::chrono::milliseconds(150 + (std::rand() % 150));

    // 4. Consistency check
    uint64_t prev_log_index = req.prev_log_index();
    uint64_t prev_log_term  = req.prev_log_term();

    if (prev_log_index >= this->log_.size()) {
        result.term = this->current_term;
        return result;
    }
    
    if (this->log_[prev_log_index].term() != prev_log_term) {
        result.term = this->current_term;
        return result;
    }

    // 5. Append / overwrite entries
    uint64_t log_index = prev_log_index + 1;
    for (int i = 0; i < req.entries_size(); i++) {
        const auto& entry = req.entries(i);
        if (log_index < this->log_.size()) {
            if (this->log_[log_index].term() != entry.term()) {
                this->log_.resize(log_index); // truncate conflicting tail
            } else {
                log_index++;
                continue; // already have matching entry
            }
        }
        this->log_.push_back(entry);
        log_index++;
    }

    // 6. Advance commit index
    if (req.leader_commit() > this->commit_index_) {
        uint64_t last_new = req.prev_log_index() + req.entries_size();
        uint64_t new_commit = std::min(req.leader_commit(), last_new);

        if (new_commit > this->commit_index_) {
            this->commit_index_ = new_commit;
            
            // MINIMAL FIX: Apply immediately inside the lock to guarantee strict ordering.
            // No more `to_apply` vector gathering.
            while (this->last_applied_ < this->commit_index_) {
                this->last_applied_++;
                if (this->last_applied_ < this->log_.size()) {
                    ApplyResult ar;
                    ar.index = this->last_applied_;
                    ar.data  = this->log_[this->last_applied_].data();
                    this->apply(ar); 
                }
            }
        }
    }

    result.term    = this->current_term;
    result.success = true;

    return result;
} // lock is released here

void Raft::update_commit_index() {
    // Note: This function assumes the caller has already acquired this->mtx.
    if (!this->is_leader_) return;

    int majority = (this->peers_.size() + 1) / 2 + 1;
    uint64_t last_index = this->log_.size() - 1;

    // Scan backwards from the end of the log down to the current commit_index_
    for (uint64_t n = last_index; n > this->commit_index_; n--) {
        
        // Figure 8 rule: Leaders cannot commit entries from older terms by counting replicas.
        // We only check for a majority if the entry at index 'n' belongs to our current_term.
        if (this->log_[n].term() == this->current_term) {
            
            int replicated = 1; // The leader always has the entry
            
            for (const auto& [peer_id, match_idx] : this->match_index_) {
                if (match_idx >= n) {
                    replicated++;
                }
            }

            // If a majority of nodes have this entry, it is safe to commit.
            if (replicated >= majority) {
                this->commit_index_ = n;

                // Safely apply all unapplied, newly committed entries in strict sequential order.
                while (this->last_applied_ < this->commit_index_) {
                    this->last_applied_++;
                    
                    if (this->last_applied_ < this->log_.size()) {
                        ApplyResult ar;
                        ar.index = this->last_applied_;
                        ar.data  = this->log_[this->last_applied_].data();
                        
                        // Push to the thread-safe queue. Because we are inside the leader's 
                        // state lock, no other thread can interleave applies.
                        this->apply(ar);
                    }
                }
                
                // Once we find the highest committable N and apply up to it, we are done.
                break; 
            }
        }
    }
}

void Raft::apply_committed_entries() {
    // This function should only be called WITHOUT holding the lock.
    // Collect under lock, apply outside.
    std::vector<ApplyResult> to_apply;
    {
        std::scoped_lock lock(this->mtx);
        while (this->last_applied_ < this->commit_index_) {
            this->last_applied_++;
            if (this->last_applied_ < this->log_.size()) {
                ApplyResult ar;
                ar.index = this->last_applied_;
                ar.data  = this->log_[this->last_applied_].data();
                to_apply.push_back(ar);
            }
        }
    }
    for (auto& ar : to_apply)
        this->apply(ar);
}

rafty::Raft::RequestVoteResult
rafty::Raft::handle_request_vote(uint64_t term, uint64_t candidate_id,
                                 uint64_t last_log_index,
                                 uint64_t last_log_term) {
    std::scoped_lock lock(this->mtx);

    RequestVoteResult result;
    result.term        = this->current_term;
    result.vote_granted = false;

    if (term < this->current_term)
        return result;

    if (term > this->current_term) {
        this->current_term = term;
        this->voted_for    = -1;
        this->is_leader_   = false;
    }

    if (this->voted_for == -1 ||
        this->voted_for == static_cast<int64_t>(candidate_id)) {

        uint64_t my_last_index = this->log_.back().index();
        uint64_t my_last_term  = this->log_.back().term();

        bool log_ok =
            (last_log_term > my_last_term) ||
            (last_log_term == my_last_term && last_log_index >= my_last_index);

        if (log_ok) {
            this->voted_for = static_cast<int64_t>(candidate_id);
            result.vote_granted = true;
            // Reset election timer when granting vote
            this->election_deadline = std::chrono::steady_clock::now() +
                std::chrono::milliseconds(150 + (std::rand() % 150));
        }
    }

    result.term = this->current_term;
    return result;
}

// rafty::Raft::AppendEntriesResult
// rafty::Raft::handle_append_entries(const raftpb::AppendEntriesRequest &req) {

//     std::vector<ApplyResult> to_apply;
//     AppendEntriesResult result;
//     result.success = false;

//     {
//         std::scoped_lock lock(this->mtx);
//         result.term = this->current_term;

//         // 1. Reject stale leader
//         if (req.term() < this->current_term)
//             return result;

//         // 2. Update term if newer
//         if (req.term() > this->current_term) {
//             this->current_term = req.term();
//             this->voted_for    = -1;
//         }

//         // 3. Valid leader — step down and reset timer
//         this->is_leader_ = false;
//         this->election_deadline = std::chrono::steady_clock::now() +
//             std::chrono::milliseconds(150 + (std::rand() % 150));

//         // 4. Consistency check
//         uint64_t prev_log_index = req.prev_log_index();
//         uint64_t prev_log_term  = req.prev_log_term();

//         if (prev_log_index >= this->log_.size()) {
//             result.term = this->current_term;
//             return result;
//         }
//         if (this->log_[prev_log_index].term() != prev_log_term) {
//             result.term = this->current_term;
//             return result;
//         }

//         // 5. Append / overwrite entries
//         uint64_t log_index = prev_log_index + 1;
//         for (int i = 0; i < req.entries_size(); i++) {
//             const auto& entry = req.entries(i);
//             if (log_index < this->log_.size()) {
//                 if (this->log_[log_index].term() != entry.term())
//                     this->log_.resize(log_index); // truncate conflicting tail
//                 else {
//                     log_index++;
//                     continue; // already have matching entry
//                 }
//             }
//             this->log_.push_back(entry);
//             log_index++;
//         }

//         // 6. Advance commit index
//         if (req.leader_commit() > this->commit_index_) {
//             uint64_t last_new = req.prev_log_index() + req.entries_size();
//             uint64_t new_commit = std::min(req.leader_commit(), last_new);

//             // MINIMAL FIX: Prevent rollback & immediately push to apply queue
//             if (new_commit > this->commit_index_) {
//                 this->commit_index_ = new_commit;
                
//                 while (this->last_applied_ < this->commit_index_) {
//                     this->last_applied_++;
//                     if (this->last_applied_ < this->log_.size()) {
//                         ApplyResult ar;
//                         ar.index = this->last_applied_;
//                         ar.data  = this->log_[this->last_applied_].data();
//                         to_apply.push_back(ar);
//                     }
//                 }
//             }
        

//             // Collect — do NOT call apply() while holding lock
//             // while (this->last_applied_ < this->commit_index_) {
//             //     this->last_applied_++;
//             //     if (this->last_applied_ < this->log_.size()) {
//             //         ApplyResult ar;
//             //         ar.index = this->last_applied_;
//             //         ar.data  = this->log_[this->last_applied_].data();
//             //         to_apply.push_back(ar);
//             //     }
//             // }
//         }

//         result.term    = this->current_term;
//         result.success = true;
//     } // lock released

//     // Apply outside the lock — no deadlock risk
//     for (auto& ar : to_apply)
//         this->apply(ar);

//     return result;
// }

ProposalResult Raft::propose(const std::string &data) {
    uint64_t new_index;
    uint64_t term_snapshot;

    std::unique_lock<std::mutex> lock(this->mtx);

    if (!this->is_leader_)
        return ProposalResult{0, this->current_term, false};

    new_index     = this->log_.size();
    term_snapshot = this->current_term;

    raftpb::Entry entry;
    entry.set_term(this->current_term);
    entry.set_index(new_index);
    entry.set_data(data);
    this->log_.push_back(entry);

    // Wake up the background loop immediately to batch proposals
    this->next_heartbeat_deadline = std::chrono::steady_clock::now();
    
    return ProposalResult{new_index, term_snapshot, true};
}

ProposalResult Raft::propose_sync(const std::string &data) {
    (void)data;
    return ProposalResult{0, this->current_term, false};
}

State Raft::get_state() const {
    std::scoped_lock lock(this->mtx);
    return State{this->current_term, this->is_leader_};
}

} // namespace rafty