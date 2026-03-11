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
            this->election_deadline = now + std::chrono::milliseconds(150 + (std::rand() % 150));
        }

        while (!this->is_dead()) {
            auto now = std::chrono::steady_clock::now();
            bool should_heartbeat = false;
            bool should_elect = false;

            {
                std::unique_lock<std::mutex> lock(this->mtx);
                if (this->is_leader_) {
                    if (now >= this->next_heartbeat_deadline) {
                        this->next_heartbeat_deadline = now + std::chrono::milliseconds(50);
                        should_heartbeat = true;
                    }
                } else {
                    if (now >= this->election_deadline) {
                        should_elect = true;
                    }
                }
            }

            if (should_heartbeat) this->send_heartbeats();
            if (should_elect)     this->start_election();
            this->apply_committed_entries(); 
            
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }).detach();
}

void Raft::start_election() {
    uint64_t term_at_election;
    uint64_t last_log_idx;
    uint64_t last_log_term;

    {
        std::scoped_lock lock(this->mtx);
        this->current_term++;
        this->voted_for = this->id;
        this->is_leader_ = false;
        this->election_deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(150 + (std::rand() % 150));

        term_at_election = this->current_term;
        last_log_idx = this->log_.size() - 1;
        last_log_term = this->log_[last_log_idx].term();
    }

    auto votes_received = std::make_shared<std::atomic<int>>(1);
    int majority = (int)(this->peer_addrs.size() + 1) / 2 + 1;

    for (const auto& [peer_id, stub] : this->peers_) {
        std::thread([this, peer_id, term_at_election, last_log_idx, last_log_term, votes_received, majority]() {
            raftpb::RequestVoteRequest req;
            req.set_term(term_at_election);
            req.set_candidate_id(this->id);
            req.set_last_log_index(last_log_idx);
            req.set_last_log_term(last_log_term);

            auto context = this->create_context(peer_id);
            raftpb::RequestVoteResponse res;
            auto status = this->peers_[peer_id]->RequestVote(context.get(), req, &res);

            if (!status.ok()) return;

            std::scoped_lock lock(this->mtx);
            if (res.term() > this->current_term) {
                this->current_term = res.term();
                this->voted_for = -1;
                this->is_leader_ = false;
                return;
            }

            if (this->current_term != term_at_election || this->is_leader_) return;

            if (res.vote_granted()) {
                if (++(*votes_received) == majority) {
                    this->is_leader_ = true;
                    uint64_t last_idx = this->log_.size() - 1;
                    for (auto const& [pid, _] : this->peers_) {
                        this->next_index_[pid] = last_idx + 1;
                        this->match_index_[pid] = 0;
                    }
                    this->next_heartbeat_deadline = std::chrono::steady_clock::now();
                }
            }
        }).detach();
    }
}

void Raft::send_heartbeats() {
    std::scoped_lock lock(this->mtx);
    if (!this->is_leader_) return;

    for (const auto& [peer_id, stub] : this->peers_) {
        raftpb::AppendEntriesRequest req;
        req.set_term(this->current_term);
        req.set_leader_id(this->id);
        req.set_leader_commit(this->commit_index_);

        uint64_t next = this->next_index_[peer_id];
        uint64_t prev = next - 1;
        
        req.set_prev_log_index(prev);
        req.set_prev_log_term(this->log_[prev].term());

        for (size_t i = next; i < this->log_.size(); ++i) {
            *req.add_entries() = this->log_[i];
        }

        std::thread([this, peer_id, req]() {
            auto context = this->create_context(peer_id);
            raftpb::AppendEntriesResponse res;
            auto status = this->peers_[peer_id]->AppendEntries(context.get(), req, &res);

            if (!status.ok()) return;

            std::scoped_lock lock(this->mtx);
            if (res.term() > this->current_term) {
                this->current_term = res.term();
                this->is_leader_ = false;
                this->voted_for = -1;
                return;
            }

            if (!this->is_leader_ || req.term() != this->current_term) return;

            if (res.success()) {
                uint64_t match = req.prev_log_index() + req.entries_size();
                this->match_index_[peer_id] = std::max(this->match_index_[peer_id], match);
                this->next_index_[peer_id] = this->match_index_[peer_id] + 1;
                this->update_commit_index();
            } else {
                if (this->next_index_[peer_id] > 1) {
                    this->next_index_[peer_id]--;
                }
            }
        }).detach();
    }
}

rafty::Raft::AppendEntriesResult Raft::handle_append_entries(const raftpb::AppendEntriesRequest &req) {
    AppendEntriesResult res;
    std::scoped_lock lock(this->mtx);
    res.term = this->current_term;
    res.success = false;

    if (req.term() < this->current_term) return res;

    if (req.term() > this->current_term) {
        this->current_term = req.term();
        this->voted_for = -1;
    }
    this->is_leader_ = false;
    this->election_deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(150 + (std::rand() % 150));

    if (req.prev_log_index() >= this->log_.size() || this->log_[req.prev_log_index()].term() != req.prev_log_term()) {
        res.term = this->current_term;
        return res;
    }

    uint64_t idx = req.prev_log_index() + 1;
    for (int i = 0; i < req.entries_size(); i++) {
        if (idx < this->log_.size()) {
            if (this->log_[idx].term() != req.entries(i).term()) {
                this->log_.resize(idx);
                this->log_.push_back(req.entries(i));
            }
        } else {
            this->log_.push_back(req.entries(i));
        }
        idx++;
    }

    if (req.leader_commit() > this->commit_index_) {
        this->commit_index_ = std::min(req.leader_commit(), (uint64_t)this->log_.size() - 1);
    }

    res.success = true;
    res.term = this->current_term;
    return res;
}

void Raft::update_commit_index() {
    if (!this->is_leader_) return;
    int majority = (this->peers_.size() + 1) / 2 + 1;
    for (uint64_t n = this->log_.size() - 1; n > this->commit_index_; n--) {
        if (this->log_[n].term() == this->current_term) {
            int count = 1;
            for (auto const& [peer_id, m_idx] : this->match_index_) {
                if (m_idx >= n) count++;
            }
            if (count >= majority) {
                this->commit_index_ = n;
                break;
            }
        }
    }
}

void Raft::apply_committed_entries() {
    std::vector<ApplyResult> to_apply;
    {
        std::scoped_lock lock(this->mtx);
        while (this->last_applied_ < this->commit_index_) {
            uint64_t next = this->last_applied_ + 1;
            if (next < this->log_.size()) {
                ApplyResult ar;
                ar.index = next;
                ar.data = this->log_[next].data();
                ar.valid = true; // Set this based on your common.hpp
                to_apply.push_back(ar);
                this->last_applied_ = next;
            } else break;
        }
    }
    for (auto& entry : to_apply) {
        this->apply(entry);
    }
}

rafty::Raft::RequestVoteResult Raft::handle_request_vote(uint64_t term, uint64_t candidate_id, uint64_t last_log_index, uint64_t last_log_term) {
    std::scoped_lock lock(this->mtx);
    RequestVoteResult res;
    res.term = this->current_term;
    res.vote_granted = false;

    if (term < this->current_term) return res;

    if (term > this->current_term) {
        this->current_term = term;
        this->voted_for = -1;
        this->is_leader_ = false;
    }

    if (this->voted_for == -1 || this->voted_for == (int64_t)candidate_id) {
        uint64_t my_last_idx = this->log_.size() - 1;
        uint64_t my_last_term = this->log_[my_last_idx].term();
        bool up_to_date = (last_log_term > my_last_term) || (last_log_term == my_last_term && last_log_index >= my_last_idx);
        
        if (up_to_date) {
            this->voted_for = candidate_id;
            res.vote_granted = true;
            this->election_deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(150 + (std::rand() % 150));
        }
    }
    res.term = this->current_term;
    return res;
}

ProposalResult Raft::propose(const std::string &data) {
    std::scoped_lock lock(this->mtx);
    if (!this->is_leader_) {
        return ProposalResult{0, this->current_term, false};
    }

    uint64_t new_idx = this->log_.size();
    raftpb::Entry entry;
    entry.set_term(this->current_term);
    entry.set_index(new_idx);
    entry.set_data(data);
    this->log_.push_back(entry);

    this->next_heartbeat_deadline = std::chrono::steady_clock::now();
    return ProposalResult{new_idx, this->current_term, true};
}

ProposalResult Raft::propose_sync(const std::string &data) {
    // 1. Propose the data
    ProposalResult res = this->propose(data);
    
    // 2. If we aren't the leader (is_leader is false), return immediately
    if (!res.is_leader) {
        return res;
    }

    // 3. Wait until the entry is committed or we lose leadership
    const int timeout_ms = 2000;
    auto start = std::chrono::steady_clock::now();
    
    while (!this->is_dead()) {
        {
            std::scoped_lock lock(this->mtx);
            // Check if committed
            if (this->commit_index_ >= res.index) {
                if (this->log_[res.index].term() == res.term) {
                    return res; // Success
                } else {
                    res.is_leader = false; 
                    return res;
                }
            }
            
            // If we lost leadership or term changed while waiting
            if (!this->is_leader_ || this->current_term != res.term) {
                res.is_leader = false;
                return res;
            }
        }

        auto now = std::chrono::steady_clock::now();
        if (std::chrono::duration_cast<std::chrono::milliseconds>(now - start).count() > timeout_ms) {
            res.is_leader = false;
            return res;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    res.is_leader = false;
    return res;
}

State Raft::get_state() const {
    std::scoped_lock lock(this->mtx);
    return State{this->current_term, this->is_leader_};
}

} // namespace rafty