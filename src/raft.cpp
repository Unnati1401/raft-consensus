#include "common/utils/rand_gen.hpp"
#include "rafty/raft.hpp"
#ifdef TRACING
#include "common/utils/tracing.hpp"
#endif

namespace {
// Replicator-only timing (does not rely on Raft::HEARTBEAT_MS in raft.hpp).
// Keep heartbeats frequent enough for read lease freshness, but not so frequent
// that background AppendEntries traffic starves foreground proposal/commit work.
constexpr int kAppendEntriesPeriodMs = 50;
constexpr int kLeaderReadLeaseMs = 40;
} // namespace

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

  {
    std::scoped_lock lock(this->mtx);
    auto now = std::chrono::steady_clock::now();
    this->election_deadline =
        now + std::chrono::milliseconds(150 + (std::rand() % 150));
    for (const auto &[peer_id, _stub] : this->peers_) {
      auto ps = std::make_unique<PeerSync>();
      ps->next_heartbeat = now;
      this->peer_sync_[peer_id] = std::move(ps);
    }
  }

  // Start one replicator thread per peer.
  for (const auto &[peer_id, _stub] : this->peers_) {
    std::thread([this, peer_id]() { this->replicator_loop(peer_id); }).detach();
  }

  // Start the dedicated apply thread.
  std::thread([this]() { this->apply_loop(); }).detach();

  // Election-timeout loop. Runs in its own thread.
  std::thread([this]() {
    this->logger->info("Raft node {} election loop started", id);
    while (!this->is_dead()) {
      bool should_elect = false;
      {
        std::unique_lock<std::mutex> lock(this->mtx);
        cv_.wait_until(lock, this->election_deadline,
                       [this] { return needs_work_ || is_dead(); });
        needs_work_ = false;

        auto now = std::chrono::steady_clock::now();
        if (!this->is_leader_ && now >= this->election_deadline) {
          should_elect = true;
        }
      }
      if (should_elect) this->start_election();
    }
  }).detach();
}

void Raft::replicator_loop(uint64_t peer_id) {
    while (!this->is_dead()) {
        raftpb::AppendEntriesRequest req;
        bool send_now = false;

        {
            std::unique_lock<std::mutex> lock(this->mtx);
            auto &ps = *this->peer_sync_[peer_id];

            // 1. Wait for work or heartbeat timeout
            ps.cv.wait_until(lock, ps.next_heartbeat, [&] {
                return this->is_dead() || 
                       (this->is_leader_ && (this->next_index_[peer_id] < this->log_.size() || 
                        std::chrono::steady_clock::now() >= ps.next_heartbeat));
            });

            // Immediately reset the work flag so propose() can signal us again for the NEXT batch
            ps.has_work.store(false);

            if (this->is_dead()) return;
            
            // If we lost leadership while sleeping, reset heartbeat and wait again
            if (!this->is_leader_) {
                ps.next_heartbeat = std::chrono::steady_clock::now() +
                    std::chrono::milliseconds(kAppendEntriesPeriodMs);
                continue;
            }

            // 2. Build the Batch Request (batching is capped at 1000 entries per RPC)
            req.set_term(this->current_term);
            req.set_leader_id(this->id);
            req.set_leader_commit(this->commit_index_);

            uint64_t next = this->next_index_[peer_id];
            if (next == 0) next = 1;
            uint64_t prev = next - 1;
            
            // Bounds check
            if (prev >= this->log_.size()) prev = this->log_.size() - 1;

            req.set_prev_log_index(prev);
            req.set_prev_log_term(this->log_[prev].term());

            // Pack as many entries as we have into this single RPC
            // (Standard gRPC limit is 4MB, so ~1000 entries is very safe)
            for (size_t i = next; i < this->log_.size(); ++i) {
                *req.add_entries() = this->log_[i];
                if (req.entries_size() >= 1000) break; 
            }

            ps.next_heartbeat = std::chrono::steady_clock::now() +
                std::chrono::milliseconds(kAppendEntriesPeriodMs);
            send_now = true;
        }

        if (!send_now) continue;

        // 3. RPC Call (Perform the network IO outside the global mutex!)
        auto context = this->create_context(peer_id);
        raftpb::AppendEntriesResponse res;
        auto status = this->peers_[peer_id]->AppendEntries(context.get(), req, &res);

        if (!status.ok()) {
            // On network error, back off slightly to avoid log-spamming/spinning
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            continue;
        }

        bool commit_advanced = false;
        {
            std::scoped_lock lock(this->mtx);
            
            // Check if we found a higher term (we are no longer leader)
            if (res.term() > this->current_term) {
                this->current_term = res.term();
                this->is_leader_ = false;
                this->voted_for = -1;
                // Invalidate lease
                this->lease_deadline_ns.store(std::chrono::steady_clock::now().time_since_epoch().count());
                needs_work_ = true;
                cv_.notify_one();
                continue;
            }

            if (!this->is_leader_ || req.term() != this->current_term) continue;

            if (res.success()) {
                uint64_t match = req.prev_log_index() + req.entries_size();
                this->match_index_[peer_id] = std::max(this->match_index_[peer_id], match);
                this->next_index_[peer_id] = this->match_index_[peer_id] + 1;

                // Successful AppendEntries extends the leader lease for local reads.
                auto new_deadline = std::chrono::steady_clock::now() +
                    std::chrono::milliseconds(kLeaderReadLeaseMs);
                this->lease_deadline_ns.store(new_deadline.time_since_epoch().count());

                uint64_t prev_commit = this->commit_index_;
                this->update_commit_index();
                if (this->commit_index_ > prev_commit) commit_advanced = true;
            } else {
                // Conflict resolution: simple decrement
                if (this->next_index_[peer_id] > 1) this->next_index_[peer_id]--;
            }
        }

        // If the commit index moved, wake up the apply_loop thread
        if (commit_advanced) apply_cv_.notify_one();
    }
}
void Raft::apply_loop() {
  while (!this->is_dead()) {
    std::vector<ApplyResult> to_apply;
    {
      std::unique_lock<std::mutex> lock(this->mtx);
      apply_cv_.wait(lock, [this] {
        return this->is_dead() || this->commit_index_ > this->last_applied_;
      });
      if (this->is_dead()) return;

      while (this->last_applied_ < this->commit_index_) {
        uint64_t next = this->last_applied_ + 1;
        if (next < this->log_.size()) {
          ApplyResult ar;
          ar.index = next;
          ar.data = this->log_[next].data();
          ar.valid = true;
          to_apply.push_back(ar);
          this->last_applied_ = next;
        } else break;
      }
    }
    for (auto &entry : to_apply) {
      this->apply(entry);
    }
  }
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
    this->election_deadline = std::chrono::steady_clock::now() +
                              std::chrono::milliseconds(150 + (std::rand() % 150));

    term_at_election = this->current_term;
    last_log_idx = this->log_.size() - 1;
    last_log_term = this->log_[last_log_idx].term();
  }

  auto votes_received = std::make_shared<std::atomic<int>>(1);
  int majority = (int)(this->peer_addrs.size() + 1) / 2 + 1;

  for (const auto &[peer_id, _stub] : this->peers_) {
    std::thread([this, peer_id, term_at_election, last_log_idx, last_log_term,
                 votes_received, majority]() {
      raftpb::RequestVoteRequest req;
      req.set_term(term_at_election);
      req.set_candidate_id(this->id);
      req.set_last_log_index(last_log_idx);
      req.set_last_log_term(last_log_term);

      auto context = this->create_context(peer_id);
      raftpb::RequestVoteResponse res;
      auto status = this->peers_[peer_id]->RequestVote(context.get(), req, &res);
      if (!status.ok()) return;

      bool became_leader = false;
      {
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
            for (auto const &[pid, _] : this->peers_) {
              this->next_index_[pid] = last_idx + 1;
              this->match_index_[pid] = 0;
            }
            became_leader = true;
          }
        }
      }

      if (became_leader) {
        // Wake all replicators to send the initial heartbeat.
        std::scoped_lock lock(this->mtx);
        for (auto &[pid, ps] : this->peer_sync_) {
          ps->next_heartbeat = std::chrono::steady_clock::now();
          ps->cv.notify_one();
        }
      }
    }).detach();
  }
}

void Raft::send_heartbeats() {
  std::scoped_lock lock(this->mtx);
  if (!this->is_leader_) return;
  auto now = std::chrono::steady_clock::now();
  for (auto &[pid, ps] : this->peer_sync_) {
    ps->next_heartbeat = now;
    ps->cv.notify_one();
  }
}

rafty::Raft::AppendEntriesResult
Raft::handle_append_entries(const raftpb::AppendEntriesRequest &req) {
  AppendEntriesResult res;
  bool commit_advanced = false;
  {
    std::scoped_lock lock(this->mtx);
    res.term = this->current_term;
    res.success = false;

    if (req.term() < this->current_term) return res;

    if (req.term() > this->current_term) {
      this->current_term = req.term();
      this->voted_for = -1;
    }
    this->is_leader_ = false;
    this->election_deadline = std::chrono::steady_clock::now() +
                              std::chrono::milliseconds(150 + (std::rand() % 150));

    needs_work_ = true;
    cv_.notify_one();

    if (req.prev_log_index() >= this->log_.size() ||
        this->log_[req.prev_log_index()].term() != req.prev_log_term()) {
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
      uint64_t prev_commit = this->commit_index_;
      this->commit_index_ =
          std::min(req.leader_commit(), (uint64_t)this->log_.size() - 1);
      if (this->commit_index_ > prev_commit) commit_advanced = true;
    }

    res.success = true;
    res.term = this->current_term;
  }

  if (commit_advanced) apply_cv_.notify_one();
  return res;
}

void Raft::update_commit_index() {
  if (!this->is_leader_) return;
  int majority = (this->peers_.size() + 1) / 2 + 1;
  for (uint64_t n = this->log_.size() - 1; n > this->commit_index_; n--) {
    if (this->log_[n].term() == this->current_term) {
      int count = 1;
      for (auto const &[peer_id, m_idx] : this->match_index_) {
        if (m_idx >= n) count++;
      }
      if (count >= majority) {
        this->commit_index_ = n;
        break;
      }
    }
  }
}

rafty::Raft::RequestVoteResult
Raft::handle_request_vote(uint64_t term, uint64_t candidate_id,
                          uint64_t last_log_index, uint64_t last_log_term) {
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
    bool up_to_date =
        (last_log_term > my_last_term) ||
        (last_log_term == my_last_term && last_log_index >= my_last_idx);

    if (up_to_date) {
      this->voted_for = candidate_id;
      res.vote_granted = true;
      this->election_deadline = std::chrono::steady_clock::now() +
                                std::chrono::milliseconds(150 + (std::rand() % 150));
    }
  }
  res.term = this->current_term;
  return res;
}

ProposalResult Raft::propose(const std::string &data) {
  uint64_t new_idx;
  uint64_t term;
  {
    std::scoped_lock lock(this->mtx);
    if (!this->is_leader_) {
      return ProposalResult{0, this->current_term, false};
    }

    new_idx = this->log_.size();
    term = this->current_term;

    raftpb::Entry entry;
    entry.set_term(term);
    entry.set_index(new_idx);
    entry.set_data(data);
    this->log_.push_back(entry);

    for (auto &[pid, ps] : this->peer_sync_) {
        // Only signal if we haven't signaled in this "batch cycle"
        if (!ps->has_work.exchange(true)) {
            ps->cv.notify_one();
        }
    }
  }

  return ProposalResult{new_idx, term, true};
}

ProposalResult Raft::propose_sync(const std::string &data) {
  ProposalResult res = this->propose(data);
  if (!res.is_leader) return res;

  const int timeout_ms = 2000;
  auto deadline =
      std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);

  while (!this->is_dead()) {
    {
      std::unique_lock<std::mutex> lock(this->mtx);
      apply_cv_.wait_until(lock, deadline, [&] {
        return this->commit_index_ >= res.index ||
               !this->is_leader_ ||
               this->current_term != res.term ||
               this->is_dead();
      });

      if (this->commit_index_ >= res.index) {
        if (this->log_[res.index].term() == res.term) return res;
        res.is_leader = false;
        return res;
      }
      if (!this->is_leader_ || this->current_term != res.term) {
        res.is_leader = false;
        return res;
      }
    }
    if (std::chrono::steady_clock::now() >= deadline) {
      res.is_leader = false;
      return res;
    }
  }

  res.is_leader = false;
  return res;
}

State Raft::get_state() const {
  std::scoped_lock lock(this->mtx);
  return State{this->current_term, this->is_leader_};
}

uint64_t Raft::get_read_index() const {
    auto now_ns = std::chrono::steady_clock::now().time_since_epoch().count();
    if (now_ns < this->lease_deadline_ns.load()) {
        return this->commit_index_;
    }
    return 0; 
}
} // namespace rafty