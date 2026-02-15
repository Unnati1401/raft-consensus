#include "rafty/raft_service_impl.hpp"
#include "rafty/raft.hpp"

namespace rafty {

RaftServiceImpl::RaftServiceImpl(Raft* raft) noexcept : raft_(raft) {}

grpc::Status RaftServiceImpl::RequestVote(
    grpc::ServerContext*,
    const raftpb::RequestVoteRequest* request,
    raftpb::RequestVoteResponse* response) {
  auto result = raft_->handle_request_vote(
      request->term(),
      request->candidate_id(),
      request->last_log_index(),
      request->last_log_term());

  response->set_term(result.term);
  response->set_vote_granted(result.vote_granted);

  return grpc::Status::OK;
}

grpc::Status RaftServiceImpl::AppendEntries(
    grpc::ServerContext*,
    const raftpb::AppendEntriesRequest* request,
    raftpb::AppendEntriesResponse* response) {

  auto result = raft_->handle_append_entries(*request);

  response->set_term(result.term);
  response->set_success(result.success);

  return grpc::Status::OK;

}

} // namespace rafty