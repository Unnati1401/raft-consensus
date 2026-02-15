#pragma once
#ifndef RAFTY_RAFT_SERVICE_IMPL_HPP
#define RAFTY_RAFT_SERVICE_IMPL_HPP

#include <memory>
#include <mutex>

#include "raft.pb.h"
#include "raft.grpc.pb.h"
#include <grpcpp/grpcpp.h>

namespace rafty {

class Raft; 

class RaftServiceImpl final : public raftpb::RaftService::Service {
public:
  explicit RaftServiceImpl(Raft* raft) noexcept;

  grpc::Status RequestVote(grpc::ServerContext* context,
                           const raftpb::RequestVoteRequest* request,
                           raftpb::RequestVoteResponse* response) override;

  grpc::Status AppendEntries(grpc::ServerContext* context,
                             const raftpb::AppendEntriesRequest* request,
                             raftpb::AppendEntriesResponse* response) override;

  RaftServiceImpl(const RaftServiceImpl&) = delete;
  RaftServiceImpl& operator=(const RaftServiceImpl&) = delete;

private:
  Raft* raft_; 
};

} 

#endif 