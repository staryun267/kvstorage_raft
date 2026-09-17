#pragma once

#include "raftRPC.pb.h"

class RaftRpcUtil
{
public:
    RaftRpcUtil(std::string ip, short port);

    ~RaftRpcUtil();
    
    bool AppendEntries(raftRpcProtoc::AppendEntriesArgs* args, raftRpcProtoc::AppendEntriesReply* response);

    bool InstallSnapshot(raftRpcProtoc::InstallSnapshotRequest* request, raftRpcProtoc::InstallSnapshotResponse* response);

    bool RequestVote(raftRpcProtoc::RequestVoteArgs* args, raftRpcProtoc::RequestVoteReply* reply);
private:
    raftRpcProtoc::raftRpc_Stub* stub_;
};