#include "raftRpcUtil.h"
#include "mprpcchannel.h"
#include "mprpccontroller.h"

RaftRpcUtil::RaftRpcUtil(std::string ip, short port)
{
    stub_ = new raftRpcProtoc::raftRpc_Stub(new MprpcChannel(ip, port, true));
}

RaftRpcUtil::~RaftRpcUtil()
{
    delete stub_;
}

bool RaftRpcUtil::AppendEntries(raftRpcProtoc::AppendEntriesArgs *args, raftRpcProtoc::AppendEntriesReply *response)
{
    MprpcController controller;
    stub_->AppendEntries(&controller, args, response, nullptr);
    return !controller.Failed();
}

bool RaftRpcUtil::InstallSnapshot(raftRpcProtoc::InstallSnapshotRequest *request, raftRpcProtoc::InstallSnapshotResponse *response)
{
    MprpcController controller;
    stub_->InstallSnapshot(&controller, request, response, nullptr);
    return !controller.Failed();
}

bool RaftRpcUtil::RequestVote(raftRpcProtoc::RequestVoteArgs *args, raftRpcProtoc::RequestVoteReply *reply)
{
    MprpcController controller;
    stub_->RequestVote(&controller, args, reply, nullptr);
    return !controller.Failed();
}