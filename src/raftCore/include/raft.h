#pragma once

#include <boost/serialization/string.hpp>
#include <boost/serialization/vector.hpp>
#include <chrono>
#include <cmath>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>
#include "ApplyMsg.h"
#include "Persister.h"
#include "boost/any.hpp"
#include "boost/serialization/serialization.hpp"
#include "config.h"
#include "Fiber.h"
#include "raftRpcUtil.h"
#include "util.h"

// 网络状态表示  todo：可以在rpc中删除该字段，实际生产中是用不到的.
constexpr int Disconnected = 0;  // 方便网络分区的时候debug，网络异常的时候为disconnected，只要网络正常就为AppNormal，防止matchIndex[]数组异常减小
constexpr int AppNormal = 1;

// 投票状态
constexpr int Killed = 0;
constexpr int Voted = 1;   //本轮已经投过票了
constexpr int Expire = 2;  //投票（消息、竞选者）过期
constexpr int Normal = 3;

class Raft : public raftRpcProtoc::raftRpc
{
public:
    // 领导者选举(2A)
    int getLastLogIndex();
    int getLastLogTerm();
    void getLastLogIndexAndTerm(int *lastLogIndex, int *lastLogTerm);
    bool UpToDate(int index, int term);
    void RequestVote(const raftRpcProtoc::RequestVoteArgs *args, raftRpcProtoc::RequestVoteReply *reply);
    bool sendRequestVote(int server, std::shared_ptr<raftRpcProtoc::RequestVoteArgs> args,
                       std::shared_ptr<raftRpcProtoc::RequestVoteReply> reply, std::shared_ptr<int> votedNum);
    void doElection();
    // 每隔一段时间检查睡眠时间内有没有重置定时器，没有则说明超时了
    // 如果有则设置合适睡眠时间：睡眠到重置时间+超时时间
    void electionTimeOutTicker();
    void GetState(int *term, bool *isLeader);
    bool matchLog(int logIndex, int logTerm);


    //日志复制(2B)
    void doHeartBeat();
    int getNewCommandIndex();
    void Start(Op command, int *newLogIndex, int *newLogTerm, bool *isLeader);
    void getPrevLogInfo(int server, int *preIndex, int *preTerm);
    void AppendEntries1(const raftRpcProtoc::AppendEntriesArgs *args, raftRpcProtoc::AppendEntriesReply *reply);
    bool sendAppendEntries(int server, std::shared_ptr<raftRpcProtoc::AppendEntriesArgs> args,
                         std::shared_ptr<raftRpcProtoc::AppendEntriesReply> reply, std::shared_ptr<int> appendNums);
    void leaderUpdateCommitIndex();
    void leaderHearBeatTicker();
    std::vector<ApplyMsg> getApplyLogs();
    void applierTicker();
    void pushMsgToKvServer(ApplyMsg msg);


    //持久化(2C)
    void persist();
    std::string persistData();
    void readPersist(std::string data);


    //快照(2D)
    int getLogTermFromLogIndex(int logIndex);
    int getSlicesIndexFromLogIndex(int logIndex);
    int GetRaftStateSize();
    void Snapshot(int index, std::string snapshot);
    void InstallSnapshot(const raftRpcProtoc::InstallSnapshotRequest *args,
                       raftRpcProtoc::InstallSnapshotResponse *reply);
    void leaderSendSnapShot(int server);
    bool CondInstallSnapshot(int lastIncludedTerm, int lastIncludedIndex, std::string snapshot);

public:
    //RPC转发
    void AppendEntries(google::protobuf::RpcController *controller, const ::raftRpcProtoc::AppendEntriesArgs *request,
                    ::raftRpcProtoc::AppendEntriesReply *response, ::google::protobuf::Closure *done) override;
    void InstallSnapshot(google::protobuf::RpcController *controller,
                       const ::raftRpcProtoc::InstallSnapshotRequest *request,
                       ::raftRpcProtoc::InstallSnapshotResponse *response, ::google::protobuf::Closure *done) override;
    void RequestVote(google::protobuf::RpcController *controller, const ::raftRpcProtoc::RequestVoteArgs *request,
                   ::raftRpcProtoc::RequestVoteReply *response, ::google::protobuf::Closure *done) override;
public:
    //初始化
    void init(std::vector<std::shared_ptr<RaftRpcUtil>> peers, int me, std::shared_ptr<Persister> persister,
            std::shared_ptr<LockQueue<ApplyMsg>> applyCh);
private:
    std::mutex m_mtx;
    std::vector<std::shared_ptr<RaftRpcUtil>> m_peers;
    std::shared_ptr<Persister> m_persister;
    int m_me;
    int m_currentTerm;
    int m_votedFor;
    std::vector<raftRpcProtoc::LogEntry> m_logs; // 日志条目数组，包含了状态机要执行的指令集，以及收到领导时的任期号
                                                  // 这两个状态所有结点都在维护，易失
    int m_commitIndex;
    int m_lastApplied; // 已经汇报给状态机（上层应用）的log 的index

    // 这两个状态是由服务器来维护，易失
    std::vector<int> m_nextIndex; // 这两个状态的下标1开始，因为通常commitIndex和lastApplied从0开始，应该是一个无效的index，因此下标从1开始
    std::vector<int> m_matchIndex;
    enum Status
    {
        Follower,
        Candidate,
        Leader
    };
    // 身份
    Status m_status;

    std::shared_ptr<LockQueue<ApplyMsg>> applyChan; // client从这里取日志（2B），client与raft通信的接口
    // ApplyMsgQueue chan ApplyMsg // raft内部使用的chan，applyChan是用于和服务层交互，最后好像没用上

    // 选举超时
    std::chrono::_V2::system_clock::time_point m_lastResetElectionTime;
    // 心跳超时，用于leader
    std::chrono::_V2::system_clock::time_point m_lastResetHearBeatTime;

    // 2D中用于传入快照点
    // 储存了快照中的最后一个日志的Index和Term
    int m_lastSnapshotIncludeIndex;
    int m_lastSnapshotIncludeTerm;

    // 协程
    std::unique_ptr<IOManager> m_ioManager = nullptr;
private:
    //for persist
    class BoostPersistRaftNode
    {
    public:
        friend class boost::serialization::access;
        template <class Archive>
        void serialize(Archive &ar, const unsigned int version)
        {
            ar & m_currentTerm;
            ar & VotedFor;
            ar & m_lastSnapshotIncludeIndex;
            ar & m_lastSnapshotIncludeTerm;
            ar & m_logs;
        }

        int m_currentTerm;
        int VotedFor;
        int m_lastSnapshotIncludeIndex;
        int m_lastSnapshotIncludeTerm;
        std::vector<std::string> m_logs;
    };
};
