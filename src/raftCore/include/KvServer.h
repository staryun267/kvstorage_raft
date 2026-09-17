#pragma once

#include <boost/any.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/foreach.hpp>
#include <boost/serialization/export.hpp>
#include <boost/serialization/serialization.hpp>
#include <boost/serialization/unordered_map.hpp>
#include <boost/serialization/vector.hpp>
#include <iostream>
#include <mutex>
#include <unordered_map>
#include "kvServerRpc.pb.h"
#include "raft.h"
#include "skiplist.h"

class KvServer : raftKVRpcProctoc::kvServerRpc
{
public:
    KvServer() = delete;

    KvServer(int me, int maxraftstate, std::string nodeInforFileName, short port);

    void ReadSnapShotToInstall(std::string snapshot);

    void ReadRaftApplyCommandLoop();

    void GetCommandFromRaft(ApplyMsg message);

    bool ifRequestDuplicate(std::string ClientId, int RequestId);

    void ExecutePutOpOnKVDB(Op op);

    void ExecuteAppendOpOnKVDB(Op op);

    void ExecuteGetOpOnKVDB(Op op, std::string *value, bool *exist);

    void IfNeedToSendSnapShotCommand(int raftIndex, int proportion);

    std::string MakeSnapShot();

    bool SendMessageToWaitChan(const Op &op, int raftIndex);

    void GetSnapShotFromRaft(ApplyMsg message);

    void DprintfKVDB();

    void PutAppend(const raftKVRpcProctoc::PutAppendArgs *args, raftKVRpcProctoc::PutAppendReply *reply);

    void Get(const raftKVRpcProctoc::GetArgs *args, raftKVRpcProctoc::GetReply *reply);
public:     //rpc
    void PutAppend(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::PutAppendArgs *request,
                 ::raftKVRpcProctoc::PutAppendReply *response, ::google::protobuf::Closure *done) override;
    void Get(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::GetArgs *request,
           ::raftKVRpcProctoc::GetReply *response, ::google::protobuf::Closure *done) override;
private:
    friend class boost::serialization::access;
    template <class Archive>
    void serialize(Archive &ar, const unsigned int version)
    {
        ar &m_serializedKVData;
        ar &m_lastRequestId;
    }

    std::string getSnapshotData()
    {
        m_serializedKVData = m_skipList.dump_file();
        std::stringstream ss;
        boost::archive::text_oarchive oa(ss);
        oa << *this;
        m_serializedKVData.clear();
        return ss.str();
    }

    void parseFromString(const std::string &str)
    {
        std::stringstream iss(str);
        boost::archive::text_iarchive ia(iss);
        ia >> *this;
        m_skipList.load_file(m_serializedKVData);
        m_serializedKVData.clear();
    }
private:
    std::mutex m_mtx;
    int m_me;
    std::shared_ptr<Raft> m_raftNode;
    std::shared_ptr<LockQueue<ApplyMsg>> applyChan; // kvServer和raft节点的通信管道
    int m_maxRaftState;                             // snapshot if log grows this big

    // Your definitions here.
    std::string m_serializedKVData; // todo ： 序列化后的kv数据，理论上可以不用，但是目前没有找到特别好的替代方法
    SkipList<std::string, std::string> m_skipList;
    std::unordered_map<std::string, std::string> m_kvDB;

    std::unordered_map<int, LockQueue<Op> *> waitApplyCh;
    // index(raft) -> chan  //？？？字段含义   waitApplyCh是一个map，键是int，值是Op类型的管道

    std::unordered_map<std::string, int> m_lastRequestId; // clientid -> requestID  //一个kV服务器可能连接多个client

    // last SnapShot point , raftIndex
    int m_lastSnapShotRaftLogIndex;
};