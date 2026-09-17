#include "KvServer.h"
#include "rpcprovider.h"
#include "mprpcconfig.h"

void KvServer::DprintfKVDB()
{
    if(!Debug)
    {
        return;
    }
    std::lock_guard<std::mutex> lock(m_mtx);
    DEFER{
        m_skipList.display_list();
    };
}

KvServer::KvServer(int me, int maxraftstate, std::string nodeInforFileName, short port)
    : m_skipList(6)
{
    std::shared_ptr<Persister> persister = std::make_shared<Persister>(me);

    m_me = me;
    m_maxRaftState = maxraftstate;

    applyChan = std::make_shared<LockQueue<ApplyMsg>>();
    m_raftNode = std::make_shared<Raft>();

    //开启服务提供
    std::thread t([this, port]() -> void {
        RpcProvider provider;
        provider.NotifyService(this);
        provider.NotifyService(this->m_raftNode.get());

        provider.Run(m_me, port);
    });
    t.detach();

    ////开启rpc远程调用能力，需要注意必须要保证所有节点都开启rpc接受功能之后才能开启rpc远程调用能力
    ////这里使用睡眠来保证
    //std::cout << "raftServer node:" << m_me << " start to sleep to wait all ohter raftnode start!!!!" << std::endl;
    sleep(6);
    //std::cout << "raftServer node:" << m_me << " wake up!!!! start to connect other raftnode" << std::endl;

    //获取所有raft节点的，ip，port，并进行连接
    MprpcConfig config;
    config.LoadConfigFile(nodeInforFileName.c_str());
    std::vector<std::pair<std::string, short>> ipPortVt;
    for(int i = 0; i < INT_MAX - 1; i++)
    {
        std::string node = "node" + std::to_string(i);

        std::string nodeIp = config.Load(node + "ip");
        std::string nodePortStr = config.Load(node + "port");

        if(nodeIp.empty())
        {
            break;
        }
        ipPortVt.emplace_back(nodeIp, atoi(nodePortStr.c_str()));
    }

    std::vector<std::shared_ptr<RaftRpcUtil>> servers;
    //进行连接
    for(int i = 0; i < ipPortVt.size(); i++)
    {
        if(i == m_me)
        {
            servers.push_back(nullptr);
            continue;
        }
        std::string otherNodeIp = ipPortVt[i].first;
        short otherNodePort = ipPortVt[i].second;

        auto *rpc = new RaftRpcUtil(otherNodeIp, otherNodePort);
        servers.push_back(std::shared_ptr<RaftRpcUtil>(rpc));
        std::cout << "node" << m_me << " 连接node" << i << "success!" << std::endl;
    }

    sleep(ipPortVt.size() - me);  //等待所有节点相互连接成功，再启动raft
    //初始化当前raft节点
    m_raftNode->init(servers, m_me, persister, applyChan);

    m_lastSnapShotRaftLogIndex = 0;
    auto snapshot = persister->ReadSnapshot();
    if(!snapshot.empty())
    {
        ReadSnapShotToInstall(snapshot);
    }

    std::thread t2(&KvServer::ReadRaftApplyCommandLoop, this); //马上向其他节点宣告自己就是leader
    t2.join();
}

void KvServer::ReadSnapShotToInstall(std::string snapshot)
{
    if(snapshot.empty())
    {
        return;
    }

    parseFromString(snapshot);
}

void KvServer::ReadRaftApplyCommandLoop()
{
    while(true)
    {
        auto message = applyChan->Pop();

        DPrint(
            "---------------tmp-------------[func-KvServer::ReadRaftApplyCommandLoop()-kvserver{%d}] 收到了下raft的消息",
            m_me);

        if(message.CommandValid)
        {
            GetCommandFromRaft(message);
        }
        if(message.SnapshotValid)
        {
            GetSnapShotFromRaft(message);
        }
    }
}

void KvServer::GetCommandFromRaft(ApplyMsg message)
{
    Op op;
    op.parseFromString(message.Command);

    DPrint(
        "[KvServer::GetCommandFromRaft-kvserver{%d}] , Got Command --> Index:{%d} , ClientId {%s}, RequestId {%d}, "
        "Opreation {%s}, Key :{%s}, Value :{%s}",
        m_me, message.CommandIndex, op.ClientId, op.RequestId, op.Operation, op.Key, op.Value);
    
    if(message.CommandIndex <= m_lastSnapShotRaftLogIndex)
    {
        return;
    }

    //判断请求是否是最新的
    if(!ifRequestDuplicate(op.ClientId, op.RequestId))
    {
        if(op.Operation == "Put")
        {
            ExecutePutOpOnKVDB(op);
        }
        if(op.Operation == "Append")
        {
            ExecuteAppendOpOnKVDB(op);
        }
    }
    //制作快照并持久化
    if(m_maxRaftState != -1)
    {
        IfNeedToSendSnapShotCommand(message.CommandIndex, 9);
    }

    SendMessageToWaitChan(op, message.CommandIndex);
}

bool KvServer::ifRequestDuplicate(std::string ClientId, int RequestId)
{
    if(m_lastRequestId.find(ClientId) == m_lastRequestId.end())
    {
        //这个客户都不存在，请求肯定没执行过
        return false;
    }
    return RequestId <= m_lastRequestId[ClientId];
}

void KvServer::ExecutePutOpOnKVDB(Op op)
{
    m_mtx.lock();
    m_skipList.insert_set_element(op.Key, op.Value);
    m_lastRequestId[op.ClientId] = op.RequestId;
    m_mtx.unlock();

    DprintfKVDB();
}

void KvServer::ExecuteAppendOpOnKVDB(Op op)
{
    m_mtx.lock();
    std::string value;
    m_skipList.insert_set_element(op.Key, op.Value);
    m_lastRequestId[op.ClientId] = op.RequestId;
    m_mtx.unlock();

    DprintfKVDB();
}

void KvServer::ExecuteGetOpOnKVDB(Op op, std::string *value, bool *exist)
{
    m_mtx.lock();

    *value = "";
    *exist = false;

    if(m_skipList.search_element(op.Key, *value))
    {
        *exist = true;
    }

    m_lastRequestId[op.ClientId] = op.RequestId;

    m_mtx.unlock();

    DprintfKVDB();
}

void KvServer::IfNeedToSendSnapShotCommand(int raftIndex, int proportion)
{
    if(m_raftNode->GetRaftStateSize() > m_maxRaftState / 10.0)
    {
        auto snapshot = MakeSnapShot();
        m_raftNode->Snapshot(raftIndex, snapshot);
    }
}

std::string KvServer::MakeSnapShot()
{
    std::lock_guard<std::mutex> lock(m_mtx);
    std::string snapshotData = getSnapshotData();
    return snapshotData;
}

bool KvServer::SendMessageToWaitChan(const Op &op, int raftIndex)
{
    std::lock_guard<std::mutex> lock(m_mtx);
    DPrint(
      "[RaftApplyMessageSendToWaitChan--> raftserver{%d}] , Send Command --> Index:{%d} , ClientId {%d}, RequestId "
      "{%d}, Opreation {%v}, Key :{%v}, Value :{%v}",
      m_me, raftIndex, op.ClientId, op.RequestId, op.Operation, op.Key, op.Value);
    if(waitApplyCh.find(raftIndex) == waitApplyCh.end())
    {
        return false;
    }

    waitApplyCh[raftIndex]->Push(op);
    DPrint(
      "[RaftApplyMessageSendToWaitChan--> raftserver{%d}] , Send Command --> Index:{%d} , ClientId {%d}, RequestId "
      "{%d}, Opreation {%v}, Key :{%v}, Value :{%v}",
      m_me, raftIndex, &op.ClientId, op.RequestId, &op.Operation, &op.Key, &op.Value);

    return true;
}

void KvServer::GetSnapShotFromRaft(ApplyMsg message)
{
    std::lock_guard<std::mutex> lock(m_mtx);

    if(m_raftNode->CondInstallSnapshot(message.SnapshotTerm, message.SnapshotIndex, message.Snapshot))
    {
        ReadSnapShotToInstall(message.Snapshot);
        m_lastSnapShotRaftLogIndex = message.SnapshotIndex;
    }
}

void KvServer::PutAppend(const raftKVRpcProctoc::PutAppendArgs *args, raftKVRpcProctoc::PutAppendReply *reply)
{
    Op op;
    op.Operation = args->op();
    op.Key = args->key();
    op.Value = args->value();
    op.ClientId = args->clientid();
    op.RequestId = args->requestid();
    int raftindex = -1;
    int _ = -1;
    bool isleader = false;

    m_raftNode->Start(op, &raftindex, &_, &isleader);

    if(!isleader)
    {
        DPrint(
            "[func -KvServer::PutAppend -kvserver{%d}]From Client %s (Request %d) To Server %d, key %s, raftIndex %d , but "
            "not leader",
            m_me, &args->clientid(), args->requestid(), m_me, &op.Key, raftindex);

        reply->set_err(ErrWrongLeader);
        return;
    }

    DPrint(
        "[func -KvServer::PutAppend -kvserver{%d}]From Client %s (Request %d) To Server %d, key %s, raftIndex %d , is "
        "leader ",
        m_me, &args->clientid(), args->requestid(), m_me, &op.Key, raftindex);

    m_mtx.lock();
    if (waitApplyCh.find(raftindex) == waitApplyCh.end())
    {
        waitApplyCh.insert(std::make_pair(raftindex, new LockQueue<Op>()));
    }

    auto chForRaftIndex = waitApplyCh[raftindex];
    m_mtx.unlock();

    Op raftCommitOp;

    if(!chForRaftIndex->timeOutPop(CONSENSUS_TIMEOUT, &raftCommitOp))
    {
        DPrint(
            "[func -KvServer::PutAppend -kvserver{%d}]TIMEOUT PUTAPPEND !!!! Server %d , get Command <-- Index:%d , "
            "ClientId %s, RequestId %s, Opreation %s Key :%s, Value :%s",
            m_me, m_me, raftindex, &op.ClientId, op.RequestId, &op.Operation, &op.Key, &op.Value);
    
        //超时，但是重复的请求，返回OK
        if(ifRequestDuplicate(op.ClientId, op.RequestId))
        {
            reply->set_err(OK);
        }
        else
        {
            reply->set_err(ErrWrongLeader); //让clerk重试
        }
    }
    else    //成功应用到状态机
    {
        if(raftCommitOp.ClientId == op.ClientId && raftCommitOp.RequestId == op.RequestId)
        {
            reply->set_err(OK);
        }
        else
        {
            reply->set_err(ErrWrongLeader);
        }
    }

    m_mtx.lock();
    auto tmp = waitApplyCh[raftindex];
    waitApplyCh.erase(raftindex);
    delete tmp;
    m_mtx.unlock();
}

void KvServer::Get(const raftKVRpcProctoc::GetArgs *args, raftKVRpcProctoc::GetReply *reply)
{
    Op op;
    op.Operation = "Get";
    op.Key = args->key();
    op.Value = "";
    op.ClientId = args->clientid();
    op.RequestId = args->requestid();

    int raftindex = -1;
    int _ = -1;
    bool isleader = false;

    m_raftNode->Start(op, &raftindex, &_, &isleader);

    // std::cout << op.Key << std::endl;
    // std::cout << op.ClientId << std::endl;
    // std::cout << op.RequestId << std::endl;

    if(!isleader)
    {
        reply->set_err(ErrWrongLeader);
        // std::cout << "-------------------------------------" << std::endl;
        // std::cout << "-------------------------------------" << std::endl;
        // std::cout << "-------------------------------------" << std::endl;
        // std::cout << "-------------------------------------" << std::endl;
        // std::cout << "-------------------------------------" << std::endl;
        // std::cout << "-------------------------------------" << std::endl;
        // std::cout << "-------------------------------------" << std::endl;
        return;
    }

    m_mtx.lock();
    if(waitApplyCh.find(raftindex) == waitApplyCh.end())
    {
        waitApplyCh.insert(std::make_pair(raftindex, new LockQueue<Op>()));
    }
    auto chForRaftIndex = waitApplyCh[raftindex];
    m_mtx.unlock();

    Op raftCommitOp;

    if(!chForRaftIndex->timeOutPop(CONSENSUS_TIMEOUT, &raftCommitOp))
    {
        int _ = -1;
        bool isleader = false;
        m_raftNode->GetState(&_, &isleader);

        if(ifRequestDuplicate(op.ClientId, op.RequestId) && isleader)
        {
            std::string value;
            bool exist = false;
            ExecuteGetOpOnKVDB(op, &value, &exist);
            if(exist)
            {
                reply->set_err(OK);
                reply->set_value(value);
            }
            else
            {
                reply->set_err(ErrNoKey);
                reply->set_value("");
            }
        }
        else
        {
            reply->set_err(ErrWrongLeader);
        }
    }
    else
    {
        if(raftCommitOp.ClientId == op.ClientId && raftCommitOp.RequestId == op.RequestId)
        {
            std::string value;
            bool exist = false;
            ExecuteGetOpOnKVDB(op, &value, &exist);
            if(exist)
            {
                reply->set_err(OK);
                reply->set_value(value);
            }
            else
            {
                reply->set_err(ErrNoKey);
                reply->set_value("");
            }
        }
        else
        {
            reply->set_err(ErrWrongLeader);
        }
    }

    m_mtx.lock();
    auto tmp = waitApplyCh[raftindex];
    waitApplyCh.erase(raftindex);
    delete tmp;
    m_mtx.unlock();
}

void KvServer::PutAppend(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::PutAppendArgs *request,
                 ::raftKVRpcProctoc::PutAppendReply *response, ::google::protobuf::Closure *done)
{
    KvServer::PutAppend(request, response);
    done->Run();
}

void KvServer::Get(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::GetArgs *request,
           ::raftKVRpcProctoc::GetReply *response, ::google::protobuf::Closure *done)
{
    KvServer::Get(request, response);
    done->Run();
}
