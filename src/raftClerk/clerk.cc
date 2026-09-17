#include "clerk.h"
#include "raftServerRpcUtil.h"
#include "util.h"

#include <string>
#include <vector>

Clerk::Clerk() : m_clientId(Uuid()), m_requestId(0), m_recentLeaderId(0) {}

// 对外暴露的接口
//初始化客户端
void Clerk::Init(std::string configFileName)
{
    //获取所有raft节点的ip，port进行连接
    MprpcConfig config;
    config.LoadConfigFile(configFileName.c_str());
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

    //进行连接
    for(const auto& item : ipPortVt)
    {
        std::string ip = item.first;
        short port = item.second;

        auto *rpc = new raftServerRpcUtil(ip, port);
        m_servers.push_back(std::shared_ptr<raftServerRpcUtil>(rpc));
    }
}

std::string Clerk::Get(std::string key)
{
    m_requestId++;
    int32_t requestId = m_requestId;
    int server = m_recentLeaderId;
    raftKVRpcProctoc::GetArgs args;
    args.set_key(key);
    args.set_clientid(m_clientId);
    args.set_requestid(requestId);

    //std::cout << "GetArgs::kRequestIdFieldNumber = " << raftKVRpcProctoc::GetArgs::kRequestIdFieldNumber << std::endl;

    //std::cout << args.clientid() << std::endl;
    //std::cout << args.requestid() << std::endl;

    while(true)
    {
        raftKVRpcProctoc::GetReply reply;
        bool ok = m_servers[server]->Get(&args, &reply);
        if(!ok || reply.err() == ErrWrongLeader)
        {
            //std::cout << ok << reply.err() << std::endl;
            //std::cout << args.requestid() << std::endl;
            //std::cout << args.clientid() << std::endl;
            server = (server + 1) % m_servers.size();
            continue;
        }
        if(reply.err() == ErrNoKey)
        {
            return "";
        }
        if(reply.err() == OK)
        {
            m_recentLeaderId = server;
            return reply.value();
        }
    }
    return "";
}

void Clerk::Put(std::string key, std::string value)
{
    PutAppend(key, value, "Put");
}

void Clerk::Append(std::string key, std::string value)
{
    PutAppend(key, value, "Append");
}

void Clerk::PutAppend(std::string key, std::string value, std::string op)
{
    m_requestId++;
    int32_t requestId = m_requestId;
    auto server = m_recentLeaderId;
    raftKVRpcProctoc::PutAppendArgs args;
    args.set_key(key);
    args.set_value(value);
    args.set_op(op);
    args.set_clientid(m_clientId);
    args.set_requestid(requestId);
    
    // std::cout << requestId << std::endl;
    // std::cout << args.requestid() << std::endl;
    while(true)
    {
        raftKVRpcProctoc::PutAppendReply reply;
        bool ok = m_servers[server]->PutAppend(&args, &reply);
        if(!ok || reply.err() == ErrWrongLeader)
        {
            DPrint("【Clerk::PutAppend】原以为的leader：{%d}请求失败，向新leader{%d}重试  ，操作：{%s}", server, server + 1,
                   op.c_str());
            if(!ok)
            {
                DPrint("重试原因 ，rpc失敗 ，");
            }
            if(reply.err() == ErrWrongLeader)
            {
                DPrint("重試原因：非leader");
            }

            server = (server + 1) % m_servers.size();   //尝试下一个raft节点
            continue;
        }

        if(reply.err() == OK)
        {
            m_recentLeaderId = server;
            return;
        }
    }
}