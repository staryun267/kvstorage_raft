#pragma once

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <cerrno>
#include <string>
#include <vector>

#include "raftServerRpcUtil.h"
#include "kvServerRpc.pb.h"
#include "mprpcconfig.h"

class Clerk
{
public:
    Clerk();
public:
    //对外暴露的接口
    void Init(std::string configFileName);

    std::string Get(std::string key);

    void Put(std::string key, std::string value);
    void Append(std::string key, std::string value);
private:
    //用于返回随机的clientid
    std::string Uuid()
    {
        return std::to_string(rand()) + std::to_string(rand()) + std::to_string(rand()) + std::to_string(rand());
    }
    
    void PutAppend(std::string key, std::string value, std::string op);

    std::vector<std::shared_ptr<raftServerRpcUtil>> m_servers;  //保存所有raft节点的fd //todo：全部初始化为-1，表示没有连接上
    std::string m_clientId;
    int m_requestId;
    int m_recentLeaderId;  //只是有可能是领导
};