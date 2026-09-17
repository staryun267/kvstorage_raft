#pragma once

#include <iostream>

#include "kvServerRpc.pb.h"
#include "mprpcchannel.h"
#include "mprpccontroller.h"
#include "rpcprovider.h"

class raftServerRpcUtil
{
public:
    bool Get(raftKVRpcProctoc::GetArgs* GetArgs, raftKVRpcProctoc::GetReply* reply);

    bool PutAppend(raftKVRpcProctoc::PutAppendArgs* args, raftKVRpcProctoc::PutAppendReply* reply);
    
    raftServerRpcUtil(std::string ip, short port);

    ~raftServerRpcUtil();
private:
    raftKVRpcProctoc::kvServerRpc_Stub *stub;
};