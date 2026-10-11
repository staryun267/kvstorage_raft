#include "mprpcchannel.h"
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <string>
#include "mprpccontroller.h"
#include "rpcheader.pb.h"
#include "util.h"

namespace {
constexpr uint32_t kMaxRpcFrameSize = 64 * 1024 * 1024;

bool sendAll(int fd, const void* data, size_t size) {
  const char* current = static_cast<const char*>(data);
  while (size > 0) {
    const ssize_t written = send(fd, current, size, 0);
    if (written > 0) { current += written; size -= written; continue; }
    if (written == -1 && errno == EINTR) { continue; }
    return false;
  }
  return true;
}

bool recvAll(int fd, void* data, size_t size) {
  char* current = static_cast<char*>(data);
  while (size > 0) {
    const ssize_t received = recv(fd, current, size, 0);
    if (received > 0) { current += received; size -= received; continue; }
    if (received == -1 && errno == EINTR) { continue; }
    return false;
  }
  return true;
}
}  // namespace

void MprpcChannel::CallMethod(const google::protobuf::MethodDescriptor* method,
                              google::protobuf::RpcController* controller, const google::protobuf::Message* request,
                              google::protobuf::Message* response, google::protobuf::Closure* done) {
  std::lock_guard<std::mutex> lock(m_mutex);
  if (m_clientFd == -1) {
    std::string errMsg;
    bool rt = newConnect(m_ip.c_str(), m_port, &errMsg);
    if (!rt) {
      DPrint("[func-MprpcChannel::CallMethod]重连接ip：{%s} port{%d}失败", m_ip.c_str(), m_port);
      controller->SetFailed(errMsg);
      return;
    } else {
      DPrint("[func-MprpcChannel::CallMethod]连接ip：{%s} port{%d}成功", m_ip.c_str(), m_port);
    }
  }

  const google::protobuf::ServiceDescriptor* sd = method->service();
  std::string service_name = sd->name();     // service_name
  std::string method_name = method->name();  // method_name

  // 获取参数的序列化字符串长度 args_size
  uint32_t args_size{};
  std::string args_str;
  if (request->SerializeToString(&args_str)) {
    args_size = args_str.size();
  } else {
    controller->SetFailed("serialize request error!");
    return;
  }

  //std::cout << "debug" << request->DebugString() << std::endl;

  //std::cout << "args_str" << args_str << std::endl;

  RPC::RpcHeader rpcHeader;
  rpcHeader.set_service_name(service_name);
  rpcHeader.set_method_name(method_name);
  rpcHeader.set_args_size(args_size);

  std::string rpc_header_str;
  if (!rpcHeader.SerializeToString(&rpc_header_str)) {
    controller->SetFailed("serialize rpc header error!");
    return;
  }

  // 使用protobuf的CodedOutputStream来构建发送的数据流
  std::string send_rpc_str;  // 用来存储最终发送的数据
  {
    // 创建一个StringOutputStream用于写入send_rpc_str
    google::protobuf::io::StringOutputStream string_output(&send_rpc_str);
    google::protobuf::io::CodedOutputStream coded_output(&string_output);

    // 先写入header的长度（变长编码）
    coded_output.WriteVarint32(static_cast<uint32_t>(rpc_header_str.size()));

    // 不需要手动写入header_size，因为上面的WriteVarint32已经包含了header的长度信息
    // 然后写入rpc_header本身
    coded_output.WriteString(rpc_header_str);
  }

  // 最后，将请求参数附加到send_rpc_str后面
  send_rpc_str += args_str;

  // 打印调试信息
  // std::cout << "============================================" << std::endl;
  // std::cout << "rpc_header_str: " << rpc_header_str << std::endl;
  // std::cout << "service_name: " << service_name << std::endl;
  // std::cout << "method_name: " << method_name << std::endl;
  // std::cout << "args_str: " << args_str << std::endl;
  // std::cout << "============================================" << std::endl;

  const uint32_t network_size = htonl(static_cast<uint32_t>(send_rpc_str.size()));
  std::string frame(reinterpret_cast<const char*>(&network_size), sizeof(network_size));
  frame += send_rpc_str;

  if (!sendAll(m_clientFd, frame.data(), frame.size())) {
    close(m_clientFd);
    m_clientFd = -1;
    std::string errMsg;
    if (!newConnect(m_ip.c_str(), m_port, &errMsg) || !sendAll(m_clientFd, frame.data(), frame.size())) {
      controller->SetFailed(errMsg);
      return;
    }
  }

  uint32_t network_response_size = 0;
  if (!recvAll(m_clientFd, &network_response_size, sizeof(network_response_size))) {
    close(m_clientFd);
    m_clientFd = -1;
    controller->SetFailed("recv rpc response size error");
    return;
  }
  const uint32_t response_size = ntohl(network_response_size);
  if (response_size > kMaxRpcFrameSize) {
    controller->SetFailed("rpc response exceeds size limit");
    return;
  }
  std::string response_str(response_size, '\0');
  if (!recvAll(m_clientFd, response_str.data(), response_str.size())) {
    close(m_clientFd);
    m_clientFd = -1;
    controller->SetFailed("recv rpc response body error");
    return;
  }

  if (!response->ParseFromString(response_str)) {
    controller->SetFailed("parse rpc response error");
    return;
  }
}

bool MprpcChannel::newConnect(const char* ip, uint16_t port, string* errMsg) {
  int clientfd = socket(AF_INET, SOCK_STREAM, 0);
  if (-1 == clientfd) {
    char errtxt[512] = {0};
    sprintf(errtxt, "create socket error! errno:%d", errno);
    m_clientFd = -1;
    *errMsg = errtxt;
    return false;
  }

  struct sockaddr_in server_addr;
  server_addr.sin_family = AF_INET;
  server_addr.sin_port = htons(port);
  server_addr.sin_addr.s_addr = inet_addr(ip);
  // 连接rpc服务节点
  if (-1 == connect(clientfd, (struct sockaddr*)&server_addr, sizeof(server_addr))) {
    close(clientfd);
    char errtxt[512] = {0};
    sprintf(errtxt, "connect fail! errno:%d", errno);
    m_clientFd = -1;
    *errMsg = errtxt;
    return false;
  }
  m_clientFd = clientfd;
  return true;
}

MprpcChannel::MprpcChannel(string ip, short port, bool connectNow) : m_ip(ip), m_port(port), m_clientFd(-1) {

  if (!connectNow) {
    return;
  }  //可以允许延迟连接
  std::string errMsg;
  auto rt = newConnect(ip.c_str(), port, &errMsg);
  int tryCount = 3;
  while (!rt && tryCount--) {
    //std::cout << errMsg << std::endl;
    rt = newConnect(ip.c_str(), port, &errMsg);
  }
}
