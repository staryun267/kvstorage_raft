#pragma once

#include <sys/socket.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/uio.h>
#include <sys/ioctl.h>

bool is_hook_enable();
void set_hook_enable(bool flag);

extern "C"
{
    //trace the original version
    using sleep_fun = unsigned int (*)(unsigned int seconds);
    extern sleep_fun sleep_f;

    using usleep_fun = int (*)(useconds_t usec);
    extern usleep_fun usleep_f;

    using nanosleep_fun = int (*)(const struct timespec* req, struct timespec* rem);
    extern nanosleep_fun nanosleep_f;

    using socket_fun = int (*)(int domain, int type, int protocol);
    extern socket_fun socket_f;

    using connect_fun = int (*)(int sockfd, const struct sockaddr* addr, socklen_t addrlen);
    extern connect_fun connect_f;

    using accept_fun = int (*)(int sockfd, struct sockaddr* addr, socklen_t* addrlen);
    extern accept_fun accept_f;

    using read_fun = ssize_t (*)(int fd, void* buf, size_t count);
    extern read_fun read_f;

    using readv_fun = ssize_t (*)(int fd, const struct iovec* iov, int iovcnt);
    extern readv_fun readv_f;

    using recv_fun = ssize_t (*)(int fd, void* buf, size_t len, int flags);
    extern recv_fun recv_f;

    using recvfrom_fun = ssize_t (*)(int fd, void* buf, size_t len, int flags, struct sockaddr* src_addr, socklen_t* addrlen);
    extern recvfrom_fun recvfrom_f;

    using recvmsg_fun = ssize_t (*)(int fd, struct msghdr* msg, int flags);
    extern recvmsg_fun recvmsg_f;

    using write_fun = ssize_t (*)(int fd, const void* buf, size_t count);
    extern write_fun write_f;

    using writev_fun = ssize_t (*)(int fd, const struct iovec* iov, int iovcnt);
    extern writev_fun writev_f;

    using send_fun = ssize_t (*)(int fd, const void* buf, size_t len, int flags);
    extern send_fun send_f;

    using sendto_fun = ssize_t (*)(int fd, const void* buf, size_t len, int flags, const struct sockaddr* dest_addr, socklen_t addrlen);
    extern sendto_fun sendto_f;

    using sendmsg_fun = ssize_t (*)(int fd, const struct msghdr* msg, int flags);
    extern sendmsg_fun sendmsg_f;

    using close_fun = int (*)(int fd);
    extern close_fun close_f;

    using fcntl_fun = int (*)(int fd, int cmd, ... /* args */);
    extern fcntl_fun fcntl_f;

    using ioctl_fun = int (*)(int fd, unsigned long request, ...);
    extern ioctl_fun ioctl_f;

    using getsockopt_fun = int (*)(int fd, int level, int optname, void* optval, socklen_t* optlen);
    extern getsockopt_fun getsockopt_f;

    using setsockopt_fun = int (*)(int fd, int level, int optname, const void* optval, socklen_t optlen);
    extern setsockopt_fun setsockopt_f;
    
    //sleep function
    unsigned int sleep(unsigned int seconds);
    int usleep(useconds_t usec);
    int nanosleep(const struct timespec* req, struct timespec* rem);

    //socket function
    int socket(int domain, int type, int protocol);
    int connect(int sockfd, const struct sockaddr* addr, socklen_t addrlen);
    int accept(int sockfd, struct sockaddr* addr, socklen_t* addrlen);

    //read
    ssize_t read(int fd, void* buf, size_t count);
    ssize_t readv(int fd, const struct iovec* iov, int iovcnt);

    ssize_t recv(int fd, void* buf, size_t len, int flags);
    ssize_t recvfrom(int fd, void* buf, size_t len, int flags, struct sockaddr* src_addr, socklen_t* addrlen);
    ssize_t recvmsg(int fd, struct msghdr* msg, int flags);

    //write
    ssize_t write(int fd, const void* buf, size_t count);
    ssize_t writev(int fd, const struct iovec* iov, int iovcnt);

    ssize_t send(int fd, const void* buf, size_t len, int flags);
    ssize_t sendto(int fd, const void* buf, size_t len, int flags, const struct sockaddr* dest_addr, socklen_t addrlen);
    ssize_t sendmsg(int fd, const struct msghdr* msg, int flags);

    //fd
    int close(int fd);

    //socket control
    int fcntl(int fd, int cmd, ... /* args */);
    int ioctl(int fd, unsigned long request, ...);

    int getsockopt(int fd, int level, int optname, void* optval, socklen_t* optlen);
    int setsockopt(int fd, int level, int optname, const void* optval, socklen_t optlen);
}