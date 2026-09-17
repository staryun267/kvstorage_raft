#include "hook.h"
#include "iomanager.h"
#include "fd_manager.h"

#include <dlfcn.h>
#include <string.h>
#include <cstdarg>
#include <iostream>

//apply XX to all function
#define HOOK_FUN(XX) \
    XX(sleep) \
    XX(usleep) \
    XX(nanosleep) \
    XX(socket) \
    XX(connect) \
    XX(accept) \
    XX(read) \
    XX(readv) \
    XX(recv) \
    XX(recvfrom) \
    XX(recvmsg) \
    XX(write) \
    XX(writev) \
    XX(send) \
    XX(sendto) \
    XX(sendmsg) \
    XX(close) \
    XX(fcntl) \
    XX(ioctl) \
    XX(getsockopt) \
    XX(setsockopt)

//if this thread is using hook function
static thread_local bool t_hook_enable = false;

bool is_hook_enable()
{
    return t_hook_enable;
}

void set_hook_enable(bool flag)
{
    t_hook_enable = flag;
}

void hook_init()
{
    static bool is_inited = false;

    if(is_inited)
    {
        return;
    }

    is_inited = true;

#define XX(name) name ## _f = (name ## _fun)dlsym(RTLD_NEXT, #name);
    HOOK_FUN(XX)
#undef XX
}

//静态变量的构造函数会在main之前执行
struct HookIniter
{
    HookIniter()
    {
        hook_init();
    }
};

static HookIniter s_hook_initer;

struct timer_info
{
    int cancelled = 0;
};

//IO函数调用的模板
template <typename OriginFun, typename... Args>
static ssize_t do_io(int fd, OriginFun fun, const char* hook_fun_name, uint32_t event, int timeout_so, Args&&... args)
{
    if(!t_hook_enable)
    {
        return fun(fd, std::forward<Args>(args)...);
    }

    FdCtx::ptr ctx = FdMgr::GetInstance()->get(fd);

    if(!ctx)
    {
        return fun(fd, std::forward<Args>(args)...);
    }

    if(ctx->isClosed())
    {
        errno = EBADF;
        return -1;
    }

    if(!ctx->isSocket() || ctx->getUserNonblock())
    {
        return fun(fd, std::forward<Args>(args)...);
    }

    //get the timeout
    uint64_t timeout = ctx->getTimeout(timeout_so);
    //timer condition
    std::shared_ptr<timer_info> tinfo(new timer_info);

retry:
    //run the function
    ssize_t n = fun(fd, std::forward<Args>(args)...);

    //被信号打断
    while(n == -1 && errno == EINTR)
    {
        n = fun(fd, std::forward<Args>(args)...);
    }

    //无数据可读/无地方可写
    if(n == -1 && errno == EAGAIN)
    {
        IOManager* iom = IOManager::GetThis();

        //timer
        Timer::ptr timer;
        std::weak_ptr<timer_info> winfo(tinfo);

        //1.超时时间已经设置，增加一个条件计时器用于取消这个操作
        if(timeout != (uint64_t) - 1)
        {
            timer = iom->addConditionTimer(timeout, [winfo, fd, iom, event](){
                auto t = winfo.lock();
                if(!t || t->cancelled)
                {
                    return;
                }
                t->cancelled = ETIMEDOUT;
                //cancel this event and trigger once to return to this fiber
                iom->cancelEvent(fd, (IOManager::Event)event);
            }, winfo);
        }

        //2. add Event
        int rt = iom->addEvent(fd, (IOManager::Event)event);
        if(rt)
        {
            std::cout << "hook: addEvent error" << std::endl;
            if(timer)
            {
                timer->cancel();
            }
            return -1;
        }
        else
        {
            Fiber::GetThis()->yield();

            if(timer)
            {
                timer->cancel();
            }
            if(tinfo->cancelled == ETIMEDOUT)
            {
                errno = tinfo->cancelled;
                return -1;
            }
            goto retry;
        }
    }
    return n;
}

extern "C"
{
#define XX(name) name ## _fun name ## _f = nullptr;
    HOOK_FUN(XX)
#undef XX

    //sleep function
    //让协程挂起指定的时间
    unsigned int sleep(unsigned int seconds)    //only use at task fiber
    {
        if(!t_hook_enable)
        {
            return sleep_f(seconds);
        }

        Fiber::ptr fiber = Fiber::GetThis();
        IOManager* iom = IOManager::GetThis();

        iom->addTimer(seconds*1000, [iom, fiber](){ iom->scheduleLock(fiber, -1); });
        fiber->yield();
        return 0;
    }

    int usleep(useconds_t usec)
    {
        if(!t_hook_enable)
        {
            return usleep_f(usec);
        }

        Fiber::ptr fiber = Fiber::GetThis();
        IOManager* iom = IOManager::GetThis();

        iom->addTimer(usec/1000, [iom, fiber](){ iom->scheduleLock(fiber); });
        fiber->yield();
        return 0;
    }

    int nanosleep(const struct timespec* req, struct timespec* rem)
    {
        if(!t_hook_enable)
        {
            return nanosleep_f(req, rem);
        }

        int timeout_ms = req->tv_sec*1000 + rem->tv_nsec/1000/1000;

        Fiber::ptr fiber = Fiber::GetThis();
        IOManager* iom = IOManager::GetThis();

        iom->addTimer(timeout_ms, [iom, fiber](){ iom->scheduleLock(fiber, -1); });
        fiber->yield();
        return 0;
    }

    //socket function
    int socket(int domain, int type, int protocol)
    {
        if(!t_hook_enable)
        {
            return socket_f(domain, type, protocol);
        }

        int fd = socket_f(domain, type, protocol);
        if(fd == -1)
        {
            std::cout << "socket() error errno: " << strerror(errno) << std::endl;
            return fd;
        }
        FdMgr::GetInstance()->get(fd,true);
        return fd;
    }

    int connect_with_timeout(int fd, const struct sockaddr* addr, socklen_t addrlen, uint64_t timeout_ms)
    {
        if(!t_hook_enable)
        {
            return connect_f(fd, addr, addrlen);
        }

        FdCtx::ptr ctx = FdMgr::GetInstance()->get(fd);
        if(!ctx || ctx->isClosed())
        {
            errno = EBADF;
            return -1;
        }

        if(!ctx->isSocket())
        {
            return connect_f(fd, addr, addrlen);
        }

        if(ctx->getUserNonblock())
        {
            return connect_f(fd, addr, addrlen);
        }

        //attemp to connect
        int n = connect_f(fd, addr, addrlen);
        if(n == 0)
        {
            return 0;
        }
        else if(n != -1 || errno != EINPROGRESS)
        {
            return n;
        }

        // wait for write event is ready -> connect succeeds
        IOManager *iom = IOManager::GetThis();
        Timer::ptr timer;
        std::shared_ptr<timer_info> tinfo(new timer_info);
        std::weak_ptr<timer_info> winfo(tinfo);

        if (timeout_ms != (uint64_t)-1)
        {
            timer = iom->addConditionTimer(timeout_ms, [winfo, fd, iom]()
                                           {
            auto t = winfo.lock();
            if(!t || t->cancelled) 
            {
                return;
            }
            t->cancelled = ETIMEDOUT;
            iom->cancelEvent(fd, IOManager::WRITE); }, winfo);
        }

        int rt = iom->addEvent(fd, IOManager::WRITE);
        if(rt == 0)
        {
            Fiber::GetThis()->yield();
            
            if(timer)
            {
                timer->cancel();
            }

            if(tinfo->cancelled)
            {
                errno = tinfo->cancelled;
                return -1;
            }
        }
        else
        {
            if(timer)
            {
                timer->cancel();
            }
            std::cout << "connect addEvent(" << fd << "WRITE) error!" << std::endl;
        }

        //chect out if this connection socket established
        int error = 0;
        socklen_t len = sizeof(int);
        if(-1 == getsockopt(fd, SOL_SOCKET, SO_ERROR, &error, &len))
        {
            return -1;
        }

        if(!error)
        {
            return 0;
        }
        else
        {
            errno = error;
            return -1;
        }
    }

    static uint64_t s_connect_timeout = -1;
    int connect(int sockfd, const struct sockaddr* addr, socklen_t addrlen)
    {
        return connect_with_timeout(sockfd, addr, addrlen, s_connect_timeout);
    }

    int accept(int sockfd, struct sockaddr* addr, socklen_t* addrlen)
    {
        int fd = do_io(sockfd, accept_f, "accept", IOManager::READ, SO_RCVTIMEO, addr, addrlen);
        if(fd >= 0)
        {
            FdMgr::GetInstance()->get(fd, true);
        }
        return fd;
    }

    //read
    ssize_t read(int fd, void* buf, size_t count)
    {
        return do_io(fd, read_f, "read", IOManager::READ, SO_RCVTIMEO, buf, count);
    }

    ssize_t readv(int fd, const struct iovec* iov, int iovcnt)
    {
        return do_io(fd, readv_f, "readv", IOManager::READ, SO_RCVTIMEO, iov, iovcnt);
    }

    ssize_t recv(int fd, void* buf, size_t len, int flags)
    {
        return do_io(fd, recv_f, "recv", IOManager::READ, SO_RCVTIMEO, buf, len, flags);
    }

    ssize_t recvfrom(int fd, void* buf, size_t len, int flags, struct sockaddr* src_addr, socklen_t* addrlen)
    {
        return do_io(fd, recvfrom_f, "recvfrom", IOManager::READ, SO_RCVTIMEO, buf, len, flags, src_addr, addrlen);
    }

    ssize_t recvmsg(int fd, struct msghdr* msg, int flags)
    {
        return do_io(fd, recvmsg_f, "recvmsg", IOManager::READ, SO_RCVTIMEO, msg, flags);
    }

    //write
    ssize_t write(int fd, const void* buf, size_t count)
    {
        return do_io(fd, write_f, "write", IOManager::WRITE, SO_SNDTIMEO, buf, count);
    }

    ssize_t writev(int fd, const struct iovec* iov, int iovcnt)
    {
        return do_io(fd, writev_f, "writev", IOManager::WRITE, SO_SNDTIMEO, iov, iovcnt);
    }

    ssize_t send(int fd, const void* buf, size_t len, int flags)
    {
        return do_io(fd, send_f, "send", IOManager::WRITE, SO_SNDTIMEO, buf, len, flags);
    }

    ssize_t sendto(int fd, const void* buf, size_t len, int flags, const struct sockaddr* dest_addr, socklen_t addrlen)
    {
        return do_io(fd, sendto_f, "sendto", IOManager::WRITE, SO_SNDTIMEO, buf, len, flags, dest_addr, addrlen);
    }

    ssize_t sendmsg(int fd, const struct msghdr* msg, int flags)
    {
        return do_io(fd, sendmsg_f, "sendmsg", IOManager::WRITE, SO_SNDTIMEO, msg, flags);
    }

    //fd
    int close(int fd)
    {
        if(!t_hook_enable)
        {
            return close_f(fd);
        }

        FdCtx::ptr ctx = FdMgr::GetInstance()->get(fd);

        if(ctx)
        {
            auto iom = IOManager::GetThis();
            if(iom)
            {
                iom->cancelAll(fd);
            }
            FdMgr::GetInstance()->del(fd);
        }

        return close_f(fd);
    }

    //socket control
    int fcntl(int fd, int cmd, ... /* args */)
    {
        va_list va;

        va_start(va, cmd);
        switch(cmd)
        {
            case F_SETFL:
                {
                    int arg = va_arg(va, int);
                    va_end(va);
                    FdCtx::ptr ctx = FdMgr::GetInstance()->get(fd);
                    if(!ctx || ctx->isClosed() || !ctx->isSocket())
                    {
                        return fcntl_f(fd, cmd, arg);
                    }
                    //用户是否设置了非阻塞
                    ctx->setUserNonblock(arg & O_NONBLOCK);
                    //最后是否阻塞根据系统设置决定
                    if(ctx->getSysNonblock())
                    {
                        arg |= O_NONBLOCK;
                    }
                    else
                    {
                        arg &= ~O_NONBLOCK;
                    }
                    return fcntl_f(fd, cmd, arg);
                }
                break;
            case F_GETFL:
                {
                    va_end(va);
                    int arg = fcntl_f(fd, cmd);
                    FdCtx::ptr ctx = FdMgr::GetInstance()->get(fd);
                    if(!ctx || ctx->isClosed() || !ctx->isSocket())
                    {
                        return arg;
                    }
                    //这里是呈现给用户的 显示用户设置的值
                    //但是底层还是根据系统设置决定的
                    if(ctx->getUserNonblock())
                    {
                        return arg | O_NONBLOCK;
                    }
                    else
                    {
                        return arg & ~O_NONBLOCK;
                    }
                }
                break;
            case F_DUPFD:
            case F_DUPFD_CLOEXEC:
            case F_SETFD:
            case F_SETOWN:
            case F_SETSIG:
            case F_SETLEASE:
            case F_NOTIFY:
#ifdef F_SETPIPE_SZ
            case F_SETPIPE_SZ:
#endif
                {
                    int arg = va_arg(va, int);
                    va_end(va);
                    return fcntl_f(fd, cmd, arg);
                }
                break;
            case F_GETFD:
            case F_GETOWN:
            case F_GETSIG:
            case F_GETLEASE:
#ifdef F_GETPIPE_SZ
            case F_GETPIPE_SZ:
#endif
                {
                    va_end(va);
                    return fcntl_f(fd, cmd);
                }
                break;
            case F_SETLK:
            case F_SETLKW:
            case F_GETLK:
                {
                    struct flock* arg = va_arg(va, struct flock*);
                    va_end(va);
                    return fcntl_f(fd, cmd, arg);
                }
                break;
            case F_GETOWN_EX:
            case F_SETOWN_EX:
                {
                    struct f_owner_exlock* arg = va_arg(va, struct f_owner_exlock*);
                    va_end(va);
                    return fcntl_f(fd, cmd, arg);
                }
                break;
            default:
                va_end(va);
                return fcntl_f(fd, cmd);
        }
    }

    int ioctl(int fd, unsigned long request, ...)
    {
        va_list va;
        va_start(va, request);
        void* arg = va_arg(va, void*);
        va_end(va);

        if(FIONBIO == request)
        {
            bool user_nonblock = !!*(int*)arg;
            FdCtx::ptr ctx = FdMgr::GetInstance()->get(fd);
            if(!ctx || ctx->isClosed() || !ctx->isSocket())
            {
                return ioctl_f(fd, request, arg);
            }
            ctx->setUserNonblock(user_nonblock);
        }
        return ioctl_f(fd, request, arg);
    }

    int getsockopt(int fd, int level, int optname, void* optval, socklen_t* optlen)
    {
        return getsockopt_f(fd, level, optname, optval, optlen);
    }

    int setsockopt(int fd, int level, int optname, const void* optval, socklen_t optlen)
    {
        if(!t_hook_enable)
        {
            return setsockopt_f(fd, level, optname, optval, optlen);
        }

        if(level == SOL_SOCKET)
        {
            if(optname == SO_RCVTIMEO || optname == SO_SNDTIMEO)
            {
                FdCtx::ptr ctx = FdMgr::GetInstance()->get(fd);
                if(ctx)
                {
                    const timeval* v = (const timeval*)optval;
                    ctx->setTimeout(optname, v->tv_sec * 1000 + v->tv_usec / 1000);
                }
            }
        }
        return setsockopt_f(fd, level, optname, optval, optlen);
    }
}
