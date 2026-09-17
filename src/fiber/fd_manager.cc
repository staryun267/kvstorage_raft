#include "fd_manager.h"
#include "hook.h"

#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

template class Singleton<FdManager>;

//静态成员变量需要在类外定义(类内的是声明)
template <typename T>
T* Singleton<T>::instance = nullptr;

template <typename T>
std::mutex Singleton<T>::mutex;

FdCtx::FdCtx(int fd)
    : m_fd(fd)
{
    init();
}

FdCtx::~FdCtx()
{

}

bool FdCtx::init()
{
    if(m_isInit)
    {
        return true;
    }

    struct stat statbuf;
    if(-1 == fstat(m_fd, &statbuf)) //无效的文件描述符
    {
        m_isInit = false;
        m_isSocket = false;
    }
    else //有效
    {
        m_isInit = true;
        m_isSocket = S_ISSOCK(statbuf.st_mode);
    }

    //如果是socket套接字，设置非阻塞
    if(m_isSocket)
    {
        int flags = fcntl_f(m_fd, F_GETFL, 0);
        if(!(flags & O_NONBLOCK))
        {
            fcntl_f(m_fd, F_SETFL, flags | O_NONBLOCK);
        }
        m_sysNonblock = true;
    }
    else
    {
        m_sysNonblock = false;
    }

    return m_isInit;
}

void FdCtx::setTimeout(int type, uint64_t v)
{
    if(type == SO_RCVTIMEO)
    {
        m_recvtimeout = v;
    }
    else
    {
        m_sendtimeout = v;
    }
}

uint64_t FdCtx::getTimeout(int type)
{
    if(type == SO_RCVTIMEO)
    {
        return m_recvtimeout;
    }
    else
    {
        return m_sendtimeout;
    }
}

FdManager::FdManager()
{
    m_datas.resize(64);
}

FdCtx::ptr FdManager::get(int fd, bool auto_create)
{
    if(fd == -1)
    {
        return nullptr;
    }

    std::shared_lock<std::shared_mutex> read_lock(m_mutex);
    if(m_datas.size() <= fd)
    {
        if(auto_create == false)
        {
            return nullptr;
        }
    }
    else
    {
        if(m_datas[fd] || !auto_create)
        {
            return m_datas[fd];
        }
    }
    read_lock.unlock();

    std::unique_lock<std::shared_mutex> write_lock(m_mutex);
    if(m_datas.size() <= fd)
    {
        m_datas.resize(fd * 1.5);
    }

    m_datas[fd] = std::make_shared<FdCtx>(fd);
    return m_datas[fd];
}

void FdManager::del(int fd)
{
    std::unique_lock<std::shared_mutex> write_lock(m_mutex);
    if(m_datas.size() <= fd)
    {
        return;
    }

    m_datas[fd].reset();
}