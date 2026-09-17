#pragma once

#include <memory>
#include <shared_mutex>

#include "thread.h"

class FdCtx : public std::enable_shared_from_this<FdCtx>
{
private:
    bool m_isInit = false;
	bool m_isSocket = false;
	bool m_sysNonblock = false;
	bool m_userNonblock = false;
	bool m_isClosed = false;
	int m_fd;

    //读写事件超时时间
    uint64_t m_recvtimeout = (uint64_t) - 1;
    uint64_t m_sendtimeout = (uint64_t) - 1;

public:
    using ptr = std::shared_ptr<FdCtx>;

    FdCtx(int fd);
    ~FdCtx();

    bool init();
    bool isInit() const { return m_isInit; }
    bool isSocket() const { return m_isSocket; }
    bool isClosed() const { return m_isClosed; }

    void setUserNonblock(bool v) { m_userNonblock = v; }
    bool getUserNonblock() const { return m_userNonblock; }

    void setSysNonblock(bool v) { m_sysNonblock = v; }
    bool getSysNonblock() const { return m_sysNonblock; }

    void setTimeout(int type, uint64_t v);
    uint64_t getTimeout(int type);
};

class FdManager
{
public:
    FdManager();

    FdCtx::ptr get(int fd, bool auto_create = false);
    void del(int fd);

private:
    std::shared_mutex m_mutex;
    std::vector<FdCtx::ptr> m_datas;
};

template <typename T>
class Singleton
{
private:
    static T* instance;
    static std::mutex mutex;

protected:
    Singleton() {}

public:
    //禁止拷贝构造和赋值操作
    Singleton(const Singleton&) = delete;
    Singleton& operator=(const Singleton&) = delete;

    static T* GetInstance()
    {
        std::lock_guard<std::mutex> lock(mutex);
        if(instance == nullptr)
        {
            instance = new T();
        }
        return instance;
    }

    static void DestroyInstance()
    {
        std::lock_guard<std::mutex> lock(mutex);
        if(instance == nullptr)
        {
            return;
        }
        delete instance;
        instance = nullptr;
    }
};

using FdMgr = Singleton<FdManager>;