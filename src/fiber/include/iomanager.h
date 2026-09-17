#pragma once

#include "scheduler.h"
#include "fiber.h"
#include "timer.h"

#include <functional>
#include <mutex>
#include <string>
#include <atomic>
#include <shared_mutex>
#include <vector>

//工作流程
//注册事件到epoll -> 等待事件触发 -> 将触发的事件(协程/回调)放入调度器的工作队列 -> 从epoll上删除事件 -> 调度器的run方法执行这个事件
class IOManager : public Scheduler, public TimerManager
{
public:
    enum Event
    {
        NONE = 0x0,
        //EPOLLIN
        READ = 0x1,
        //EPOLLOUT
        WRITE = 0x4
    };
private:
    struct FdContext
    {
        struct EventContext
        {
            Scheduler* scheduler = nullptr;
            Fiber::ptr fiber;
            std::function<void()> cb;
        };

        EventContext read;
        EventContext write;

        int fd = 0;

        Event events = NONE;
        std::mutex mutex;

        EventContext& getEventContext(Event event);
        void resetEventContext(EventContext& ctx);
        void triggerEvent(Event event);
    };

public:
    IOManager(size_t threads = 1,bool use_caller = true,const std::string& name = "IOManager");
    ~IOManager();

    //add one event at a time
    int addEvent(int fd, Event event, std::function<void()> cb = nullptr);
    //delete event
    bool delEvent(int fd, Event event);
    //delete this event and trigger its callback
    bool cancelEvent(int fd, Event event);
    //delete all events and trigger their callback
    bool cancelAll(int fd);

    static IOManager* GetThis();

protected:
    void tickle() override;
    
    bool stopping() override;

    void idle() override;

    void onTimerInsertedAtFront() override;

    void ContextResize(size_t size);
private:
    int m_epfd = 0;

    //fd[0] read , fd[1] write
    int m_tickleFds[2];

    std::atomic<size_t> m_pendingEventCount = {0};

    std::shared_mutex m_mutex;

    //store fdcontext
    std::vector<FdContext*> m_fdContexts;
};