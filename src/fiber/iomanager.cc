#include "iomanager.h"

#include <assert.h>
#include <unistd.h>
#include <fcntl.h>
#include <cstring>
#include <errno.h>
#include <stdexcept>
#include <sys/epoll.h>
#include <iostream>

IOManager::FdContext::EventContext& IOManager::FdContext::getEventContext(IOManager::Event event)
{
    assert(event == READ || event == WRITE);

    switch(event)
    {
    case READ:
        return read;
    case WRITE:
        return write;
    }

    throw std::invalid_argument("event is invaild");
}

void IOManager::FdContext::resetEventContext(IOManager::FdContext::EventContext &ctx)
{
    ctx.cb = nullptr;
    ctx.fiber.reset();
    ctx.scheduler = nullptr;
}

void IOManager::FdContext::triggerEvent(IOManager::Event event)
{
    assert(events & event);

    //delete event
    events = (Event)(events & ~event);

    //trigger
    EventContext& ctx = getEventContext(event);
    if(ctx.cb)
    {
        ctx.scheduler->scheduleLock(ctx.cb);
    }
    else
    {
        ctx.scheduler->scheduleLock(ctx.fiber);
    }

    //reset
    resetEventContext(ctx);

    return;
}

IOManager::IOManager(size_t threads, bool use_caller, const std::string &name)
    : Scheduler(threads,use_caller,name),TimerManager()
{
    //create epoll fd
    m_epfd = epoll_create(5000);
    assert(m_epfd > 0);

    //create pipe
    int rt = pipe(m_tickleFds);
    assert(!rt);

    //add read event to epoll
    epoll_event event;
    event.events = EPOLLIN | EPOLLET;
    event.data.fd = m_tickleFds[0];

    //set nonblock
    rt = fcntl(m_tickleFds[0],F_SETFL,O_NONBLOCK);
    assert(!rt);

    rt = epoll_ctl(m_epfd,EPOLL_CTL_ADD,m_tickleFds[0],&event);
    assert(!rt);

    ContextResize(32);

    start();
}

IOManager::~IOManager()
{
    stop();
    close(m_epfd);
    close(m_tickleFds[0]);
    close(m_tickleFds[1]);

    for(size_t i=0;i<m_fdContexts.size();i++)
    {
        if(m_fdContexts[i])
        {
            delete m_fdContexts[i];
        }
    }
}

// add one event at a time
int IOManager::addEvent(int fd, IOManager::Event event, std::function<void()> cb)
{
    //attemp to find fdcontext
    FdContext* fd_ctx = nullptr;

    std::shared_lock<std::shared_mutex> read_lock(m_mutex);
    if((int)m_fdContexts.size() > fd)
    {
        fd_ctx = m_fdContexts[fd];
        read_lock.unlock();
    }
    else
    {
        read_lock.unlock();
        std::unique_lock<std::shared_mutex> write_lock(m_mutex);
        ContextResize(fd * 1.5);
        fd_ctx = m_fdContexts[fd];
    }

    std::lock_guard<std::mutex> lock(fd_ctx->mutex);
    //the event has already added
    if(fd_ctx->events & event)
    {
        return -1;
    }

    //add new event
    int op = fd_ctx->events ? EPOLL_CTL_MOD : EPOLL_CTL_ADD;
    epoll_event epevent;
    epevent.events = EPOLLET | fd_ctx->events | event;
    epevent.data.ptr = fd_ctx;

    int rt = epoll_ctl(m_epfd,op,fd,&epevent);
    if(rt)
    {
        std::cout << "addEvent: epoll_ctl error: " << errno << std::endl;
        return -1;
    }

    m_pendingEventCount++;

    //update fdcontext
    fd_ctx->events = (Event)(fd_ctx->events | event);

    //update eventcontext
    FdContext::EventContext& event_ctx = fd_ctx->getEventContext(event);
    assert(!event_ctx.cb && !event_ctx.fiber && !event_ctx.scheduler);
    event_ctx.scheduler = Scheduler::GetThis();
    if(cb)
    {
        event_ctx.cb.swap(cb);
    }
    else
    {
        event_ctx.fiber = Fiber::GetThis();
        assert(event_ctx.fiber->GetState() == Fiber::RUNNING);
    }
    return 0;
}

// delete event
bool IOManager::delEvent(int fd, IOManager::Event event)
{
    //attemp to find fdcontext
    FdContext* fd_ctx = nullptr;
    std::shared_lock<std::shared_mutex> read_lock(m_mutex);
    if((int)m_fdContexts.size() > fd)
    {
        fd_ctx = m_fdContexts[fd];
        read_lock.unlock();
    }
    else
    {
        read_lock.unlock();
        return false;
    }

    std::lock_guard<std::mutex> lock(fd_ctx->mutex);
    if(!(fd_ctx->events & event))
    {
        return false;
    }

    //delete this event
    Event new_events = (Event)(fd_ctx->events & ~event);
    int op = new_events ? EPOLL_CTL_MOD : EPOLL_CTL_DEL;

    epoll_event epevent;
    epevent.events = EPOLLET | new_events;
    epevent.data.ptr = fd_ctx;

    int rt = epoll_ctl(m_epfd,op,fd,&epevent);
    if(rt)
    {
        std::cout << "delEvent: epoll_ctl error: " << errno << std::endl;
        return false;
    }

    m_pendingEventCount--;

    //update fdcontext
    fd_ctx->events = new_events;

    //update eventcontext
    FdContext::EventContext& event_ctx = fd_ctx->getEventContext(event);
    fd_ctx->resetEventContext(event_ctx);
    return true;
}

// delete this event and trigger its callback
bool IOManager::cancelEvent(int fd, IOManager::Event event)
{
    //attemp to find fdcontex
    FdContext* fd_ctx = nullptr;
    std::shared_lock<std::shared_mutex> read_lock(m_mutex);
    if((int)m_fdContexts.size() > fd)
    {
        fd_ctx = m_fdContexts[fd];
        read_lock.unlock();
    }
    else
    {
        read_lock.unlock();
        return false;
    }

    std::lock_guard<std::mutex> lock(fd_ctx->mutex);
    if(!(fd_ctx->events & event))
    {
        return false;
    }

    //delete the event
    Event new_events = (Event)(fd_ctx->events & ~event);
    int op = new_events ? EPOLL_CTL_MOD : EPOLL_CTL_DEL;

    epoll_event epevent;
    epevent.events = EPOLLET | new_events;
    epevent.data.ptr = fd_ctx;

    int rt = epoll_ctl(m_epfd,op,fd,&epevent);
    if(rt)
    {
        std::cout << "cancelEvent: epoll_ctl error: " << errno << std::endl;
        return false;
    }

    m_pendingEventCount--;
    
    //update fdcontext eventcontext , trigger event
    fd_ctx->triggerEvent(event);
    return true;
}

// delete all events and trigger their callback
bool IOManager::cancelAll(int fd)
{
    FdContext* fd_ctx = nullptr;
    std::shared_lock<std::shared_mutex> read_lock(m_mutex);
    if((int)m_fdContexts.size() > fd)
    {
        fd_ctx = m_fdContexts[fd];
        read_lock.unlock();
    }
    else
    {
        read_lock.unlock();
        return false;
    }

    std::lock_guard<std::mutex> lock(fd_ctx->mutex);
    if(!fd_ctx->events)
    {
        return false;
    }

    epoll_event epevent;
    epevent.events = 0;
    epevent.data.ptr = fd_ctx;
    int op = EPOLL_CTL_DEL;

    int rt = epoll_ctl(m_epfd,op,fd,&epevent);
    if(rt)
    {
        std::cout << "cancelAll: epoll_ctl error: " << errno << std::endl;
        return false;
    }

    if(fd_ctx->events & READ)
    {
        fd_ctx->triggerEvent(READ);
        m_pendingEventCount--;
    }

    if(fd_ctx->events & WRITE)
    {
        fd_ctx->triggerEvent(WRITE);
        m_pendingEventCount--;
    }

    assert(fd_ctx->events == 0);
    return true;
}

IOManager* IOManager::GetThis()
{
    return dynamic_cast<IOManager*>(Scheduler::GetThis());
}

void IOManager::tickle()
{
    //no idle threads
    if(!hasIdleThread())
    {
        return;
    }

    int rt = write(m_tickleFds[1],"T",1);
    assert(rt == 1);
}

bool IOManager::stopping()
{
    uint64_t timeout = getNextTimer();
    return timeout == ~0ull && m_pendingEventCount == 0 && Scheduler::stopping();
}

void IOManager::idle()
{
    static const uint64_t MAX_EVENTS = 256;
    std::unique_ptr<epoll_event[]> events(new epoll_event[MAX_EVENTS]);

    while(true)
    {
        if(stopping())
        {
            break;
        }

        //blocked at epoll_wait
        int rt = 0;
        while(true)
        {
            static const uint64_t MAX_TIMEOUT = 5000;
            uint64_t next_timeout = getNextTimer();
            next_timeout = std::min(next_timeout,MAX_TIMEOUT);

            rt = epoll_wait(m_epfd,events.get(),MAX_EVENTS,next_timeout);
            if(rt < 0 && errno == EINTR)
            {
                continue;
            }
            else
            {
                break;
            }
        }

        //collect all timers overdue
        std::vector<std::function<void()>> cbs;
        listExpiredCb(cbs);
        if(!cbs.empty())
        {
            for(auto& cb : cbs)
            {
                scheduleLock(cb);
            }
            cbs.clear();
        }

        //collect all event ready
        for(int i=0;i<rt;i++)
        {
            epoll_event& event = events[i];
            //tickle event
            if(event.data.fd == m_tickleFds[0])
            {
                uint8_t dummy[256];
                //ET
                while(read(m_tickleFds[0],dummy,sizeof(dummy)) > 0);
                continue;
            }

            //other events
            FdContext* fd_ctx = (FdContext*)event.data.ptr;
            std::lock_guard<std::mutex> lock(fd_ctx->mutex);

            //convert EPOLLERR or EPOLLHUB to read or write event
            if(event.events & (EPOLLERR | EPOLLHUP))
            {
                event.events |= (EPOLLIN | EPOLLOUT) & fd_ctx->events;
            }

            int real_events = NONE;
            if(event.events & EPOLLIN)
            {
                real_events |= READ;
            }
            if(event.events & EPOLLOUT)
            {
                real_events |= WRITE;
            }

            if((fd_ctx->events & real_events) == NONE)
            {
                continue;
            }

            //delete the event that have already happend
            int left_events = (fd_ctx->events & ~real_events);
            int op = left_events ? EPOLL_CTL_MOD : EPOLL_CTL_DEL;

            event.events = EPOLLET | left_events;

            int rt1 = epoll_ctl(m_epfd,op,fd_ctx->fd,&event);
            if(rt1)
            {
                std::cout << "idle: epoll_ctl error: " << errno << std::endl;
                continue;
            }

            //scheduler callback update fdcontext and eventcontext
            if(real_events & READ)
            {
                fd_ctx->triggerEvent(READ);
                m_pendingEventCount--;
            }
            if(real_events & WRITE)
            {
                fd_ctx->triggerEvent(WRITE);
                m_pendingEventCount--;
            }
        }

        Fiber::GetThis()->yield();
    }
}

void IOManager::onTimerInsertedAtFront()
{
    tickle();
}

void IOManager::ContextResize(size_t size)
{
    m_fdContexts.resize(size);

    for(size_t i=0;i<m_fdContexts.size();i++)
    {
        if(m_fdContexts[i] == nullptr)
        {
            m_fdContexts[i] = new FdContext();
            m_fdContexts[i]->fd = i;
        }
    }
}