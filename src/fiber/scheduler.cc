#include "scheduler.h"

#include <unistd.h>
#include <iostream>

static thread_local Scheduler* t_scheduler = nullptr;

Scheduler::Scheduler(size_t threads, bool use_caller, const std::string& name)
    : m_useCaller(use_caller),m_name(name)
{
    if(threads < 0 || GetThis() != nullptr)
    {
        std::cout << "Scheduler init error!" << std::endl;
        exit(EXIT_FAILURE);
    }

    SetThis();
    Thread::SetName(m_name);

    //使用主线程当作工作线程
    if(use_caller)
    {
        threads--;

        //创建主协程
        Fiber::GetThis();

        //创建调度协程
        m_schedulerFiber.reset(new Fiber(std::bind(&Scheduler::run,this),0,false));
        Fiber::SetSchedulerFiber(m_schedulerFiber.get());

        m_rootThread = Thread::GetThreadId();
        m_threadIds.push_back(m_rootThread);
    }

    m_threadCount = threads;
}

Scheduler::~Scheduler()
{
    assert(m_stopping);
    if(GetThis() == this)
    {
        t_scheduler = nullptr;
    }
}

//获取正在运行的调度器
Scheduler* Scheduler::GetThis()
{
    return t_scheduler;
}

//设置正在运行的调度器
void Scheduler::SetThis()
{
    t_scheduler = this;
}

//启动线程池
void Scheduler::start()
{
    std::lock_guard<std::mutex> lock(m_mutex);
    if(m_stopping)
    {
        std::cout << "Scheduler has stopped" << std::endl;
        return;
    }

    assert(m_threads.empty());
    m_threads.resize(m_threadCount);
    for(int i=0;i<m_threadCount;i++)
    {
        m_threads[i].reset(new Thread(std::bind(&Scheduler::run,this),m_name + "_" + std::to_string(i)));
        m_threadIds.push_back(m_threads[i]->getId());
    }
}

//关闭线程池
void Scheduler::stop()
{
    if(stopping())
    {
        return;
    }

    m_stopping = true;

    if(m_useCaller)
    {
        assert(GetThis() == this);
    }
    else
    {
        assert(GetThis() != this);
    }

    for(size_t i=0;i<m_threadCount;i++)
    {
        tickle();
    }

    if(m_schedulerFiber)
    {
        tickle();
    }

    if(m_schedulerFiber)
    {
        m_schedulerFiber->resume();
    }

    std::vector<Thread::ptr> thrs;
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        thrs.swap(m_threads);
    }

    for(auto& i : thrs)
    {
        i->join();
    }
}

void Scheduler::tickle()
{

}

//是否可以关闭
bool Scheduler::stopping()
{
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_stopping && m_tasks.empty() && m_activeThreadCount == 0;
}

void Scheduler::run()
{
    int thread_id = Thread::GetThreadId();

    set_hook_enable(true);

    SetThis();

    //运行在新创建的线程，需要创建主协程
    if(thread_id != m_rootThread)
    {
        Fiber::GetThis();
    }
    
    Fiber::ptr idle_fiber = std::make_shared<Fiber>(std::bind(&Scheduler::idle,this));
    ScheduleTask task;

    while(true)
    {
        task.reset();
        bool tickle_me = false;
        {
            //1.遍历任务队列
            std::lock_guard<std::mutex> lock(m_mutex);
            auto it = m_tasks.begin();
            while(it != m_tasks.end())
            {
                //已经指定非当前线程去执行这个任务了
                if(it->thread != -1 && it->thread != thread_id)
                {
                    it++;
                    tickle_me = true;
                    continue;
                }

                //2.取出任务
                assert(it->fiber || it->cb);
                task = *it;
                m_tasks.erase(it);
                m_activeThreadCount++;
                break;
            }
            tickle_me = tickle_me || (it != m_tasks.end());
        }

        if(tickle_me)
        {
            tickle();
        }

        //3.执行任务
        if(task.fiber)
        {
            {
                std::lock_guard<std::mutex> lock(task.fiber->m_mutex);
                if(task.fiber->GetState() != Fiber::TERM)
                {
                    task.fiber->resume();
                }
            }

            m_activeThreadCount--;
            task.reset();
        }
        else if(task.cb)
        {
            Fiber::ptr cb_fiber = std::make_shared<Fiber>(task.cb);
            {
                std::lock_guard<std::mutex> lock(cb_fiber->m_mutex);
                if(cb_fiber->GetState() != Fiber::TERM)
                {
                    cb_fiber->resume();
                }
            }

            m_activeThreadCount--;
            task.reset();
        }
        else    //4.无任务，执行空闲协程
        {
            //将idle协程从死循环救出
            if(idle_fiber->GetState()  == Fiber::TERM)
            {
                break;
            }

            m_idleThreadCount++;
            idle_fiber->resume();
            m_idleThreadCount--;
        }
    }
}

//空闲协程函数
void Scheduler::idle()
{
    while(!m_stopping)
    {
        sleep(1);
        Fiber::GetThis()->yield();
    }
}