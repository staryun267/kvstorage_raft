#pragma once

#include <mutex>
#include <vector>
#include <string>
#include <atomic>
#include <assert.h>
#include <functional>

#include "thread.h"
#include "fiber.h"
#include "hook.h"

class Scheduler
{
public:
    Scheduler(size_t threads = 1, bool use_caller = true, const std::string& name = "Scheduler");
    virtual ~Scheduler();

    //获取正在运行的调度器
    static Scheduler* GetThis();

    const std::string& getName() const { return m_name; }

public:
    template <class FiberOrCb>
    void scheduleLock(FiberOrCb fc, int thread = -1)
    {
        bool need_tickle;
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            need_tickle = m_tasks.empty();  //任务队列为空，则说明线程闲置，需要唤醒线程
            ScheduleTask task(fc,thread);

            if(task.cb || task.fiber)
            {
                m_tasks.push_back(task);
            }
        }

        if(need_tickle)
        {
            tickle();
        }
    }

    //启动线程池
    virtual void start();

    //关闭线程池
    virtual void stop();

protected:
    virtual void tickle();

    //设置正在运行的调度器
    void SetThis();

    //线程函数
    virtual void run();

    //空闲协程函数
    virtual void idle();

    //是否可以关闭
    virtual bool stopping();

    bool hasIdleThread() { return m_idleThreadCount > 0; }
private:
    //任务
    struct ScheduleTask
    {
        Fiber::ptr fiber;
        std::function<void()> cb;
        int thread;     //指定任务需要运行的线程id

        ScheduleTask()
        {
            fiber = nullptr;
            cb = nullptr;
            thread = -1;
        }

        ScheduleTask(Fiber::ptr f, int thr)
        {
            fiber = f;
            thread = thr;
        }

        ScheduleTask(Fiber::ptr* f, int thr)
        {
            fiber.swap(*f);
            thread = thr;
        }

        ScheduleTask(std::function<void()> f, int thr)
        {
            cb = f;
            thread = thr;
        }

        ScheduleTask(std::function<void()>* f, int thr)
        {
            cb.swap(*f);
            thread = thr;
        }

        void reset()
        {
            fiber = nullptr;
            cb = nullptr;
            thread = -1;
        }
    };
private:
    std::string m_name;
    std::mutex m_mutex;
    //线程池
    std::vector<Thread::ptr> m_threads;
    //任务队列
    std::vector<ScheduleTask> m_tasks;
    //存储工作线程的线程id
    std::vector<int> m_threadIds;
    //需要额外创建的线程数
    size_t m_threadCount = 0;
    //活跃线程数
    std::atomic<size_t> m_activeThreadCount = {0};
    //空闲线程数
    std::atomic<size_t> m_idleThreadCount = {0};

    //主线程是否用作工作线程
    bool m_useCaller;
    //如果是，需要额外创建调度协程并记录主线程id
    Fiber::ptr m_schedulerFiber;
    int m_rootThread = -1;

    //是否正在关闭
    bool m_stopping = false;
};