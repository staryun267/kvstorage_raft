#pragma once

#include <ucontext.h>
#include <memory>
#include <mutex>
#include <functional>

class Fiber : public std::enable_shared_from_this<Fiber>
{
public:
    using ptr = std::shared_ptr<Fiber>;
    std::mutex m_mutex;

    //协程状态
    enum State {
        TERM,
        RUNNING,
        READY,
    };
private:
    //主协程
    Fiber();
public:
    //用户自定义协程
    Fiber(std::function<void()> cb, size_t stacksize = 0,bool run_in_scheduler = true);
    ~Fiber();

    //重用一个协程
    void reset(std::function<void()> cb);

    //切换到当前协程执行
    void resume();

    //切换到后台执行
    void yield();

    uint64_t GetId() const { return m_id; }
    State GetState() const { return m_state; }
public:
    //设置当前协程
    static void SetThis(Fiber* f);

    //返回当前协程
    static Fiber::ptr GetThis();

    //设置调度协程
    static void SetSchedulerFiber(Fiber* f);

    //协程函数
    static void MainFunc();

    //返回当前运行的协程的ID
    static uint64_t GetFiberId();
private:
    uint64_t m_id = 0;
    uint32_t m_stacksize = 0;
    State m_state = READY;

    ucontext_t m_ctx;
    void* m_stack = nullptr;
    std::function<void()> m_cb;

    //是否让出执行权交给调度协程
    bool m_runInScheduler;
};