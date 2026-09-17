#include "fiber.h"

#include <atomic>
#include <errno.h>
#include <iostream>

//协程ID自增计数器，全局唯一
static std::atomic<uint64_t> s_fiber_id{0};
//协程总计数器
static std::atomic<uint64_t> s_fiber_count{0};

//指向当前线程正在执行的协程
static thread_local Fiber* t_fiber = nullptr;
//指向当前线程的main协程
static thread_local Fiber::ptr t_thread_fiber = nullptr;
//调度协程
static thread_local Fiber* t_scheduler_fiber = nullptr;

//协程独立栈的大小
static const uint32_t g_fiber_stack_size = 1024 * 1024;

//内存管理
class MallocStackAllocator
{
public:
    static void* Alloc(size_t size)
    {
        return malloc(size);
    }

    static void Dealloc(void* vp, size_t size)
    {
        return free(vp);
    }
};

//降低耦合，便于未来的拓展
using StackAllocator = MallocStackAllocator;

//主协程
Fiber::Fiber()
{
    m_state = RUNNING;
    SetThis(this);

    if(getcontext(&m_ctx))
    {
        std::cout << "getcontext error! errno: " << errno << std::endl; 
        exit(EXIT_FAILURE);
    }

    m_id = s_fiber_id++;
	s_fiber_count ++;
}

//用户自定义协程
Fiber::Fiber(std::function<void()> cb, size_t stacksize, bool run_in_scheduler)
        : m_id(s_fiber_id++),
          m_cb(cb),
          m_runInScheduler(run_in_scheduler)
{
    s_fiber_count++;
    m_stacksize = stacksize ? stacksize : g_fiber_stack_size;

    m_stack = StackAllocator::Alloc(m_stacksize);
    if(getcontext(&m_ctx))
    {
        std::cout << "getcontext error! errno: " << errno << std::endl;
        exit(EXIT_FAILURE);
    }

    m_ctx.uc_link = nullptr;
    m_ctx.uc_stack.ss_sp = m_stack;
    m_ctx.uc_stack.ss_size = m_stacksize;

    makecontext(&m_ctx,&Fiber::MainFunc,0);
}

Fiber::~Fiber()
{
    --s_fiber_count;
    if(m_stack)
    {
        StackAllocator::Dealloc(m_stack,m_stacksize);
    }
    else
    {
        Fiber* cur = t_fiber;
        if(cur == this)
        {
            SetThis(nullptr);
        }
    }
}

// 重置协程函数并重置状态  INFO TERM
void Fiber::reset(std::function<void()> cb)
{
    if(!m_stack)
    {
        //std::cout << "Fiber::reset error,m_stack is null!" << std::endl;
        exit(EXIT_FAILURE);
    }

    if(m_state != TERM)
    {
        //std::cout << "Fiber::reset error, invalid state!" << std::endl;
        exit(EXIT_FAILURE);
    }

    m_cb = cb;
    if(getcontext(&m_ctx))
    {
        //std::cout << "getcontext error! errno: " << errno << std::endl;
        exit(EXIT_FAILURE);
    }

    m_ctx.uc_link = nullptr;
    m_ctx.uc_stack.ss_sp = m_stack;
    m_ctx.uc_stack.ss_size = m_stacksize;

    makecontext(&m_ctx,&Fiber::MainFunc,0);
    m_state = READY;
}

// 切换到当前协程执行
void Fiber::resume()
{
    SetThis(this);
    if(m_state != READY)
    {
        //std::cout << "swapIn error : invalid state" << std::endl;
        exit(EXIT_FAILURE);
    }

    m_state = RUNNING;
    if(m_runInScheduler)
    {
        if(swapcontext(&t_scheduler_fiber->m_ctx,&m_ctx))
        {
            //std::cout << "swapIn error: swapcontext error!" << std::endl;
            exit(EXIT_FAILURE);
        }
    }
    else
    {
        if(swapcontext(&t_thread_fiber->m_ctx, &m_ctx))
        {
            //std::cout << "resume error: swapcontext error!" << std::endl;
            exit(EXIT_FAILURE);
        }
    }
}

// 切换到后台执行
void Fiber::yield()
{
    if(m_state == READY)
    {
        //std::cout << "Fiber::yield m_state error!" << std::endl;
        exit(EXIT_FAILURE);
    }

    m_state = READY;
    if(m_runInScheduler)
    {
        SetThis(t_scheduler_fiber);
        if(swapcontext(&m_ctx,&t_scheduler_fiber->m_ctx))
        {
            //std::cout << "yield error: swapcontext error!" << std::endl;
            exit(EXIT_FAILURE);
        }
    }
    else
    {
        SetThis(t_thread_fiber.get());
        if(swapcontext(&m_ctx, &t_thread_fiber->m_ctx))
        {
            //std::cout << "yield error: swapcontext error!" << std::endl;
            exit(EXIT_FAILURE);
        }
    }
}

//设置当前协程
void Fiber::SetThis(Fiber* f)
{
    t_fiber = f;
}

//返回当前正在运行的协程，没有则创建主协程
Fiber::ptr Fiber::GetThis()
{
    if(t_fiber)
    {
        return t_fiber->shared_from_this();
    }
    Fiber::ptr main_fiber(new Fiber());
    t_thread_fiber = main_fiber;
    t_scheduler_fiber = main_fiber.get();  //主协程默认为调度协程

    if(t_fiber != main_fiber.get())
    {
        std::cout << "main Fiber init error!" << std::endl;
        exit(EXIT_FAILURE);
    }
    return t_fiber->shared_from_this();
}

//设置调度协程
void Fiber::SetSchedulerFiber(Fiber* f)
{
    t_scheduler_fiber = f;
}

void Fiber::MainFunc()
{
    Fiber::ptr cur = GetThis();
    if(cur == nullptr)
    {
        std::cout << "MainFun GetThis error!" << std::endl;
        exit(EXIT_FAILURE);
    }

    cur->m_cb();
    cur->m_cb = nullptr;
    cur->m_state = TERM;

    //运行完毕，让出执行权
    auto raw_ptr = cur.get();
    cur.reset();
    raw_ptr->yield();
}

uint64_t Fiber::GetFiberId()
{
    if(t_fiber)
    {
        return t_fiber->GetId();
    }
    return (uint64_t)-1;
}