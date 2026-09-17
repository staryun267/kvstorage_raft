#include "thread.h"

#include <iostream>
#include <sys/syscall.h>
#include <unistd.h>

static thread_local Thread* t_thread = nullptr;
static thread_local std::string t_thread_name = "UNKNOW";

void* Thread::run(void* arg)
{
    Thread* thread = (Thread*)arg;
    t_thread = thread;
    t_thread_name = thread->m_name;
    thread->m_id = GetThreadId();

    pthread_setname_np(pthread_self(),thread->m_name.substr(0,15).c_str());
    std::function<void()> cb;
    cb.swap(thread->m_cb);  //swap 可减少m_cb智能指针的引用次数

    //初始化完成
    thread->m_semphore.signal();
    cb();
    
    return 0;
}

Thread::Thread(std::function<void()> cb, const std::string &name)
    : m_cb(cb),m_name(name)
{
    if(name.empty())
    {
        m_name = "UNKNOW";
    }

    int rt = pthread_create(&m_thread,nullptr,&run,this);
    if(rt)
    {
        //std::cout << "pthread_create error! rt = " << rt << ", name = " << name.c_str() << std::endl;
        throw std::logic_error("pthread_create error");
    }

    //等待线程函数初始化完毕
    m_semphore.wait();
}

Thread::~Thread()
{
    if(m_thread)
    {
        pthread_detach(m_thread);
        m_thread = 0;
    }
}



void Thread::join()
{
    if(m_thread)
    {
        int rt = pthread_join(m_thread,nullptr);
        if(rt)
        {
            //std::cout << "pthread_create error! rt = " << rt << ", name = " << m_name.c_str() << std::endl;
            throw std::logic_error("pthread_join error");
        }
        m_thread = 0;
    }
}

Thread* Thread::GetThis()
{
    return t_thread;
}

const std::string& Thread::GetName()
{
    return t_thread_name;
}

void Thread::SetName(const std::string& name)
{
    if(t_thread)
    {
        t_thread->m_name = name;
    }
    t_thread_name = name;
}

pid_t Thread::GetThreadId()
{
    return syscall(SYS_gettid);
}