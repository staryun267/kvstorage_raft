#pragma once

#include <memory>
#include <functional>
#include <string>
#include <thread>
#include <mutex>
#include <condition_variable>

//用于线程方法之间的同步
class Semaphore
{
public:
    explicit Semaphore(int count = 0) : count_(count) {}

    void wait()
    {
        std::unique_lock<std::mutex> lock(mtx);
        while(count_ == 0)
        {
            cv.wait(lock);
        }

        count_--;
    }

    void signal()
    {
        std::unique_lock<std::mutex> lock(mtx);
        count_++;
        cv.notify_one();
    }
private:
    int count_;
    std::mutex mtx;
    std::condition_variable cv;
};

class Thread
{
public:
    using ptr = std::shared_ptr<Thread>;

    Thread(std::function<void()> cb,const std::string& name);
    ~Thread();

    pid_t getId() const { return m_id; }
    const std::string& getName() const { return m_name; }

    void join();

    static Thread* GetThis();
    static const std::string& GetName();
    static void SetName(const std::string& name);
    static pid_t GetThreadId();
private:
    Thread(const Thread&) = delete;
    Thread(const Thread&&) = delete;
    Thread& operator=(const Thread&) = delete;

    static void* run(void* arg);
private:
    pid_t m_id;
    pthread_t m_thread;
    std::function<void()> m_cb;
    std::string m_name;

    Semaphore m_semphore;
};