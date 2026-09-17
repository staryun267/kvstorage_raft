#pragma once

#include <set>
#include <memory>
#include <mutex>
#include <assert.h>
#include <vector>
#include <functional>
#include <shared_mutex>

class TimerManager;

class Timer : public std::enable_shared_from_this<Timer>
{
    friend class TimerManager;
public:
    using ptr = std::shared_ptr<Timer>;
    //从时间堆中删除timer
    bool cancel();
    //刷新timer
    bool refresh();
    //重设timer的超时时间
    bool reset(uint64_t ms,bool from_now);

private:
    Timer(uint64_t ms,std::function<void()> cb,bool recurring,TimerManager* manager);

private:
    //是否循环
    bool m_recurring = false;
    //超时时间
    uint64_t m_ms = 0;
    //绝对超时时间
    std::chrono::time_point<std::chrono::system_clock> m_next;
    //超时时触发的回调函数
    std::function<void()> m_cb;
    //管理此Timer的管理器
    TimerManager* m_manager = nullptr;

private:
    //是实现最小堆的比较函数
    struct Comparator
    {
        bool operator()(const Timer::ptr& lhs,const Timer::ptr& rhs) const;
    };
};

class TimerManager
{
    friend class Timer;
public:
    TimerManager();
    virtual ~TimerManager();

    //添加Timer
    Timer::ptr addTimer(uint64_t ms,std::function<void()> cb,bool recurring = false);

    //添加条件Timer
    Timer::ptr addConditionTimer(uint64_t ms,std::function<void()> cb,std::weak_ptr<void> weak_cond,bool recurring = false);

    //拿到堆中最近的超时时间
    uint64_t getNextTimer();

    //取出所有超时定时器的回调函数
    void listExpiredCb(std::vector<std::function<void()>> &cbs);

    //堆中是否有timer
    bool hasTimer();
protected:
    //当一个最早的Timer加入到堆中，调用该函数
    virtual void onTimerInsertedAtFront() {};

    //添加timer
    void addTimer(Timer::ptr timer);

private:
    //当系统时间改变时调用该函数
    bool detectClockRollover();

private:
    std::shared_mutex m_mutex;
    //时间堆
    std::set<Timer::ptr,Timer::Comparator> m_timers;
    //上一次检查系统时间是否回退的绝对时间
    std::chrono::time_point<std::chrono::system_clock> m_previousTime;
    //在下次getNextTime执行前，onTimerInsertAtFront是否已经触发了，在此期间onTimerInsertAtFront只能执行一次
    bool m_tickled = false;
};