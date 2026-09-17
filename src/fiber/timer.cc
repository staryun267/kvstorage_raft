#include "timer.h"

//从时间堆中删除timer
bool Timer::cancel()
{
    std::unique_lock<std::shared_mutex> write_lock(m_manager->m_mutex);
    if(m_cb == nullptr)
    {
        return false;
    }
    else
    {
        m_cb = nullptr;
    }

    auto it = m_manager->m_timers.find(shared_from_this());
    if(it != m_manager->m_timers.end())
    {
        m_manager->m_timers.erase(it);
    }
    return true;
}

//刷新timer,只会向后调整
bool Timer::refresh()
{
    std::unique_lock<std::shared_mutex> write_lock(m_manager->m_mutex);
    if(!m_cb)
    {
        return false;
    }

    auto it = m_manager->m_timers.find(shared_from_this());
    if(it == m_manager->m_timers.end())
    {
        return false;
    }

    m_manager->m_timers.erase(it);
    m_next = std::chrono::system_clock::now() + std::chrono::milliseconds(m_ms);
    m_manager->m_timers.insert(shared_from_this());
    return true;
}

//重设timer的超时时间
bool Timer::reset(uint64_t ms,bool from_now)
{
    if(ms == m_ms && !from_now)
    {
        return true;
    }

    {
        std::unique_lock<std::shared_mutex> write_lock(m_manager->m_mutex);

        if(!m_cb)
        {
            return false;
        }

        auto it = m_manager->m_timers.find(shared_from_this());
        if(it == m_manager->m_timers.end())
        {
            return false;
        }

        m_manager->m_timers.erase(it);
    }

    //重新插入时间堆
    auto start = from_now ? std::chrono::system_clock::now() : m_next - std::chrono::milliseconds(m_ms);
    m_ms = ms;
    m_next = start + std::chrono::milliseconds(m_ms);
    m_manager->addTimer(shared_from_this());
    return true;
}

Timer::Timer(uint64_t ms,std::function<void()> cb,bool recurring,TimerManager* manager)
    : m_ms(ms),m_cb(cb),m_recurring(recurring),m_manager(manager)
{
    auto now = std::chrono::system_clock::now();
    m_next = now + std::chrono::milliseconds(m_ms);
}

//是实现最小堆的比较函数
bool Timer::Comparator::operator()(const Timer::ptr& lhs,const Timer::ptr& rhs) const
{
    assert(lhs && rhs);
    return lhs->m_next < rhs->m_next;
}

TimerManager::TimerManager()
{
    m_previousTime = std::chrono::system_clock::now();
}

TimerManager::~TimerManager()
{

}

//添加Timer
Timer::ptr TimerManager::addTimer(uint64_t ms,std::function<void()> cb,bool recurring)
{
    Timer::ptr timer(new Timer(ms,cb,recurring,this));
    addTimer(timer);
    return timer;
}

static void OnTimer(std::weak_ptr<void> weak_cond,std::function<void()> cb)
{
    std::shared_ptr<void> tmp = weak_cond.lock();
    if(tmp)
    {
        cb();
    }
}
//添加条件Timer,安全的执行回调函数(防止对象已经析构但定时器还存在的情况)
Timer::ptr TimerManager::addConditionTimer(uint64_t ms,std::function<void()> cb,std::weak_ptr<void> weak_cond,bool recurring)
{
    return addTimer(ms,std::bind(&OnTimer,weak_cond,cb),recurring);
}

//拿到堆中最近的超时时间
uint64_t TimerManager::getNextTimer()
{
    std::shared_lock<std::shared_mutex> read_lock(m_mutex);

    //重置m_tickled
    m_tickled = false;

    if(m_timers.empty())
    {   
        //返回最大值
        return ~0ull;
    }

    auto now = std::chrono::system_clock::now();
    auto time = (*m_timers.begin())->m_next;

    if(now >= time)
    {
        //已经有定时器超时了
        return 0;
    }
    else
    {
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(time - now);
        return static_cast<uint64_t>(duration.count());
    }
}

//取出所有超时定时器的回调函数
void TimerManager::listExpiredCb(std::vector<std::function<void()>> &cbs)
{
    auto now = std::chrono::system_clock::now();
    std::unique_lock<std::shared_mutex> write_lock(m_mutex);
    bool rollover = detectClockRollover();

    //发生回退，清理所有timer || 超时，清理超时的timer
    while(!m_timers.empty() && rollover || !m_timers.empty() && now >= (*m_timers.begin())->m_next)
    {
        Timer::ptr timer = *m_timers.begin();
        m_timers.erase(m_timers.begin());

        cbs.push_back(timer->m_cb);

        if(timer->m_recurring)
        {
            //重新入堆
            timer->m_next = now + std::chrono::microseconds(timer->m_ms);
            m_timers.insert(timer);
        }
        else
        {
            timer->m_cb = nullptr;
        }
    }
}

//堆中是否有timer
bool TimerManager::hasTimer()
{
    std::shared_lock<std::shared_mutex> read_lock(m_mutex);
    return !m_timers.empty();
}

//添加timer(lock + tickle)
void TimerManager::addTimer(Timer::ptr timer)
{
    bool at_front = false;
    {
        std::unique_lock<std::shared_mutex> write_lock(m_mutex);
        auto it = m_timers.insert(timer).first;
        at_front = (it == m_timers.begin()) && !m_tickled;

        if(at_front)
        {
            m_tickled = true;
        }
    }

    if(at_front)
    {
        onTimerInsertedAtFront();
    }
}

//当系统时间改变时调用该函数
bool TimerManager::detectClockRollover()
{
    bool rollover = false;
    auto now = std::chrono::system_clock::now();

    if(now < (m_previousTime - std::chrono::milliseconds(60 * 60 * 1000)))
    {
        rollover = true;
    }
    
    m_previousTime = now;
    return rollover;
}