/*
* Copyright (C) Xinjing Cho
*/
#ifndef RAFTCORE_QUEUE_H_
#define RAFTCORE_QUEUE_H_
#include <cstddef>
#include <queue>
#include <utility>
#include <condition_variable>

namespace raftcore {

/* simple thread-safe queue implementation */
template<typename _Ele>
class thread_safe_queue {
public:
    size_t size() { return q_.size(); }

    bool empty() const { return q_.empty(); }
    
    _Ele pop() {
        std::unique_lock<std::mutex> lck(mtx_);
        while(q_.empty()) cv_.wait(lck);
        auto e = q_.front();
        q_.pop();
        return e;
    }

    void pop(_Ele & e) {
        std::unique_lock<std::mutex> lck(mtx_);
        while(q_.empty()) cv_.wait(lck);
        e = q_.front();
        q_.pop();
    }

    /* 
    * try to pop a element,
    * return immediately if there is no element available.
    */
    bool try_pop(_Ele & e) {
        std::unique_lock<std::mutex> lck(mtx_);
        if (q_.empty())return false;
        e = q_.front();
        q_.pop();
        return true;
    }
    
    bool empty() {
        std::unique_lock<std::mutex> lck(mtx_);
        return q_.empty();
    }
    
    void push(const _Ele & e) {
        std::unique_lock<std::mutex> lck(mtx_);
        q_.push(e);
        lck.unlock();   // unlock mutex before notification to minimize lock contention
        cv_.notify_one();
    }

    void push(_Ele && e) {
        std::unique_lock<std::mutex> lck(mtx_);
        q_.push(std::move(e));
        lck.unlock();   // unlock mutex before notification to minimize lock contention
        cv_.notify_one();
    }
private:
    std::queue<_Ele>        q_;
    std::mutex              mtx_;
    std::condition_variable cv_;
};

}
#endif