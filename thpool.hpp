#ifndef THPOOL_HPP
#define THPOOL_HPP

#include <functional>
#include <mutex>
#include <queue>
#include <vector>
#include <thread>
#include <condition_variable>
#include <cstddef>

namespace hvostov {
  struct task_t {
    void operator()();
  };

  struct th_pool_t {
    using f_t = std::function< void(void) >;

    explicit th_pool_t(unsigned int k);
    ~th_pool_t();

    th_pool_t(const th_pool_t&) = delete;
    th_pool_t& operator=(const th_pool_t&) = delete;

    void submit(f_t f);
    void shutdown();
    void pause();
    void resume();
    void wait();
    bool isPaused();

  private:
    void execute();

    std::queue< f_t > tasks_;
    std::mutex mx_;
    std::condition_variable cd_;
    std::condition_variable cv_finished_;

    bool stop_{false};
    bool paused_{false};
    std::size_t active_workers_{0};

    std::vector< std::thread > ths_;
  };
}

#endif
