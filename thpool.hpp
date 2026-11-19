#ifndef THPOOL_HPP
#define THPOOL_HPP

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <shared_mutex>
#include <thread>
#include <vector>

namespace hvostov {
  struct task_t {
    void operator()();
  };

  struct th_pool_stats_t {
    std::size_t batches;
    std::size_t steals;
    std::size_t stolen_tasks;
  };

  struct th_pool_t {
    using f_t = std::function< void(void) >;

    explicit th_pool_t(unsigned int k, std::size_t max_batch = 4);
    ~th_pool_t();

    th_pool_t(const th_pool_t&) = delete;
    th_pool_t& operator=(const th_pool_t&) = delete;

    void submit(f_t f);
    void shutdown();
    void pause();
    void resume();
    void wait();
    bool isPaused();
    th_pool_stats_t stats() const;

  private:
    struct worker_t {
      std::mutex mx;
      std::deque< f_t > local;
    };

    void execute(std::size_t id);
    bool next_task(std::size_t id, f_t& task);
    bool pop_local(worker_t& self, f_t& task);
    bool fetch_batch(worker_t& self);
    bool steal(std::size_t id);

    std::queue< f_t > global_;
    mutable std::shared_mutex rw_;
    std::condition_variable_any cv_;

    std::mutex done_mx_;
    std::condition_variable done_cv_;

    std::atomic< bool > stop_{false};
    std::atomic< bool > paused_{false};
    std::atomic< std::size_t > queued_{0};
    std::atomic< std::size_t > pending_{0};

    std::atomic< std::size_t > batches_{0};
    std::atomic< std::size_t > steals_{0};
    std::atomic< std::size_t > stolen_tasks_{0};

    std::size_t max_batch_;
    std::vector< std::unique_ptr< worker_t > > workers_;
    std::vector< std::thread > ths_;
  };
}

#endif
