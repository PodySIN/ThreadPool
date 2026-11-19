#include "thpool.hpp"
#include <atomic>
#include <chrono>
#include <iostream>
#include <stdexcept>
#include <syncstream>
#include <thread>
#include <vector>

namespace {
  using namespace std::chrono_literals;
  using clock_t_ = std::chrono::steady_clock;

  struct mixed_result_t {
    long long ms;
    std::size_t executed;
    hvostov::th_pool_stats_t st;
  };

  mixed_result_t run_mixed(unsigned threads, std::size_t batch)
  {
    constexpr int long_n = 8;
    constexpr int short_n = 200;
    std::atomic< std::size_t > executed{0};

    auto start = clock_t_::now();
    hvostov::th_pool_t pool(threads, batch);
    for (int i = 0; i < long_n; ++i) {
      pool.submit([&executed] {
        std::this_thread::sleep_for(100ms);
        ++executed;
      });
    }
    for (int i = 0; i < short_n; ++i) {
      const auto d = std::chrono::milliseconds(1 + i % 5);
      pool.submit([&executed, d] {
        std::this_thread::sleep_for(d);
        ++executed;
      });
    }
    pool.wait();
    auto ms = std::chrono::duration_cast< std::chrono::milliseconds >(clock_t_::now() - start).count();
    return {ms, executed.load(), pool.stats()};
  }
} // namespace

int main()
{
  std::osyncstream(std::cout) << "Task 1: stop without finishing tasks\n";
  {
    hvostov::th_pool_t pool(2);
    for (int i = 0; i < 10; ++i) {
      pool.submit([i] {
        std::this_thread::sleep_for(30ms);
        std::osyncstream(std::cout) << "Task " << i << " done\n";
      });
    }
    std::this_thread::sleep_for(50ms);
    std::osyncstream(std::cout) << "Calling shutdown...\n";
    pool.shutdown();
    std::osyncstream(std::cout) << "Shutdown finished. Queued tasks were dropped.\n\n";
  }

  std::osyncstream(std::cout) << "Task 2: wait for current tasks to finish\n";
  {
    hvostov::th_pool_t pool(4);
    for (int i = 0; i < 8; ++i) {
      pool.submit([i] {
        std::this_thread::sleep_for(50ms);
        std::osyncstream(std::cout) << "Task " << i << " done\n";
      });
    }
    std::osyncstream(std::cout) << "Waiting for all tasks...\n";
    pool.wait();
    std::osyncstream(std::cout) << "All tasks finished.\n\n";
  }

  std::osyncstream(std::cout) << "Task 3: pause and resume\n";
  {
    hvostov::th_pool_t pool(2);
    pool.pause();
    std::osyncstream(std::cout) << "Pool paused: " << pool.isPaused() << "\n";
    std::atomic< int > ran_while_paused{0};
    for (int i = 0; i < 4; ++i) {
      pool.submit([i, &ran_while_paused] {
        ++ran_while_paused;
        std::osyncstream(std::cout) << "Task " << i << " done\n";
      });
    }
    std::this_thread::sleep_for(200ms);
    std::osyncstream(std::cout) << "While paused, tasks must not run (ran: " << ran_while_paused << ")\n";
    pool.resume();
    std::osyncstream(std::cout) << "Pool resumed: " << pool.isPaused() << "\n";
    pool.wait();
    std::osyncstream(std::cout) << "All tasks finished.\n\n";
  }

  std::osyncstream(std::cout) << "Task 4: tasks of different duration (8 x "
                                 "100ms + 200 x 1..5ms), 4 threads\n";
  {
    for (std::size_t batch : {1u, 4u, 8u, 16u}) {
      auto r = run_mixed(4, batch);
      std::osyncstream(std::cout) << "batch=" << batch << ": " << r.ms << " ms, executed=" << r.executed
                                  << "/208, batches=" << r.st.batches << ", steals=" << r.st.steals
                                  << ", stolen_tasks=" << r.st.stolen_tasks << "\n";
    }
    std::osyncstream(std::cout) << "\n";
  }

  std::osyncstream(std::cout) << "All tests done.\n";
  return 0;
}
