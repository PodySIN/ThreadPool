#include "thpool.hpp"
#include <iostream>
#include <syncstream>
#include <stdexcept>
#include <thread>
#include <chrono>

int main()
{
  using namespace std::chrono_literals;

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
    for (int i = 0; i < 4; ++i) {
      pool.submit([i] { std::osyncstream(std::cout) << "Task " << i << " done\n"; });
    }
    std::this_thread::sleep_for(200ms);
    std::osyncstream(std::cout) << "While paused, tasks must not run\n";
    pool.resume();
    std::osyncstream(std::cout) << "Pool resumed: " << pool.isPaused() << "\n";
    pool.wait();
    std::osyncstream(std::cout) << "All tasks finished.\n\n";
  }

  std::osyncstream(std::cout) << "All tests done.\n";
  return 0;
}
