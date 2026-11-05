#include "thpool.hpp"
#include <stdexcept>
#include <utility>

hvostov::th_pool_t::th_pool_t(unsigned int k)
{
  if (k == 0) {
    k = 1;
  }

  for (unsigned int i = 0; i < k; ++i) {
    ths_.emplace_back([this] { execute(); });
  }
}

hvostov::th_pool_t::~th_pool_t()
{
  {
    std::lock_guard< std::mutex > lock(mx_);
    paused_ = false;
  }
  cd_.notify_all();

  wait();
  shutdown();
}

void hvostov::th_pool_t::submit(f_t f)
{
  {
    std::lock_guard< std::mutex > lock(mx_);
    if (stop_) {
      throw std::runtime_error("submit on stopped thread pool");
    }
    tasks_.push(std::move(f));
  }
  cd_.notify_one();
}

void hvostov::th_pool_t::shutdown()
{
  {
    std::lock_guard< std::mutex > lock(mx_);
    stop_ = true;
    while (!tasks_.empty()) {
      tasks_.pop();
    }
  }
  cd_.notify_all();

  for (auto& th : ths_) {
    if (th.joinable()) {
      th.join();
    }
  }
  ths_.clear();

  cv_finished_.notify_all();
}

void hvostov::th_pool_t::pause()
{
  std::lock_guard< std::mutex > lock(mx_);
  paused_ = true;
}

void hvostov::th_pool_t::resume()
{
  {
    std::lock_guard< std::mutex > lock(mx_);
    paused_ = false;
  }
  cd_.notify_all();
}

void hvostov::th_pool_t::wait()
{
  std::unique_lock< std::mutex > lock(mx_);
  cv_finished_.wait(lock, [this] { return tasks_.empty() && active_workers_ == 0; });
}

bool hvostov::th_pool_t::isPaused()
{
  std::lock_guard< std::mutex > lock(mx_);
  return paused_;
}

void hvostov::th_pool_t::execute()
{
  while (true) {
    f_t task;
    {
      std::unique_lock< std::mutex > lock(mx_);
      cd_.wait(lock, [this] { return stop_ || (!paused_ && !tasks_.empty()); });
      if (stop_ && tasks_.empty()) {
        return;
      }
      if (paused_) {
        continue;
      }
      task = std::move(tasks_.front());
      tasks_.pop();
      ++active_workers_;
    }

    try {
      task();
    } catch (...) {
    }
    {
      std::lock_guard< std::mutex > lock(mx_);
      --active_workers_;
      if (active_workers_ == 0 && tasks_.empty()) {
        cv_finished_.notify_all();
      }
    }
  }
}
