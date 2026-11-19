#include "thpool.hpp"
#include <algorithm>
#include <stdexcept>
#include <utility>

hvostov::th_pool_t::th_pool_t(unsigned int k, std::size_t max_batch):
  max_batch_(max_batch == 0 ? 1 : max_batch)
{
  if (k == 0) {
    k = 1;
  }
  for (unsigned int i = 0; i < k; ++i) {
    workers_.push_back(std::make_unique< worker_t >());
  }
  for (unsigned int i = 0; i < k; ++i) {
    ths_.emplace_back([this, i] { execute(i); });
  }
}

hvostov::th_pool_t::~th_pool_t()
{
  resume();
  wait();
  shutdown();
}

void hvostov::th_pool_t::submit(f_t f)
{
  {
    std::unique_lock< std::shared_mutex > lock(rw_);
    if (stop_) {
      throw std::runtime_error("submit on stopped thread pool");
    }
    global_.push(std::move(f));
    ++queued_;
    ++pending_;
  }
  cv_.notify_one();
}

void hvostov::th_pool_t::shutdown()
{
  {
    std::unique_lock< std::shared_mutex > lock(rw_);
    stop_ = true;
    while (!global_.empty()) {
      global_.pop();
    }
  }
  cv_.notify_all();

  for (auto& th : ths_) {
    if (th.joinable()) {
      th.join();
    }
  }
  ths_.clear();

  for (auto& w : workers_) {
    std::lock_guard< std::mutex > lock(w->mx);
    w->local.clear();
  }
  queued_ = 0;
  {
    std::lock_guard< std::mutex > lock(done_mx_);
    pending_ = 0;
  }
  done_cv_.notify_all();
}

void hvostov::th_pool_t::pause()
{
  std::unique_lock< std::shared_mutex > lock(rw_);
  paused_ = true;
}

void hvostov::th_pool_t::resume()
{
  {
    std::unique_lock< std::shared_mutex > lock(rw_);
    paused_ = false;
  }
  cv_.notify_all();
}

void hvostov::th_pool_t::wait()
{
  std::unique_lock< std::mutex > lock(done_mx_);
  done_cv_.wait(lock, [this] { return pending_ == 0; });
}

bool hvostov::th_pool_t::isPaused()
{
  return paused_;
}

hvostov::th_pool_stats_t hvostov::th_pool_t::stats() const
{
  return {batches_, steals_, stolen_tasks_};
}

bool hvostov::th_pool_t::pop_local(worker_t& self, f_t& task)
{
  std::lock_guard< std::mutex > lock(self.mx);
  if (self.local.empty()) {
    return false;
  }
  task = std::move(self.local.front());
  self.local.pop_front();
  --queued_;
  return true;
}

bool hvostov::th_pool_t::fetch_batch(worker_t& self)
{
  {
    std::shared_lock< std::shared_mutex > rlock(rw_);
    if (global_.empty()) {
      return false;
    }
  }

  std::size_t n = 0;
  {
    std::unique_lock< std::shared_mutex > wlock(rw_);
    if (global_.empty() || stop_ || paused_) {
      return false;
    }
    const std::size_t workers = workers_.size();
    n = std::min(max_batch_, std::max< std::size_t >(1, (global_.size() + workers - 1) / workers));

    std::lock_guard< std::mutex > llock(self.mx);
    for (std::size_t i = 0; i < n; ++i) {
      self.local.push_back(std::move(global_.front()));
      global_.pop();
    }
  }
  ++batches_;
  if (n > 1) {
    cv_.notify_one();
  }
  return true;
}

bool hvostov::th_pool_t::steal(std::size_t id)
{
  const std::size_t n = workers_.size();
  for (std::size_t k = 1; k < n; ++k) {
    worker_t& victim = *workers_[(id + k) % n];
    std::vector< f_t > loot;
    {
      std::lock_guard< std::mutex > lock(victim.mx);
      const std::size_t sz = victim.local.size();
      if (sz == 0) {
        continue;
      }
      const std::size_t cnt = (sz + 1) / 2;
      for (std::size_t i = 0; i < cnt; ++i) {
        loot.push_back(std::move(victim.local.back()));
        victim.local.pop_back();
      }
    }
    {
      std::lock_guard< std::mutex > lock(workers_[id]->mx);
      for (auto& t : loot) {
        workers_[id]->local.push_back(std::move(t));
      }
    }
    ++steals_;
    stolen_tasks_ += loot.size();
    if (loot.size() > 1) {
      cv_.notify_one();
    }
    return true;
  }
  return false;
}

bool hvostov::th_pool_t::next_task(std::size_t id, f_t& task)
{
  worker_t& self = *workers_[id];
  while (true) {
    if (stop_) {
      return false;
    }
    if (!paused_) {
      if (pop_local(self, task)) {
        return true;
      }
      if (fetch_batch(self) || steal(id)) {
        continue;
      }
    }

    std::unique_lock< std::shared_mutex > lock(rw_);
    cv_.wait(lock, [this] { return stop_ || (!paused_ && queued_ > 0); });
  }
}

void hvostov::th_pool_t::execute(std::size_t id)
{
  while (true) {
    f_t task;
    if (!next_task(id, task)) {
      return;
    }

    try {
      task();
    } catch (...) {
    }

    if (--pending_ == 0) {
      std::lock_guard< std::mutex > lock(done_mx_);
      done_cv_.notify_all();
    }
  }
}
