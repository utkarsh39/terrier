#pragma once

#include <queue>
#include <vector>
#include <common/worker_pool.h>
#include "common/spin_latch.h"
#include "storage/record_buffer.h"
#include "storage/write_ahead_log/log_io.h"
#include "storage/write_ahead_log/log_record.h"
#include "transaction/transaction_defs.h"

namespace terrier::storage {
class LogThreadContext {
 public:
  LogThreadContext(const char *log_file_path) : out_(log_file_path) {}

  /**
   * Flush the logs to make sure all serialized records before this invocation are persistent. Callbacks from committed
   * transactions are also invoked when possible. This method should only be called from a dedicated logging thread.
   *
   * Usually this method is called from Process(), but can also be called by itself if need be.
   */
  void Flush() {
    out_.Persist();
    for (auto &callback : commits_in_buffer_) callback.first(callback.second);
    commits_in_buffer_.clear();
  }

  void AddCallback(transaction::callback_fn callback, void *args) {
    commits_in_buffer_.emplace_back(callback, args);
  }

  template <class T>
  void WriteValue(const T &val) {
    out_.BufferWrite(&val, sizeof(T));
  }

  template <class T>
  void WriteValue(const T &val, uint32_t size) {
    out_.BufferWrite(&val, size);
  }

 private:
  friend class LogManager;
  // TODO(Tianyu): This can be changed later to be include things that are not necessarily backed by a disk
  //  (e.g. logs can be streamed out to the network for remote replication)
  BufferedLogWriter out_;
  // These do not need to be thread safe since the only thread adding or removing from it is the flushing thread
  std::vector<std::pair<transaction::callback_fn, void *>> commits_in_buffer_;
};

}  // namespace terrier::storage