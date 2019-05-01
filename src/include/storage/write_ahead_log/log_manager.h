#pragma once

#include <common/worker_pool.h>
#include <queue>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include "common/spin_latch.h"
#include "common/strong_typedef.h"
#include "storage/record_buffer.h"
#include "storage/write_ahead_log/log_io.h"
#include "storage/write_ahead_log/log_record.h"
#include "storage/write_ahead_log/log_thread_context.h"
#include "transaction/transaction_defs.h"

namespace terrier::storage {
/**
 * A LogManager is responsible for serializing log records out and keeping track of whether changes from a transaction
 * are persistent.
 */
class LogManager {
 public:
  /**
   * Constructs a new LogManager, writing its logs out to the given file.
   *
   * @param log_file_path path to the desired log file location. If the log file does not exist, one will be created;
   *                      otherwise, changes are appended to the end of the file.
   * @param buffer_pool the object pool to draw log buffers from. This must be the same pool transactions draw their
   *                    buffers from
   */
  LogManager(const char *log_file_path, RecordBufferSegmentPool *const buffer_pool, int num_threads)
      : buffer_pool_(buffer_pool), num_threads_(num_threads), thread_pool_(num_threads_, {}) {
    common::SpinLatch::ScopedSpinLatch guard(&contexts_latch_);
    for (int i = 0; i < num_threads_; i++) {
      auto *context = new LogThreadContext(log_file_path);
      logging_contexts_queue_.push_front(context);
    }
  }

  ~LogManager() {
    common::SpinLatch::ScopedSpinLatch guard(&contexts_latch_);
    for (auto *context : logging_contexts_queue_) {
      delete context;
    }
  }

  /**
   * Must be called when no other threads are doing work
   */
  void Shutdown() {
    Process();
    FlushAll();
    CloseAll();
    thread_pool_.Shutdown();
  }

  /**
   * Returns a (perhaps partially) filled log buffer to the log manager to be consumed. Caller should drop its
   * reference to the buffer after the method returns immediately, as it would no longer be safe to read from or
   * write to the buffer. This method can be called safely from concurrent execution threads.
   *
   * @param buffer_segment the (perhaps partially) filled log buffer ready to be consumed
   */
  void AddBufferToFlushQueue(RecordBufferSegment *const buffer_segment) {
    common::SpinLatch::ScopedSpinLatch guard(&flush_queue_latch_);
    flush_queue_.push(buffer_segment);
  }

  void FlushAll();

  void CloseAll();

  /**
   * Process all the accumulated log records and serialize them out to disk. A flush will always happen at the end.
   * (Beware the performance consequences of calling flush too frequently) This method should only be called from a
   * dedicated
   * logging thread.
   */
  void Process();

  void SetNumberOfThreads(int num_threads) { num_threads_ = num_threads; }

 private:
  friend class LogThreadContext;

  RecordBufferSegmentPool *buffer_pool_;

  // TODO(Tianyu): Might not be necessary, since commit on txn manager is already protected with a latch
  common::SpinLatch flush_queue_latch_;
  // TODO(Tianyu): benchmark for if these should be concurrent data structures, and if we should apply the same
  //  optimization we applied to the GC queue.
  std::queue<RecordBufferSegment *> flush_queue_;

  // The number of threads
  int num_threads_;
  common::WorkerPool thread_pool_;
  // Logging Contexts for each of the threads
  std::forward_list<LogThreadContext *> logging_contexts_queue_;
  // Latch on the contexts queue
  common::SpinLatch contexts_latch_;
  // Latch on the file so that only one thread can write to a file at a time
  common::SpinLatch file_latch_;

  LogThreadContext *GetContext();

  void PushContextToQueue(LogThreadContext *context);

  /**
   * Calculate the size of the log for a record
   * @param record the record
   * @return the size of the record
   */
  uint32_t GetRecordSize(const terrier::storage::LogRecord &record) const;

  /**
   * Calculate the size of the log for a task buffer
   * @param task_buffer the task buffer
   * @return the size of the buffer
   */
  uint32_t GetTaskBufferSize(IterableBufferSegment<LogRecord> *task_buffer);

  /**
   * Calculate the size of the value
   * @tparam T
   * @param val the value
   * @return the size of the value
   */
  template <class T>
  uint32_t GetValueSize(const T &val) const {
    return sizeof(T);
  }

  void ProcessTaskBuffer(IterableBufferSegment<LogRecord> *task_buffer, LogThreadContext *context);

  /**
   * Serialize out the record to the log
   * @param task_buffer the task buffer
   */
  void SerializeRecord(const LogRecord &record, LogThreadContext *context);

  /**
   * Serialize out the task buffer to the log
   * @param task_buffer the task buffer
   */
  void SerializeTaskBuffer(IterableBufferSegment<LogRecord> *task_buffer, LogThreadContext *context);
};
}  // namespace terrier::storage
