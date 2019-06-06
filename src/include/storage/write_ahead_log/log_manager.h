#pragma once

#include <memory>
#include <queue>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include "common/container/concurrent_blocking_queue.h"
#include "common/dedicated_thread_owner.h"
#include "common/managed_pointer.h"
#include "common/spin_latch.h"
#include "common/strong_typedef.h"
#include "settings/settings_manager.h"
#include "storage/record_buffer.h"
#include "storage/write_ahead_log/disk_log_writer_task.h"
#include "storage/write_ahead_log/log_io.h"
#include "storage/write_ahead_log/log_record.h"
#include "transaction/transaction_defs.h"

namespace terrier::storage {

// Forward declaration for class DiskLogWriterTask
class DiskLogWriterTask;
class LogSerializerTask;
class LogFlusherTask;

/**
 * Callback functionn and arguments to be called when record is persisted
 */
using CommitCallback = std::pair<transaction::callback_fn, void *>;

/**
 * A BufferedLogWriter containing serialized logs, as well as all commit callbacks for transaction's whose commit are
 * serialized in this BufferedLogWriter
 */
using SerializedLogs = std::pair<BufferedLogWriter *, std::vector<CommitCallback>>;

/**
 * A LogManager is responsible for serializing log records out and keeping track of whether changes from a transaction
 * are persistent. The standard flow of a log record from a transaction all the way to disk is as follows:
 *      1. The LogManager receives buffers containing records from transactions via the AddBufferToFlushQueue, and
 * adds them to a flush queue (flush_queue_)
 *      2. The LogSerializerTask will periodically call Process() to process and serialize buffers in the flush queue
 * and hand them over to the consumer queue (filled_buffer_queue_). The reason this is done offline and not as soon as
 * logs are received is to reduce the amount of time a transaction spends interacting with the log manager
 *      3. When a buffer of logs is handed over to a consumer, the consumer will wake up and process the logs. In the
 * case of the DiskLogWriterTask, this means writing it to the log file.
 *      4. The LogFlusher task will periodically call ForceFlush() to persist the log file to disk using fsync. Once
 * this is done, the commit callback on any persisted logs will be called.
 */
class LogManager : public DedicatedThreadOwner {
 public:
  /**
   * Constructs a new LogManager, writing its logs out to the given file.
   *
   * @param log_file_path path to the desired log file location. If the log file does not exist, one will be created;
   *                      otherwise, changes are appended to the end of the file.
   * @param num_buffers Number of buffers to use for buffering logs
   * @param buffer_pool the object pool to draw log buffers from. This must be the same pool transactions draw their
   *                    buffers from
   */
  LogManager(const char *log_file_path, uint64_t num_buffers, const std::chrono::milliseconds serialization_interval,
             const std::chrono::milliseconds flushing_interval, RecordBufferSegmentPool *const buffer_pool)
      : run_log_manager_(false),
        log_file_path_(log_file_path),
        num_buffers_(num_buffers),
        buffer_pool_(buffer_pool),
        filled_buffer_(nullptr),
        serialization_interval_(serialization_interval),
        flushing_interval_(flushing_interval),
        do_persist_(true) {}

  /**
   * Starts logging
   */
  void Start();

  /**
   * Must be called when no other threads are doing work. Processes and persists all unpersisted logs.
   */
  void Shutdown();

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

  /**
   * For testing only
   * @return number of buffers used for logging
   */
  uint64_t TestGetNumBuffers() { return num_buffers_; }

  /**
   * Set the number of buffers used for buffering logs.
   *
   * The operation fails if the LogManager has already allocated more buffers than the new size
   *
   * @param new_num_buffers the new number of buffers the log manager can use
   * @return true if new_num_buffers is successfully set and false the operation fails
   */
  bool SetNumBuffers(uint64_t new_num_buffers) {
    if (new_num_buffers >= num_buffers_) {
      // Add in new buffers
      for (size_t i = 0; i < new_num_buffers - num_buffers_; i++) {
        buffers_.emplace_back(BufferedLogWriter(log_file_path_));
        empty_buffer_queue_.Enqueue(&buffers_[num_buffers_ + i]);
      }
      num_buffers_ = new_num_buffers;
      return true;
    }
    return false;
  }

  /**
   * Process all the accumulated log records and serialize them to log consumer tasks. This method should only be called
   * from a dedicated logging thread.
   */
  void Process();

  /**
   * Flush the logs to make sure all serialized records before this invocation are persistent. Callbacks from committed
   * transactions are invoked by log consumers when the commit records are persisted on disk. This method should only be
   * called from a dedicated logging thread or during Shutdown
   * @warning Beware the performance consequences of calling flush too frequently
   */
  void ForceFlush();

 private:
  friend class DiskLogWriterTask;

  bool run_log_manager_;

  // Settings Manager
  settings::SettingsManager *settings_manager_;

  // System path for log file
  const char *log_file_path_;

  // Number of buffers to use for buffering logs
  uint64_t num_buffers_;

  // TODO(Tianyu): This can be changed later to be include things that are not necessarily backed by a disk
  //  (e.g. logs can be streamed out to the network for remote replication)
  RecordBufferSegmentPool *buffer_pool_;

  // TODO(Tianyu): Might not be necessary, since commit on txn manager is already protected with a latch
  common::SpinLatch flush_queue_latch_;
  // TODO(Tianyu): benchmark for if these should be concurrent data structures, and if we should apply the same
  //  optimization we applied to the GC queue.
  std::queue<RecordBufferSegment *> flush_queue_;

  // These do not need to be thread safe since the only thread adding or removing from it is the flushing thread
  std::vector<std::pair<transaction::callback_fn, void *>> commits_in_buffer_;

  // This stores all the buffers the serializer or the log consumer threads use
  std::vector<BufferedLogWriter> buffers_;
  // This is the buffer the serializer thread will write to
  BufferedLogWriter *filled_buffer_;
  // The queue containing empty buffers which the serializer thread will use
  common::ConcurrentBlockingQueue<BufferedLogWriter *> empty_buffer_queue_;
  // The queue containing filled buffers pending flush to the disk
  common::ConcurrentBlockingQueue<SerializedLogs> filled_buffer_queue_;

  // Log serializer task that processes buffers handed over by transactions and serializes them into consumer buffers
  const std::chrono::milliseconds serialization_interval_;
  common::ManagedPointer<LogSerializerTask> log_serializer_task_;

  // Log flusher task that periodically forces the DiskLogWriterTask to persist the log file on disk
  const std::chrono::milliseconds flushing_interval_;
  common::ManagedPointer<LogFlusherTask> log_flusher_task_;

  // The log consumer task which flushes filled buffers to the disk
  common::ManagedPointer<DiskLogWriterTask> disk_log_writer_task_ = common::ManagedPointer<DiskLogWriterTask>(nullptr);
  // Flag used by the serializer thread to signal the disk log writer task thread to persist the data on disk
  volatile bool do_persist_;

  // Synchronisation primitives to synchronise persisting buffers to disk
  std::mutex persist_lock_;
  std::condition_variable persist_cv_;
  // Condition variable to signal disk log writer task  thread to wake up and flush buffers to disk or if shutdown has
  // initiated, then quit
  std::condition_variable disk_log_writer_thread_cv_;

  /**
   * Serialize out the record to the log
   * @param record the redo record to serialise
   */
  void SerializeRecord(const LogRecord &record);

  /**
   * Serialize out the task buffer to the log
   * @param buffer_to_serialize the iterator to the redo buffer to be serialized
   */
  void SerializeBuffer(IterableBufferSegment<LogRecord> *buffer_to_serialize);

  /**
   * Used by the serializer thread to get a buffer to serialize data to
   * @return buffer to write to
   */
  BufferedLogWriter *GetCurrentWriteBuffer() {
    if (filled_buffer_ == nullptr) {
      empty_buffer_queue_.Dequeue(&filled_buffer_);
      TERRIER_ASSERT(commits_in_buffer_.empty(), "Commit callbacks should have been handed off to log consumer");
    }
    return filled_buffer_;
  }

  /**
   * Serialize the data pointed to by val to a buffer
   * @tparam T Type of the value
   * @param val The value to write to the buffer
   */
  template <class T>
  void WriteValue(const T &val) {
    WriteValue(&val, sizeof(T));
  }

  /**
   * Serialize the data pointed to by val to a buffer
   * @param val the value
   * @param size size of the value to serialize
   */
  void WriteValue(const void *val, uint32_t size);

  /**
   * Mark the current buffer that the serializer thread is writing to as filled
   */
  void HandFilledBufferToWriter() {
    filled_buffer_queue_.Enqueue(std::make_pair(filled_buffer_, commits_in_buffer_));
    // Signal disk log writer task  thread that a buffer is ready to be flushed to the disk
    {
      std::unique_lock<std::mutex> lock(persist_lock_);
      disk_log_writer_thread_cv_.notify_one();
    }
    // Mark that serializer thread doesn't have a buffer in its possession to which it can write to
    filled_buffer_ = nullptr;
    commits_in_buffer_.clear();
  }

  /**
   * If the central thread registry grants us a thread, we accept if we currently don't have a thread to run the task.
   * Else we don't need the thread, so we respectfully decline
   * @return true if we accepted the thread, else false
   */
  bool OnThreadGranted() override {
    if (GetThreadCount() == 0) {
      // Register disk log writer task
      TERRIER_ASSERT(disk_log_writer_task_ == nullptr, "We should not have a task if we don't own a thread for it yet");
      disk_log_writer_task_ = DedicatedThreadRegistry::GetInstance().RegisterDedicatedThread<DiskLogWriterTask>(
          this /* requester */, this /* argument to task constructor */);
      return true;
    }
    return false;
  }

  /**
   * If the central registry wants to removes our thread used for the disk log writer task, we only allow removal if we
   * are in shut down, else we need to keep the task, so we reject the removal
   * @return true if we allowed thread to be removed, else false
   */
  bool OnThreadRemoved(common::ManagedPointer<DedicatedThreadTask> task) override {
    // We don't want to register a task if the log manager is shutting down though.
    return !run_log_manager_;
  }
};

/**
 * Task that processes buffers handed over by transactions and serializes them into consumer buffers
 */
class LogSerializerTask : public DedicatedThreadTask {
 public:
  explicit LogSerializerTask(LogManager *log_manager, const std::chrono::milliseconds serialization_interval)
      : log_manager_(log_manager), serialization_interval_(serialization_interval), run_task_(false) {}

  /**
   * Runs main disk log writer loop. Called by thread registry upon initialization of thread
   */
  void RunTask() override {
    run_task_ = true;
    LogSerializerTaskLoop();
  }

  /**
   * Signals task to stop. Called by thread registry upon termination of thread
   */
  void Terminate() override { run_task_ = false; }

 private:
  LogManager *log_manager_;
  const std::chrono::milliseconds serialization_interval_;
  bool run_task_;

  /**
   * Main serialization loop. Calls Process on LogManager every interval
   */
  void LogSerializerTaskLoop() {
    while (run_task_) {
      std::this_thread::sleep_for(serialization_interval_);
      log_manager_->Process();
    }
  }
};

/**
 * Task signals disk log consumer to flush logs into disk
 */
class LogFlusherTask : public DedicatedThreadTask {
 public:
  explicit LogFlusherTask(LogManager *log_manager, const std::chrono::milliseconds flushing_interval)
      : log_manager_(log_manager), flushing_interval_(flushing_interval), run_task_(false) {}

  /**
   * Runs main disk log writer loop. Called by thread registry upon initialization of thread
   */
  void RunTask() override {
    run_task_ = true;
    LogFlusherTaskLoop();
  }

  /**
   * Signals task to stop. Called by thread registry upon termination of thread
   */
  void Terminate() override { run_task_ = false; }

 private:
  LogManager *log_manager_;
  const std::chrono::milliseconds flushing_interval_;
  bool run_task_;

  /**
   * Main log flush loop. Calls ForceFlush on LogManager every interval
   */
  void LogFlusherTaskLoop() {
    while (run_task_) {
      std::this_thread::sleep_for(flushing_interval_);
      log_manager_->ForceFlush();
    }
  }
};

}  // namespace terrier::storage
