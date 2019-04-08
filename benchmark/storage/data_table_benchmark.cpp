#include <memory>
#include <vector>

#include "benchmark/benchmark.h"
#include "common/strong_typedef.h"
#include "storage/data_table.h"
#include "storage/garbage_collector.h"
#include "storage/storage_util.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_manager.h"
#include "util/multithread_test_util.h"
#include "util/storage_test_util.h"
#include "util/transaction_test_util.h"
#include "common/scoped_timer.h"

namespace terrier {

// This benchmark simulates a key-value store inserting a large number of tuples. This provides a good baseline and
// reference to other fast data structures (indexes) to compare against. We are interested in the DataTable's raw
// performance, so the tuple's contents are intentionally left garbage and we don't verify correctness. That's the job
// of the Google Tests.

class DataTableBenchmark : public benchmark::Fixture {
 public:
  void SetUp(const benchmark::State &state) final {
    // generate a random redo ProjectedRow to Insert
    redo_buffer_ = common::AllocationUtil::AllocateAligned(initializer_.ProjectedRowSize());
    redo_ = initializer_.InitializeRow(redo_buffer_);
    StorageTestUtil::PopulateRandomRow(redo_, layout_, 0, &generator_);

    // generate a ProjectedRow buffer to Read
    read_buffer_ = common::AllocationUtil::AllocateAligned(initializer_.ProjectedRowSize());
    read_ = initializer_.InitializeRow(read_buffer_);

    // generate a vector of ProjectedRow buffers for concurrent reads
    for (uint32_t i = 0; i < num_threads_; ++i) {
      // Create read buffer
      byte *read_buffer = common::AllocationUtil::AllocateAligned(initializer_.ProjectedRowSize());
      storage::ProjectedRow *read = initializer_.InitializeRow(read_buffer);
      read_buffers_.emplace_back(read_buffer);
      reads_.emplace_back(read);
    }
    version_chain_length_traversed = 0;
  }

  void TearDown(const benchmark::State &state) final {
    delete[] redo_buffer_;
    delete[] read_buffer_;
    for (uint32_t i = 0; i < num_threads_; ++i) delete[] read_buffers_[i];
    // google benchmark might run benchmark several iterations. We need to clear vectors.
    read_buffers_.clear();
    reads_.clear();
  }

  void StartGC(transaction::TransactionManager *const txn_manager) {
    gc_ = new storage::GarbageCollector(txn_manager);
    run_gc_ = true;
    gc_thread_ = std::thread([this] { GCThreadLoop(); });
  }

  void EndGC() {
    run_gc_ = false;
    gc_thread_.join();
    // Make sure all garbage is collected. This take 2 runs for unlink and deallocate
    gc_->PerformGarbageCollection();
    gc_->PerformGarbageCollection();
    delete gc_;
  }

  std::thread gc_thread_;
  storage::GarbageCollector *gc_ = nullptr;
  volatile bool run_gc_ = false;
  const std::chrono::milliseconds gc_period_{10};

  void GCThreadLoop() {
    while (run_gc_) {
      std::this_thread::sleep_for(gc_period_);
      gc_->PerformGarbageCollection();
    }
  }

  // Tuple layout
  const uint8_t column_size_ = 8;
  const storage::BlockLayout layout_{{column_size_, column_size_, column_size_}};

  // Tuple properties
  const storage::ProjectedRowInitializer initializer_{layout_, StorageTestUtil::ProjectionListAllColumns(layout_)};

  // Workload
  const uint32_t num_inserts_ = 10000000;
  const uint32_t num_reads_ = 10000000;
  const uint32_t num_threads_ = 4;
  const uint64_t buffer_pool_reuse_limit_ = 10000000;

  // Test infrastructure
  std::default_random_engine generator_;
  storage::BlockStore block_store_{1000, 1000};
  storage::RecordBufferSegmentPool buffer_pool_{num_inserts_, buffer_pool_reuse_limit_};

  // Version Chain Length
  uint32_t version_chain_length_traversed;

  // Insert buffer pointers
  byte *redo_buffer_;
  storage::ProjectedRow *redo_;

  // Read buffer pointers;
  byte *read_buffer_;
  storage::ProjectedRow *read_;

  // Read buffers pointers for concurrent reads
  std::vector<byte *> read_buffers_;
  std::vector<storage::ProjectedRow *> reads_;
};

// Insert the num_inserts_ of tuples into a DataTable in a single thread
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(DataTableBenchmark, SimpleInsert)(benchmark::State &state) {
  // NOLINTNEXTLINE
  for (auto _ : state) {
    storage::DataTable table(&block_store_, layout_, storage::layout_version_t(0));
    // We can use dummy timestamps here since we're not invoking concurrency control
    transaction::TransactionContext txn(transaction::timestamp_t(0), transaction::timestamp_t(0), &buffer_pool_,
                                        LOGGING_DISABLED);
    for (uint32_t i = 0; i < num_inserts_; ++i) {
      table.Insert(&txn, *redo_);
    }
  }

  state.SetItemsProcessed(state.iterations() * num_inserts_);
}

// Insert the num_inserts_ of tuples into a DataTable concurrently
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(DataTableBenchmark, ConcurrentInsert)(benchmark::State &state) {
  // NOLINTNEXTLINE
  for (auto _ : state) {
    storage::DataTable table(&block_store_, layout_, storage::layout_version_t(0));
    auto workload = [&](uint32_t id) {
      // We can use dummy timestamps here since we're not invoking concurrency control
      transaction::TransactionContext txn(transaction::timestamp_t(0), transaction::timestamp_t(0), &buffer_pool_,
                                          LOGGING_DISABLED);
      for (uint32_t i = 0; i < num_inserts_ / num_threads_; i++) table.Insert(&txn, *redo_);
    };
    common::WorkerPool thread_pool(num_threads_, {});
    MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads_, workload);
  }

  state.SetItemsProcessed(state.iterations() * num_inserts_);
}

// Read the num_reads_ of tuples in a sequential order from a DataTable in a single thread
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(DataTableBenchmark, SequentialRead)(benchmark::State &state) {
  storage::DataTable read_table(&block_store_, layout_, storage::layout_version_t(0));
  // Populate read_table by inserting tuples
  // We can use dummy timestamps here since we're not invoking concurrency control
  transaction::TransactionContext txn(transaction::timestamp_t(0), transaction::timestamp_t(0), &buffer_pool_,
                                      LOGGING_DISABLED);
  std::vector<storage::TupleSlot> read_order;
  for (uint32_t i = 0; i < num_reads_; ++i) {
    read_order.emplace_back(read_table.Insert(&txn, *redo_));
  }
  // NOLINTNEXTLINE
  for (auto _ : state) {
    for (uint32_t i = 0; i < num_reads_; ++i) {
      read_table.Select(&txn, read_order[i], read_);
    }
  }

  state.SetItemsProcessed(state.iterations() * num_reads_);
}

// Read the num_reads_ of tuples in a random order from a DataTable in a single thread
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(DataTableBenchmark, RandomRead)(benchmark::State &state) {
  storage::DataTable read_table(&block_store_, layout_, storage::layout_version_t(0));
  // Populate read_table_ by inserting tuples
  // We can use dummy timestamps here since we're not invoking concurrency control
  transaction::TransactionContext txn(transaction::timestamp_t(0), transaction::timestamp_t(0), &buffer_pool_,
                                      LOGGING_DISABLED);
  std::vector<storage::TupleSlot> read_order;
  for (uint32_t i = 0; i < num_reads_; ++i) {
    read_order.emplace_back(read_table.Insert(&txn, *redo_));
  }
  // Create random reads
  std::shuffle(read_order.begin(), read_order.end(), generator_);
  // NOLINTNEXTLINE
  for (auto _ : state) {
    for (uint32_t i = 0; i < num_reads_; ++i) {
      read_table.Select(&txn, read_order[i], read_);
    }
  }

  state.SetItemsProcessed(state.iterations() * num_reads_);
}

// Read the num_reads_ of tuples in a random order from a DataTable concurrently
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(DataTableBenchmark, ConcurrentRandomRead)(benchmark::State &state) {
  storage::DataTable read_table(&block_store_, layout_, storage::layout_version_t(0));

  // populate read_table_ by inserting tuples
  // We can use dummy timestamps here since we're not invoking concurrency control
  transaction::TransactionContext txn(transaction::timestamp_t(0), transaction::timestamp_t(0), &buffer_pool_,
                                      LOGGING_DISABLED);
  std::vector<storage::TupleSlot> read_order;
  for (uint32_t i = 0; i < num_reads_; ++i) {
    read_order.emplace_back(read_table.Insert(&txn, *redo_));
  }
  // Generate random read orders and read buffer for each thread
  std::shuffle(read_order.begin(), read_order.end(), generator_);
  std::uniform_int_distribution<uint32_t> rand_start(0, static_cast<uint32_t>(read_order.size() - 1));
  std::vector<uint32_t> rand_read_offsets;
  for (uint32_t i = 0; i < num_threads_; ++i) {
    // Create random reads
    rand_read_offsets.emplace_back(rand_start(generator_));
  }
  // NOLINTNEXTLINE
  for (auto _ : state) {
    auto workload = [&](uint32_t id) {
      // We can use dummy timestamps here since we're not invoking concurrency control
      transaction::TransactionContext txn(transaction::timestamp_t(0), transaction::timestamp_t(0), &buffer_pool_,
                                          LOGGING_DISABLED);
      for (uint32_t i = 0; i < num_reads_ / num_threads_; i++)
        read_table.Select(&txn, read_order[(rand_read_offsets[id] + i) % read_order.size()], reads_[id]);
    };
    common::WorkerPool thread_pool(num_threads_, {});
    MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads_, workload);
  }

  state.SetItemsProcessed(state.iterations() * num_reads_);
}

// Read the num_reads_ of tuples in a sequential order from a DataTable in a single thread
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(DataTableBenchmark, LongUpdatesandRead)(benchmark::State &state) {
  storage::DataTable read_table(&block_store_, layout_, storage::layout_version_t(0));
  // Populate read_table_ by inserting tuples
  // We can use dummy timestamps here since we're not invoking concurrency control
  transaction::TransactionManager txn_manager(&buffer_pool_, true, LOGGING_DISABLED);
//  transaction::TransactionContext olap_txn(transaction::timestamp_t(0), transaction::timestamp_t(0), &buffer_pool_,
//                                      LOGGING_DISABLED);
//  transaction::TransactionContext txn(transaction::timestamp_t(1), transaction::timestamp_t(1), &buffer_pool_,
//                                      LOGGING_DISABLED);

  transaction::TransactionContext *insert_txn = txn_manager.BeginTransaction();

  std::vector<storage::TupleSlot> read_order;
  std::vector<storage::TupleSlot> hotspot;
  const uint32_t num_inserts = 100000;
  const uint32_t num_updates_per_iteration = 1000;
  const uint64_t num_iterations = 5;
  const uint64_t num_hotspot = 1000;

  uint64_t average_version_length = 0;
  for (uint32_t i = 0; i < num_inserts; ++i) {
    auto slot = read_table.Insert(insert_txn, *redo_);
    if (i < num_hotspot) {
      hotspot.emplace_back(slot);
    }
    read_order.emplace_back(slot);
  }
  txn_manager.Commit(insert_txn, TestCallbacks::EmptyCallback, nullptr);

  transaction::TransactionContext *olap_txn = txn_manager.BeginTransaction();

  StartGC(&txn_manager);
  for (auto _ : state) {
    for (uint32_t i = 0; i < num_iterations; ++i) {
      uint64_t elapsed_ms;
      {
        common::ScopedTimer timer(&elapsed_ms);
        // Scan the entire table
        for (uint32_t j = 0; j < num_inserts; ++j) {
          read_table.Select(olap_txn, read_order[j], read_, &version_chain_length_traversed);
          if ( j < num_hotspot) {
            average_version_length += version_chain_length_traversed;
          }
        }
        average_version_length /= num_hotspot;
      }
      printf("Average version chain length this scan: %llu Time(in ms): %llu\n", average_version_length, elapsed_ms);
      state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
      // Update the hotspot
      for (uint32_t j = 0; j < num_updates_per_iteration; ++j) {
        transaction::TransactionContext *txn = txn_manager.BeginTransaction();
        for (uint32_t k = 0; k < num_hotspot; ++k) {
          read_table.Update(txn, hotspot[k], *redo_);
        }
        txn_manager.Commit(txn, TestCallbacks::EmptyCallback, nullptr);
      }
    }
  }
  EndGC();
  state.SetItemsProcessed(state.iterations() * num_reads_);
}

//BENCHMARK_REGISTER_F(DataTableBenchmark, SimpleInsert)->Unit(benchmark::kMillisecond);
//
//BENCHMARK_REGISTER_F(DataTableBenchmark, ConcurrentInsert)->Unit(benchmark::kMillisecond)->UseRealTime();
//
//BENCHMARK_REGISTER_F(DataTableBenchmark, SequentialRead)->Unit(benchmark::kMillisecond);
//
//BENCHMARK_REGISTER_F(DataTableBenchmark, RandomRead)->Unit(benchmark::kMillisecond);
//
//BENCHMARK_REGISTER_F(DataTableBenchmark, ConcurrentRandomRead)->Unit(benchmark::kMillisecond)->UseRealTime();

BENCHMARK_REGISTER_F(DataTableBenchmark, LongUpdatesandRead)->Unit(benchmark::kMillisecond);
}  // namespace terrier
