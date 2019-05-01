#include "storage/write_ahead_log/log_manager.h"
#include <transaction/transaction_context.h>

namespace terrier::storage {
void LogManager::Process() {
  while (true) {
    RecordBufferSegment *buffer;
    // In a short critical section, try to dequeue an item
    {
      common::SpinLatch::ScopedSpinLatch guard(&flush_queue_latch_);
      if (flush_queue_.empty()) break;
      buffer = flush_queue_.front();
      flush_queue_.pop();
    }

    thread_pool_.SubmitTask([=]() {
      IterableBufferSegment<LogRecord> task_buffer(buffer);
      LogThreadContext *context = GetContext();
      ProcessTaskBuffer(&task_buffer, context);
      buffer_pool_->Release(buffer);
      PushContextToQueue(context);
    });
  }
  thread_pool_.WaitUntilAllFinished();
  FlushAll();
}

void LogManager::ProcessTaskBuffer(IterableBufferSegment<LogRecord> *const task_buffer, LogThreadContext *context) {
  uint32_t size = GetTaskBufferSize(task_buffer);
  if (context->out_.CanBuffer(size)) {
    SerializeTaskBuffer(task_buffer, context);
  } else if (size <= context->out_.GetBufferCapacity()) {
    {
      common::SpinLatch::ScopedSpinLatch guard(&file_latch_);
      context->Flush();
    }
    SerializeTaskBuffer(task_buffer, context);
  } else {
    common::SpinLatch::ScopedSpinLatch guard(&file_latch_);
    SerializeTaskBuffer(task_buffer, context);
    context->Flush();
  }
}

void LogManager::FlushAll() {
  common::SpinLatch::ScopedSpinLatch guard(&contexts_latch_);
  for (auto *context : logging_contexts_queue_) {
    common::SpinLatch::ScopedSpinLatch file_guard(&file_latch_);
    context->Flush();
  }
}

void LogManager::CloseAll() {
  common::SpinLatch::ScopedSpinLatch guard(&contexts_latch_);
  for (auto *context : logging_contexts_queue_) {
    common::SpinLatch::ScopedSpinLatch file_guard(&file_latch_);
    context->out_.Close();
  }
}

void LogManager::SerializeTaskBuffer(IterableBufferSegment<LogRecord> *const task_buffer, LogThreadContext *context) {
  for (LogRecord &record : *task_buffer) {
    if (record.RecordType() == LogRecordType::COMMIT) {
      auto *commit_record = record.GetUnderlyingRecordBodyAs<CommitRecord>();

      // If a transaction is read-only, then the only record it generates is its commit record. This commit record is
      // necessary for the transaction's callback function to be invoked, but there is no need to serialize it, as
      // it corresponds to a transaction with nothing to redo.
      if (!commit_record->IsReadOnly()) SerializeRecord(record, context);
      context->AddCallback(commit_record->Callback(), commit_record->CallbackArg());
      // Not safe to mark read only transactions as the transactions are deallocated preemptively without waiting for
      // logging (there is nothing to log after all)
      if (!commit_record->IsReadOnly()) commit_record->Txn()->log_processed_ = true;
    } else {
      // Any record that is not a commit record is always serialized.`
      SerializeRecord(record, context);
    }
  }
}

uint32_t LogManager::GetTaskBufferSize(IterableBufferSegment<LogRecord> *const task_buffer) {
  uint32_t size = 0;
  for (LogRecord &record : *task_buffer) {
    if (record.RecordType() == LogRecordType::COMMIT) {
      auto *commit_record = record.GetUnderlyingRecordBodyAs<CommitRecord>();
      // Only commit records that are not read only are serialized
      if (!commit_record->IsReadOnly()) size += GetRecordSize(record);
    } else {
      // Any record that is not a commit record is always serialized.`
      size += GetRecordSize(record);
    }
  }
  return size;
}

void LogManager::SerializeRecord(const terrier::storage::LogRecord &record, LogThreadContext *context) {
  // First, serialize out fields common across all LogRecordType's.

  // Note: This is the in-memory size of the log record itself, i.e. inclusive of padding and not considering the size
  // of any potential varlen entries. It is logically different from the size of the serialized record, which the log
  // manager generates in this function. In particular, the later value is very likely to be strictly smaller when the
  // LogRecordType is REDO. On recovery, the goal is to turn the serialized format back into an in-memory log record of
  // this size.
  context->WriteValue(record.Size());

  context->WriteValue(record.RecordType());
  context->WriteValue(record.TxnBegin());

  switch (record.RecordType()) {
    case LogRecordType::REDO: {
      auto *record_body = record.GetUnderlyingRecordBodyAs<RedoRecord>();
      auto *data_table = record_body->GetDataTable();
      context->WriteValue(data_table->TableOid());

      // TODO(Justin): Be careful about how tuple slot is interpreted during real recovery. Right now I think we kind of
      //  sidestep the issue with "bookkeeping".
      context->WriteValue(record_body->GetTupleSlot());

      auto *delta = record_body->Delta();
      // Write out which column ids this redo record is concerned with. On recovery, we can construct the appropriate
      // ProjectedRowInitializer from these ids and their corresponding block layout.
      context->WriteValue(delta->NumColumns());
      context->WriteValue(delta->ColumnIds(), static_cast<uint32_t>(sizeof(col_id_t)) * delta->NumColumns());

      // Write out the null bitmap.
      context->WriteValue(&(delta->Bitmap()), common::RawBitmap::SizeInBytes(delta->NumColumns()));

      // We need the block layout to determine the size of each attribute.
      const auto &block_layout = data_table->GetBlockLayout();
      for (uint16_t i = 0; i < delta->NumColumns(); i++) {
        const auto *column_value_address = delta->AccessWithNullCheck(i);
        if (column_value_address == nullptr) {
          // If the column in this REDO record is null, then there's nothing to serialize out. The bitmap contains all
          // the relevant information.
          continue;
        }
        // Get the column id of the current column in the ProjectedRow.
        col_id_t col_id = delta->ColumnIds()[i];

        if (block_layout.IsVarlen(col_id)) {
          // Inline column value is a pointer to a VarlenEntry, so reinterpret as such.
          const auto *varlen_entry = reinterpret_cast<const VarlenEntry *>(*column_value_address);
          // Serialize out length of the varlen entry.
          context->WriteValue(varlen_entry->Size());
          if (varlen_entry->IsInlined()) {
            // Serialize out the prefix of the varlen entry.
            context->WriteValue(varlen_entry->Prefix(), varlen_entry->Size());
          } else {
            // Serialize out the content field of the varlen entry.
            context->WriteValue(varlen_entry->Content(), varlen_entry->Size());
          }
        } else {
          // Inline column value is the actual data we want to serialize out.
          // Note that by writing out AttrSize(col_id) bytes instead of just the difference between successive offsets
          // of the delta record, we avoid serializing out any potential padding.
          context->WriteValue(column_value_address, block_layout.AttrSize(col_id));
        }
      }
      break;
    }
    case LogRecordType::DELETE: {
      auto *record_body = record.GetUnderlyingRecordBodyAs<DeleteRecord>();
      auto *data_table = record_body->GetDataTable();
      context->WriteValue(data_table->TableOid());
      context->WriteValue(record_body->GetTupleSlot());
      break;
    }
    case LogRecordType::COMMIT:
      context->WriteValue(record.GetUnderlyingRecordBodyAs<CommitRecord>()->CommitTime());
  }
}

uint32_t LogManager::GetRecordSize(const terrier::storage::LogRecord &record) const {
  uint32_t size = 0;

  size += GetValueSize(record.Size());
  size += GetValueSize(record.RecordType());
  size += GetValueSize(record.TxnBegin());

  switch (record.RecordType()) {
    case LogRecordType::REDO: {
      auto *record_body = record.GetUnderlyingRecordBodyAs<RedoRecord>();
      auto *data_table = record_body->GetDataTable();
      size += GetValueSize(data_table->TableOid());
      size += GetValueSize(record_body->GetTupleSlot());

      auto *delta = record_body->Delta();
      // Write out which column ids this redo record is concerned with. On recovery, we can construct the appropriate
      // ProjectedRowInitializer from these ids and their corresponding block layout.
      size += GetValueSize(delta->NumColumns());
      size += static_cast<uint32_t>(sizeof(col_id_t)) * delta->NumColumns();

      // Write out the null bitmap.
      size += common::RawBitmap::SizeInBytes(delta->NumColumns());

      // We need the block layout to determine the size of each attribute.
      const auto &block_layout = data_table->GetBlockLayout();
      for (uint16_t i = 0; i < delta->NumColumns(); i++) {
        const auto *column_value_address = delta->AccessWithNullCheck(i);
        if (column_value_address == nullptr) {
          // If the column in this REDO record is null, then there's nothing to serialize out. The bitmap contains all
          // the relevant information.
          continue;
        }
        // Get the column id of the current column in the ProjectedRow.
        col_id_t col_id = delta->ColumnIds()[i];

        if (block_layout.IsVarlen(col_id)) {
          // Inline column value is a pointer to a VarlenEntry, so reinterpret as such.
          const auto *varlen_entry = reinterpret_cast<const VarlenEntry *>(*column_value_address);
          // Serialize out length of the varlen entry.
          size += GetValueSize(varlen_entry->Size());
          size += varlen_entry->Size();
        } else {
          // Inline column value is the actual data we want to serialize out.
          // Note that by writing out AttrSize(col_id) bytes instead of just the difference between successive offsets
          // of the delta record, we avoid serializing out any potential padding.
          size += block_layout.AttrSize(col_id);
        }
      }
      break;
    }
    case LogRecordType::DELETE: {
      auto *record_body = record.GetUnderlyingRecordBodyAs<DeleteRecord>();
      auto *data_table = record_body->GetDataTable();
      size += GetValueSize(data_table->TableOid());
      size += GetValueSize(record_body->GetTupleSlot());
      break;
    }
    case LogRecordType::COMMIT:
      size += GetValueSize(record.GetUnderlyingRecordBodyAs<CommitRecord>()->CommitTime());
  }

  return size;
}

LogThreadContext *LogManager::GetContext() {
  common::SpinLatch::ScopedSpinLatch guard(&contexts_latch_);
  LogThreadContext *context = logging_contexts_queue_.front();
  logging_contexts_queue_.pop_front();
  return context;
}

void LogManager::PushContextToQueue(LogThreadContext *context) {
  common::SpinLatch::ScopedSpinLatch guard(&contexts_latch_);
  logging_contexts_queue_.push_front(context);
}

}  // namespace terrier::storage
