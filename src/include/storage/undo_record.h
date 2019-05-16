#pragma once
#include <vector>
#include "storage/projected_row.h"
#include "transaction/transaction_defs.h"

namespace terrier::storage {
class DataTable;

/**
 * Struct used for safely accessing the Transaction pointer stored in the Undo Record.
 */
class UndoRecordOwner {
 public:
  /**
   * @return Pointer to the transaction which owns this UndoRecord
   * @warning This can be NULL if the undo record is unlinked or is a compacted record
   */
  transaction::TransactionContext *Get() {
    if (owned_by_ != reinterpret_cast<transaction::TransactionContext *>(uintptr_t(-1))) {
      return owned_by_;
    }
    return nullptr;
  }

  /**
   * Assigns the pointer to the transaction which owns this UndoRecord
   * @param txn pointer to the owner transaction to be assigned to the undo record.
   */
  void Put(transaction::TransactionContext *txn) { owned_by_ = txn; }

  /**
   * Assigns pointer to a special reserved value of -1 used only for records owned by the GC.
   */
  void SetCompacted() { owned_by_ = reinterpret_cast<transaction::TransactionContext *>(-1); }

  /**
   * Returns true if the Undo Record is owned by the GC, false otherwise
   * @return if the Undo Record is owned by the GC
   */
  bool IsOwnedByGC() { return owned_by_ == reinterpret_cast<transaction::TransactionContext *>(-1); }

  /**
   * Returns true if the pointer to the owner is null, false otherwise
   * @return if the pointer to the owner is null
   */
  bool IsNull() { return owned_by_ == nullptr; }

 private:
  transaction::TransactionContext *owned_by_;
};

/**
 * Extension of a ProjectedRow that adds relevant information to be able to traverse the version chain and find the
 * relevant tuple version:
 * pointer to the next record, timestamp of the transaction that created this record, pointer to the data table, and the
 * tuple slot.
 */
class UndoRecord {
 public:
  MEM_REINTERPRETATION_ONLY(UndoRecord)

  /**
   * @return Pointer to the next element in the version chain
   */
  std::atomic<UndoRecord *> &Next() { return next_; }

  /**
   * @return const Pointer to the next element in the version chain
   */
  const std::atomic<UndoRecord *> &Next() const { return next_; }

  /**
   * @return Timestamp up to which the old projected row was visible.
   */
  std::atomic<transaction::timestamp_t> &Timestamp() { return timestamp_; }

  /**
   * @return Timestamp up to which the old projected row was visible.
   */
  const std::atomic<transaction::timestamp_t> &Timestamp() const { return timestamp_; }

  /**
   * @return the type of this undo record
   */
  DeltaRecordType Type() const { return type_; }

  /**
   * @return the DataTable this UndoRecord points to
   * @warning this can be nullptr if it was never installed in a version chain, or unlinked by GC
   */
  DataTable *&Table() { return table_; }

  /**
   * @return the DataTable this UndoRecord points to
   * @warning this can be nullptr if it was never installed in a version chain, or unlinked by GC
   */
  DataTable *Table() const { return table_; }

  /**
   * @return the TupleSlot this UndoRecord points to
   */
  TupleSlot Slot() const { return slot_; }

  /**
   * Access the ProjectedRow containing this record's modifications
   * @return pointer to the delta (modifications)
   */
  ProjectedRow *Delta() { return reinterpret_cast<ProjectedRow *>(varlen_contents_); }

  /**
   * Access the ProjectedRow containing this record's modifications
   * @return const pointer to the delta
   */
  const ProjectedRow *Delta() const { return reinterpret_cast<const ProjectedRow *>(varlen_contents_); }

  /**
   * @return size of this UndoRecord in memory, in bytes.
   */
  uint32_t Size() const {
    return static_cast<uint32_t>(sizeof(UndoRecord) + (type_ == DeltaRecordType::UPDATE ? Delta()->Size() : 0));
  }

  /**
   * @param redo the redo changes to be applied
   * @return size of the UndoRecord which can store the delta resulting from applying redo in memory, in bytes
   */
  static uint32_t Size(const ProjectedRow &redo) { return static_cast<uint32_t>(sizeof(UndoRecord)) + redo.Size(); }

  /**
   * Calculates the size of this UndoRecord, including all members, values, and bitmap
   *
   * @param initializer initializer to use for the embedded ProjectedRow
   * @return number of bytes for this UndoRecord
   */
  static uint32_t Size(const ProjectedRowInitializer &initializer) {
    return static_cast<uint32_t>(sizeof(UndoRecord)) + initializer.ProjectedRowSize();
  }

  /**
   * Populates the UndoRecord to hold an insert.
   *
   * @param head pointer to the byte buffer to initialize as a UndoRecord
   * @param timestamp timestamp of the transaction that generated this UndoRecord
   * @param slot the TupleSlot this UndoRecord points to
   * @param table the DataTable this UndoRecord points to
   * @param txn the transaction this UndoRecord belongs to
   * @return pointer to the initialized UndoRecord
   */
  static UndoRecord *InitializeInsert(byte *const head, const transaction::timestamp_t timestamp, const TupleSlot slot,
                                      DataTable *const table, transaction::TransactionContext *const txn) {
    auto *result = reinterpret_cast<UndoRecord *>(head);
    result->type_ = DeltaRecordType::INSERT;
    result->next_ = nullptr;
    result->timestamp_.store(timestamp);
    result->table_ = table;
    result->slot_ = slot;
    result->txnptr_.Put(txn);
    return result;
  }

  /**
   * Populates the UndoRecord to hold a delete.
   *
   * @param head pointer to the byte buffer to initialize as a UndoRecord
   * @param timestamp timestamp of the transaction that generated this UndoRecord
   * @param slot the TupleSlot this UndoRecord points to
   * @param table the DataTable this UndoRecord points to
   * @param txn the transaction this UndoRecord belongs to
   * @return pointer to the initialized UndoRecord
   */
  static UndoRecord *InitializeDelete(byte *const head, const transaction::timestamp_t timestamp, const TupleSlot slot,
                                      DataTable *const table, transaction::TransactionContext *const txn) {
    auto *result = reinterpret_cast<UndoRecord *>(head);
    result->type_ = DeltaRecordType::DELETE;
    result->next_ = nullptr;
    result->timestamp_.store(timestamp);
    result->table_ = table;
    result->slot_ = slot;
    result->txnptr_.Put(txn);
    return result;
  }

  /**
   * Populates the UndoRecord's members to hold an update.
   *
   * @param head pointer to the byte buffer to initialize as a UndoRecord
   * @param timestamp timestamp of the transaction that generated this UndoRecord
   * @param slot the TupleSlot this UndoRecord points to
   * @param table the DataTable this UndoRecord points to
   * @param txn the transaction this UndoRecord belongs to
   * @param initializer the initializer to use for the embedded ProjectedRow
   * @return pointer to the initialized UndoRecord
   */
  static UndoRecord *InitializeUpdate(byte *const head, const transaction::timestamp_t timestamp, const TupleSlot slot,
                                      DataTable *const table, transaction::TransactionContext *const txn,
                                      const ProjectedRowInitializer &initializer) {
    auto *result = reinterpret_cast<UndoRecord *>(head);

    result->type_ = DeltaRecordType::UPDATE;
    result->next_ = nullptr;
    result->timestamp_.store(timestamp);
    result->table_ = table;
    result->slot_ = slot;
    result->txnptr_.Put(txn);

    initializer.InitializeRow(result->varlen_contents_);

    return result;
  }

  /**
   * Populates the UndoRecord's members to hold an update based on the redo changes that this UndoRecord is supposed to
   * log.
   *
   * @param head pointer to the byte buffer to initialize as a UndoRecord
   * @param timestamp timestamp of the transaction that generated this UndoRecord
   * @param slot the TupleSlot this UndoRecord points to
   * @param table the DataTable this UndoRecord points to
   * @param txn the transaction this UndoRecord belongs to
   * @param redo the redo changes to be applied
   * @return pointer to the initialized UndoRecord
   */
  static UndoRecord *InitializeUpdate(byte *const head, const transaction::timestamp_t timestamp, const TupleSlot slot,
                                      DataTable *const table, transaction::TransactionContext *const txn,
                                      const storage::ProjectedRow &redo) {
    auto *result = reinterpret_cast<UndoRecord *>(head);

    result->type_ = DeltaRecordType::UPDATE;
    result->next_ = nullptr;
    result->timestamp_.store(timestamp);
    result->table_ = table;
    result->slot_ = slot;
    result->txnptr_.Put(txn);

    ProjectedRow::CopyProjectedRowLayout(result->varlen_contents_, redo);

    return result;
  }

 private:
  friend class GarbageCollector;
  DeltaRecordType type_;
  std::atomic<UndoRecord *> next_;
  std::atomic<transaction::timestamp_t> timestamp_;
  DataTable *table_;
  TupleSlot slot_;
  UndoRecordOwner txnptr_;
  // This needs to be aligned to 8 bytes to ensure the real size of UndoRecord (plus actual ProjectedRow) is also
  // a multiple of 8.
  uint64_t varlen_contents_[0];
};

static_assert(sizeof(UndoRecord) % 8 == 0,
              "a projected row inside the undo record needs to be aligned to 8 bytes"
              "to ensure true atomicity");

}  // namespace terrier::storage
