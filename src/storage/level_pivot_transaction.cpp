#include "level_pivot_transaction.hpp"

namespace duckdb {

// --- LevelPivotTransaction ---

LevelPivotTransaction::LevelPivotTransaction(TransactionManager &manager, ClientContext &context)
    : Transaction(manager, context) {
}

LevelPivotTransaction::~LevelPivotTransaction() = default;

// --- LevelPivotTransactionManager ---

LevelPivotTransactionManager::LevelPivotTransactionManager(AttachedDatabase &db) : TransactionManager(db) {
}

LevelPivotTransactionManager::~LevelPivotTransactionManager() = default;

Transaction &LevelPivotTransactionManager::StartTransaction(ClientContext &context) {
	lock_guard<mutex> l(transaction_lock);
	current_transaction = make_uniq<LevelPivotTransaction>(*this, context);
	return *current_transaction;
}

ErrorData LevelPivotTransactionManager::CommitTransaction(ClientContext &context, Transaction &transaction) {
	return ErrorData();
}

void LevelPivotTransactionManager::RollbackTransaction(Transaction &transaction) {
}

void LevelPivotTransactionManager::Checkpoint(ClientContext &context, bool force) {
}

} // namespace duckdb
