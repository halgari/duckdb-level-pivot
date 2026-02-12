#pragma once

#include "duckdb/transaction/transaction_manager.hpp"
#include "duckdb/transaction/transaction.hpp"

namespace duckdb {

class LevelPivotTransaction : public Transaction {
public:
	LevelPivotTransaction(TransactionManager &manager, ClientContext &context);
	~LevelPivotTransaction() override;
};

class LevelPivotTransactionManager : public TransactionManager {
public:
	explicit LevelPivotTransactionManager(AttachedDatabase &db);
	~LevelPivotTransactionManager() override;

	Transaction &StartTransaction(ClientContext &context) override;
	ErrorData CommitTransaction(ClientContext &context, Transaction &transaction) override;
	void RollbackTransaction(Transaction &transaction) override;
	void Checkpoint(ClientContext &context, bool force = false) override;

private:
	mutex transaction_lock;
	unique_ptr<LevelPivotTransaction> current_transaction;
};

} // namespace duckdb
