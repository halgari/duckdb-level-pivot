#pragma once

#include "duckdb/transaction/transaction_manager.hpp"
#include "duckdb/transaction/transaction.hpp"
#include <unordered_set>
#include <string>
#include <string_view>

namespace duckdb {

class LevelPivotSchemaEntry;

class LevelPivotTransaction : public Transaction {
public:
	LevelPivotTransaction(TransactionManager &manager, ClientContext &context);
	~LevelPivotTransaction() override;

	//! Check a key against all tables in the schema and mark matching ones dirty
	void CheckKeyAgainstTables(std::string_view key, LevelPivotSchemaEntry &schema);

	bool HasDirtyTables() const {
		return !dirty_tables_.empty();
	}
	const std::unordered_set<std::string> &GetDirtyTables() const {
		return dirty_tables_;
	}

private:
	std::unordered_set<std::string> dirty_tables_;
	bool all_dirty_ = false;
};

class LevelPivotTransactionManager : public TransactionManager {
public:
	explicit LevelPivotTransactionManager(AttachedDatabase &db);
	~LevelPivotTransactionManager() override;

	Transaction &StartTransaction(ClientContext &context) override;
	ErrorData CommitTransaction(ClientContext &context, Transaction &transaction) override;
	void RollbackTransaction(Transaction &transaction) override;
	void Checkpoint(ClientContext &context, bool force = false) override;

	//! Get the current transaction, or nullptr if none active
	LevelPivotTransaction *GetCurrentTransaction();

private:
	mutex transaction_lock;
	unique_ptr<LevelPivotTransaction> current_transaction;
};

} // namespace duckdb
