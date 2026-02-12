#include "level_pivot_transaction.hpp"
#include "level_pivot_schema.hpp"
#include "level_pivot_table_entry.hpp"

namespace duckdb {

// --- LevelPivotTransaction ---

LevelPivotTransaction::LevelPivotTransaction(TransactionManager &manager, ClientContext &context)
    : Transaction(manager, context) {
}

LevelPivotTransaction::~LevelPivotTransaction() = default;

void LevelPivotTransaction::CheckKeyAgainstTables(std::string_view key, LevelPivotSchemaEntry &schema) {
	if (all_dirty_) {
		return;
	}

	idx_t total_tables = 0;
	schema.Scan(CatalogType::TABLE_ENTRY, [&](CatalogEntry &entry) {
		total_tables++;
		auto &table = entry.Cast<LevelPivotTableEntry>();
		const auto &table_name = table.name;

		// Skip tables already known dirty
		if (dirty_tables_.count(table_name)) {
			return;
		}

		if (table.GetTableMode() == LevelPivotTableMode::RAW) {
			// Raw tables are always affected by any write
			dirty_tables_.insert(table_name);
		} else {
			// Pivot table: fast prefix check, then full parse
			auto &parser = table.GetKeyParser();
			auto &prefix = parser.pattern().literal_prefix();
			if (!prefix.empty() && key.compare(0, prefix.size(), prefix) != 0) {
				return;
			}
			if (parser.parse_view(key).has_value()) {
				dirty_tables_.insert(table_name);
			}
		}
	});

	if (dirty_tables_.size() >= total_tables) {
		all_dirty_ = true;
	}
}

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
	lock_guard<mutex> l(transaction_lock);
	current_transaction.reset();
	return ErrorData();
}

void LevelPivotTransactionManager::RollbackTransaction(Transaction &transaction) {
	lock_guard<mutex> l(transaction_lock);
	current_transaction.reset();
}

void LevelPivotTransactionManager::Checkpoint(ClientContext &context, bool force) {
}

LevelPivotTransaction *LevelPivotTransactionManager::GetCurrentTransaction() {
	lock_guard<mutex> l(transaction_lock);
	return current_transaction.get();
}

} // namespace duckdb
