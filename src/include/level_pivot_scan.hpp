#pragma once

#include "duckdb/function/table_function.hpp"

namespace duckdb {

class LevelPivotTableEntry;

struct LevelPivotScanData : public TableFunctionData {
	LevelPivotTableEntry *table_entry;
	string filter_prefix; // Narrowed prefix from pushdown_complex_filter (empty = use default)

	unique_ptr<FunctionData> Copy() const override {
		auto copy = make_uniq<LevelPivotScanData>();
		copy->table_entry = table_entry;
		copy->filter_prefix = filter_prefix;
		return std::move(copy);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<LevelPivotScanData>();
		return table_entry == other.table_entry && filter_prefix == other.filter_prefix;
	}

	bool SupportStatementCache() const override {
		return false;
	}
};

struct LevelPivotScanGlobalState : public GlobalTableFunctionState {
	explicit LevelPivotScanGlobalState();
	idx_t MaxThreads() const override {
		return 1;
	}
	bool done = false;
	vector<column_t> column_ids;
	string filter_prefix; // Narrowed prefix from filter pushdown (empty = use default)
};

TableFunction LevelPivotScanFunction();

} // namespace duckdb
