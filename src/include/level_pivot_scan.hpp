#pragma once

#include "duckdb/function/table_function.hpp"

namespace duckdb {

class LevelPivotTableEntry;

struct LevelPivotScanData : public TableFunctionData {
	LevelPivotTableEntry *table_entry;
};

struct LevelPivotScanGlobalState : public GlobalTableFunctionState {
	explicit LevelPivotScanGlobalState();
	idx_t MaxThreads() const override {
		return 1;
	}
	bool done = false;
	vector<column_t> column_ids;
};

TableFunction LevelPivotScanFunction();

} // namespace duckdb
