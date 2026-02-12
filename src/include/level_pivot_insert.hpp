#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

namespace duckdb {

class LevelPivotTableEntry;

class LevelPivotInsert : public PhysicalOperator {
public:
	LevelPivotInsert(PhysicalPlan &plan, vector<LogicalType> types, TableCatalogEntry &table,
	                 idx_t estimated_cardinality);

	TableCatalogEntry &table;

	// --- Sink interface ---
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;

	bool IsSink() const override {
		return true;
	}
	bool ParallelSink() const override {
		return false;
	}

	// --- Source interface ---
	bool IsSource() const override {
		return true;
	}
	// DuckDB API compatibility: In v1.4.4, GetData is the virtual override point.
	// In later versions, GetData became non-virtual and GetDataInternal is the
	// protected virtual override point. We define both (without `override`) so the
	// code compiles with either version. GetData forwards to GetDataInternal.
	// NOLINTNEXTLINE(modernize-use-override, clang-diagnostic-inconsistent-missing-override)
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const;
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const;
};

} // namespace duckdb
