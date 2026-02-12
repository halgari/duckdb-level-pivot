#include "level_pivot_update.hpp"
#include "level_pivot_table_entry.hpp"
#include "level_pivot_catalog.hpp"
#include "level_pivot_transaction.hpp"
#include "level_pivot_utils.hpp"
#include "key_parser.hpp"
#include "level_pivot_storage.hpp"

namespace duckdb {

struct LevelPivotUpdateGlobalState : public GlobalSinkState {
	idx_t update_count = 0;
};

LevelPivotUpdate::LevelPivotUpdate(PhysicalPlan &plan, vector<LogicalType> types, TableCatalogEntry &table,
                                   vector<PhysicalIndex> columns, idx_t estimated_cardinality)
    : PhysicalOperator(plan, PhysicalOperatorType::EXTENSION, std::move(types), estimated_cardinality), table(table),
      columns(std::move(columns)) {
}

unique_ptr<GlobalSinkState> LevelPivotUpdate::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<LevelPivotUpdateGlobalState>();
}

SinkResultType LevelPivotUpdate::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<LevelPivotUpdateGlobalState>();
	auto &lp_table = table.Cast<LevelPivotTableEntry>();
	auto &connection = *lp_table.GetConnection();
	auto &catalog = lp_table.ParentCatalog().Cast<LevelPivotCatalog>();
	auto &txn = Transaction::Get(context.client, catalog).Cast<LevelPivotTransaction>();
	auto &schema = catalog.GetMainSchema();

	if (lp_table.GetTableMode() == LevelPivotTableMode::PIVOT) {
		auto &parser = lp_table.GetKeyParser();
		auto &table_columns = lp_table.GetColumns();
		auto &identity_cols = lp_table.GetIdentityColumns();
		auto row_id_cols = lp_table.GetRowIdColumns();
		auto batch = connection.create_batch();

		// The child chunk layout (from DuckDB's update projection):
		// [update_col_0, update_col_1, ..., row_id_col_0, row_id_col_1, ...]
		// Row ID columns are at the END of the chunk
		idx_t num_update_cols = this->columns.size();
		idx_t num_row_id_cols = row_id_cols.size();
		idx_t row_id_offset = chunk.ColumnCount() - num_row_id_cols;

		for (idx_t row = 0; row < chunk.size(); row++) {
			// Extract identity from row_id columns (at end of chunk)
			auto identity_values = ExtractIdentityValues(chunk, row, row_id_offset, num_row_id_cols);

			// Process each updated column (at beginning of chunk)
			for (idx_t i = 0; i < num_update_cols; i++) {
				auto physical_idx = this->columns[i].index;
				auto &col = table_columns.GetColumn(PhysicalIndex(physical_idx));
				auto &col_name = col.Name();

				// Get the new value from the chunk
				auto new_val = chunk.data[i].GetValue(row);

				// Check if this is an identity column
				bool is_identity = false;
				for (auto &id_col : identity_cols) {
					if (id_col == col_name) {
						is_identity = true;
						break;
					}
				}

				if (is_identity) {
					throw NotImplementedException("Updating identity columns is not yet supported");
				}

				// It's an attr column - update the specific key
				std::string key = parser.build(identity_values, col_name);
				if (new_val.IsNull()) {
					batch.del(key);
				} else {
					batch.put(key, new_val.ToString());
				}
				txn.CheckKeyAgainstTables(key, schema);
			}
		}

		batch.commit();
		gstate.update_count += chunk.size();
	} else {
		// Raw mode: chunk layout is [update_value, row_id_key]
		auto batch = connection.create_batch();
		idx_t key_col_idx = chunk.ColumnCount() - 1;
		for (idx_t row = 0; row < chunk.size(); row++) {
			auto key_val = chunk.data[key_col_idx].GetValue(row);
			if (key_val.IsNull()) {
				continue;
			}
			auto val = chunk.data[0].GetValue(row);
			std::string key = key_val.ToString();
			batch.put(key, val.IsNull() ? "" : val.ToString());
			txn.CheckKeyAgainstTables(key, schema);
		}
		batch.commit();
		gstate.update_count += chunk.size();
	}

	return SinkResultType::NEED_MORE_INPUT;
}

SinkFinalizeType LevelPivotUpdate::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                            OperatorSinkFinalizeInput &input) const {
	return SinkFinalizeType::READY;
}

SourceResultType LevelPivotUpdate::GetData(ExecutionContext &context, DataChunk &chunk,
                                           OperatorSourceInput &input) const {
	auto &gstate = sink_state->Cast<LevelPivotUpdateGlobalState>();
	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(static_cast<int64_t>(gstate.update_count)));
	return SourceResultType::FINISHED;
}

} // namespace duckdb
