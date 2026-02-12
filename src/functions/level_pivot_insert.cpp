#include "level_pivot_insert.hpp"
#include "level_pivot_table_entry.hpp"
#include "key_parser.hpp"
#include "level_pivot_storage.hpp"
#include <unordered_map>

namespace duckdb {

struct LevelPivotInsertGlobalState : public GlobalSinkState {
	idx_t insert_count = 0;
};

LevelPivotInsert::LevelPivotInsert(PhysicalPlan &plan, vector<LogicalType> types, TableCatalogEntry &table,
                                   idx_t estimated_cardinality)
    : PhysicalOperator(plan, PhysicalOperatorType::EXTENSION, std::move(types), estimated_cardinality), table(table) {
}

unique_ptr<GlobalSinkState> LevelPivotInsert::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<LevelPivotInsertGlobalState>();
}

SinkResultType LevelPivotInsert::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<LevelPivotInsertGlobalState>();
	auto &lp_table = table.Cast<LevelPivotTableEntry>();
	auto &connection = *lp_table.GetConnection();

	if (lp_table.GetTableMode() == LevelPivotTableMode::PIVOT) {
		auto &parser = lp_table.GetKeyParser();
		auto &identity_cols = lp_table.GetIdentityColumns();
		auto &attr_cols = lp_table.GetAttrColumns();
		auto &columns = lp_table.GetColumns();

		auto batch = connection.create_batch();

		// Pre-build column name -> chunk index map (O(1) lookups in the row loop)
		std::unordered_map<std::string, idx_t> col_index_map;
		for (auto &col : columns.Logical()) {
			col_index_map[col.Name()] = col.Logical().index;
		}

		for (idx_t row = 0; row < chunk.size(); row++) {
			// Extract identity values in capture order
			std::vector<std::string> identity_values;
			auto &capture_names = parser.pattern().capture_names();
			for (auto &cap_name : capture_names) {
				auto it2 = col_index_map.find(cap_name);
				if (it2 != col_index_map.end()) {
					auto val = chunk.data[it2->second].GetValue(row);
					if (val.IsNull()) {
						throw InvalidInputException("Cannot insert NULL into identity column '%s'", cap_name);
					}
					identity_values.push_back(val.ToString());
				}
			}

			// Write a key for each non-null attr column
			for (auto &attr_name : attr_cols) {
				auto it2 = col_index_map.find(attr_name);
				if (it2 != col_index_map.end()) {
					auto val = chunk.data[it2->second].GetValue(row);
					if (!val.IsNull()) {
						std::string key = parser.build(identity_values, attr_name);
						batch.put(key, val.ToString());
					}
				}
			}
		}

		batch.commit();
		gstate.insert_count += chunk.size();
	} else {
		// Raw mode: column 0 = key, column 1 = value
		auto &prefix = lp_table.GetRawKeyPrefix();
		auto batch = connection.create_batch();
		for (idx_t row = 0; row < chunk.size(); row++) {
			auto key_val = chunk.data[0].GetValue(row);
			auto val_val = chunk.data[1].GetValue(row);
			if (key_val.IsNull()) {
				throw InvalidInputException("Cannot insert NULL key in raw mode");
			}
			batch.put(prefix + key_val.ToString(), val_val.IsNull() ? "" : val_val.ToString());
		}
		batch.commit();
		gstate.insert_count += chunk.size();
	}

	return SinkResultType::NEED_MORE_INPUT;
}

SinkFinalizeType LevelPivotInsert::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                            OperatorSinkFinalizeInput &input) const {
	return SinkFinalizeType::READY;
}

SourceResultType LevelPivotInsert::GetData(ExecutionContext &context, DataChunk &chunk,
                                           OperatorSourceInput &input) const {
	auto &gstate = sink_state->Cast<LevelPivotInsertGlobalState>();
	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(static_cast<int64_t>(gstate.insert_count)));
	return SourceResultType::FINISHED;
}

} // namespace duckdb
