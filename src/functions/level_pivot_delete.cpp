#include "level_pivot_delete.hpp"
#include "level_pivot_table_entry.hpp"
#include "key_parser.hpp"
#include "level_pivot_storage.hpp"

namespace duckdb {

struct LevelPivotDeleteGlobalState : public GlobalSinkState {
	idx_t delete_count = 0;
};

LevelPivotDelete::LevelPivotDelete(PhysicalPlan &plan, vector<LogicalType> types, TableCatalogEntry &table,
                                   idx_t estimated_cardinality)
    : PhysicalOperator(plan, PhysicalOperatorType::EXTENSION, std::move(types), estimated_cardinality), table(table) {
}

unique_ptr<GlobalSinkState> LevelPivotDelete::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<LevelPivotDeleteGlobalState>();
}

SinkResultType LevelPivotDelete::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<LevelPivotDeleteGlobalState>();
	auto &lp_table = table.Cast<LevelPivotTableEntry>();
	auto &connection = *lp_table.GetConnection();

	if (lp_table.GetTableMode() == LevelPivotTableMode::PIVOT) {
		auto &parser = lp_table.GetKeyParser();
		auto batch = connection.create_batch();

		for (idx_t row = 0; row < chunk.size(); row++) {
			// The child plan emits the identity columns (from GetRowIdColumns)
			// Extract identity values from the incoming chunk
			std::vector<std::string> identity_values;
			for (idx_t col = 0; col < chunk.ColumnCount(); col++) {
				auto val = chunk.data[col].GetValue(row);
				identity_values.push_back(val.IsNull() ? "" : val.ToString());
			}

			// Find all keys matching this identity and delete them
			std::string prefix = parser.build_prefix(identity_values);
			auto iter = connection.iterator();
			if (prefix.empty()) {
				iter.seek_to_first();
			} else {
				iter.seek(prefix);
			}

			while (iter.valid()) {
				std::string_view key_sv = iter.key_view();
				if (!prefix.empty() && (key_sv.size() < prefix.size() || key_sv.substr(0, prefix.size()) != prefix)) {
					break;
				}

				auto parsed = parser.parse_view(key_sv);
				if (parsed) {
					// Verify identity matches
					bool matches = true;
					if (parsed->capture_values.size() == identity_values.size()) {
						for (size_t i = 0; i < identity_values.size(); i++) {
							if (parsed->capture_values[i] != identity_values[i]) {
								matches = false;
								break;
							}
						}
					} else {
						matches = false;
					}

					if (matches) {
						batch.del(std::string(key_sv));
					}
				}
				iter.next();
			}
		}

		batch.commit();
		gstate.delete_count += chunk.size();
	} else {
		// Raw mode: the child emits the key column (with prefix already)
		auto &prefix = lp_table.GetRawKeyPrefix();
		auto batch = connection.create_batch();
		for (idx_t row = 0; row < chunk.size(); row++) {
			auto key_val = chunk.data[0].GetValue(row);
			if (!key_val.IsNull()) {
				batch.del(prefix + key_val.ToString());
			}
		}
		batch.commit();
		gstate.delete_count += chunk.size();
	}

	return SinkResultType::NEED_MORE_INPUT;
}

SinkFinalizeType LevelPivotDelete::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                            OperatorSinkFinalizeInput &input) const {
	return SinkFinalizeType::READY;
}

SourceResultType LevelPivotDelete::GetData(ExecutionContext &context, DataChunk &chunk,
                                           OperatorSourceInput &input) const {
	auto &gstate = sink_state->Cast<LevelPivotDeleteGlobalState>();
	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(static_cast<int64_t>(gstate.delete_count)));
	return SourceResultType::FINISHED;
}

} // namespace duckdb
