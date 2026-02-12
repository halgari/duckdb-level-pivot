#include "level_pivot_scan.hpp"
#include "level_pivot_table_entry.hpp"
#include "key_parser.hpp"
#include "level_pivot_storage.hpp"
#include "duckdb/common/types/data_chunk.hpp"

namespace duckdb {

LevelPivotScanGlobalState::LevelPivotScanGlobalState() : done(false) {
}

struct LevelPivotScanLocalState : public LocalTableFunctionState {
	// Pivot mode state
	std::unique_ptr<level_pivot::LevelDBIterator> iterator;
	std::string prefix;
	bool initialized = false;

	// Current row accumulation (pivot mode)
	std::optional<std::vector<std::string>> current_identity;
	std::unordered_map<std::string, std::string> current_attrs;

	// Raw mode state
	bool raw_done = false;
};

static unique_ptr<FunctionData> LevelPivotBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
	// This is called from GetScanFunction, bind_data is already set
	throw InternalException("LevelPivot scan should not be bound directly");
}

static unique_ptr<GlobalTableFunctionState> LevelPivotInitGlobal(ClientContext &context,
                                                                 TableFunctionInitInput &input) {
	auto result = make_uniq<LevelPivotScanGlobalState>();
	result->column_ids = input.column_ids;
	return std::move(result);
}

static unique_ptr<LocalTableFunctionState> LevelPivotInitLocal(ExecutionContext &context,
                                                               TableFunctionInitInput &input,
                                                               GlobalTableFunctionState *global_state) {
	return make_uniq<LevelPivotScanLocalState>();
}

static bool IsWithinPrefix(std::string_view key, const std::string &prefix) {
	if (prefix.empty()) {
		return true;
	}
	if (key.size() < prefix.size()) {
		return false;
	}
	return key.substr(0, prefix.size()) == prefix;
}

static bool IdentityMatches(const std::vector<std::string> &identity, const std::vector<std::string_view> &views) {
	if (identity.size() != views.size()) {
		return false;
	}
	for (size_t i = 0; i < identity.size(); ++i) {
		if (identity[i] != views[i]) {
			return false;
		}
	}
	return true;
}

static std::vector<std::string> MaterializeIdentity(const std::vector<std::string_view> &views) {
	std::vector<std::string> result;
	result.reserve(views.size());
	for (const auto &sv : views) {
		result.emplace_back(sv);
	}
	return result;
}

static void PivotScan(LevelPivotTableEntry &table_entry, LevelPivotScanLocalState &lstate,
                      LevelPivotScanGlobalState &gstate, DataChunk &output, const vector<column_t> &column_ids) {
	auto &parser = table_entry.GetKeyParser();
	auto &connection = *table_entry.GetConnection();
	auto &identity_cols = table_entry.GetIdentityColumns();
	auto &attr_cols = table_entry.GetAttrColumns();
	auto &columns = table_entry.GetColumns();

	if (!lstate.initialized) {
		lstate.prefix = parser.build_prefix();
		lstate.iterator = std::make_unique<level_pivot::LevelDBIterator>(connection.iterator());
		if (lstate.prefix.empty()) {
			lstate.iterator->seek_to_first();
		} else {
			lstate.iterator->seek(lstate.prefix);
		}
		lstate.initialized = true;
	}

	idx_t count = 0;
	while (count < STANDARD_VECTOR_SIZE) {
		// Try to complete a row from accumulated state
		bool need_more_keys = true;

		while (need_more_keys && lstate.iterator && lstate.iterator->valid()) {
			std::string_view key_sv = lstate.iterator->key_view();

			if (!IsWithinPrefix(key_sv, lstate.prefix)) {
				// Past prefix - emit any accumulated row and stop
				if (lstate.current_identity.has_value()) {
					need_more_keys = false;
					break;
				}
				gstate.done = true;
				return;
			}

			auto parsed = parser.parse_view(key_sv);
			if (!parsed) {
				lstate.iterator->next();
				continue;
			}

			if (!lstate.current_identity.has_value()) {
				// First key - start new row
				lstate.current_identity = MaterializeIdentity(parsed->capture_values);
				lstate.current_attrs.clear();
			} else if (!IdentityMatches(*lstate.current_identity, parsed->capture_values)) {
				// Identity changed - emit the completed row
				need_more_keys = false;

				// Don't lose this key's attr - save state for next row
				auto new_identity = MaterializeIdentity(parsed->capture_values);
				std::string_view attr_name = parsed->attr_name;

				// Emit current row
				for (idx_t i = 0; i < column_ids.size(); i++) {
					auto col_idx = column_ids[i];
					if (col_idx == COLUMN_IDENTIFIER_ROW_ID) {
						// Emit serialized identity as row_id (shouldn't normally happen with our GetRowIdColumns)
						continue;
					}
					auto &col = columns.GetColumn(LogicalIndex(col_idx));
					auto &col_name = col.Name();

					// Check if identity column
					bool found = false;
					for (idx_t id_idx = 0; id_idx < identity_cols.size(); id_idx++) {
						if (col_name == identity_cols[id_idx]) {
							auto capture_idx = parser.pattern().capture_index(col_name);
							if (capture_idx >= 0 &&
							    static_cast<size_t>(capture_idx) < lstate.current_identity->size()) {
								output.data[i].SetValue(count,
								                        Value((*lstate.current_identity)[capture_idx]));
							} else {
								output.data[i].SetValue(count, Value());
							}
							found = true;
							break;
						}
					}

					if (!found) {
						// Attr column
						auto it = lstate.current_attrs.find(col_name);
						if (it != lstate.current_attrs.end()) {
							output.data[i].SetValue(count, Value(it->second));
						} else {
							output.data[i].SetValue(count, Value());
						}
					}
				}
				count++;

				// Start next row
				lstate.current_identity = std::move(new_identity);
				lstate.current_attrs.clear();

				// Accumulate the current key's attr into the new row
				std::string attr_str(attr_name);
				bool is_known_attr = false;
				for (auto &ac : attr_cols) {
					if (ac == attr_str) {
						is_known_attr = true;
						break;
					}
				}
				if (is_known_attr) {
					lstate.current_attrs[attr_str] = std::string(lstate.iterator->value_view());
				}
				lstate.iterator->next();
				continue;
			}

			// Same identity - accumulate attr
			std::string_view attr_name = parsed->attr_name;
			std::string attr_str(attr_name);
			bool is_known_attr = false;
			for (auto &ac : attr_cols) {
				if (ac == attr_str) {
					is_known_attr = true;
					break;
				}
			}
			if (is_known_attr) {
				lstate.current_attrs[attr_str] = std::string(lstate.iterator->value_view());
			}
			lstate.iterator->next();
		}

		// If iterator exhausted or past prefix, emit remaining accumulated row
		if (lstate.current_identity.has_value() && need_more_keys) {
			for (idx_t i = 0; i < column_ids.size(); i++) {
				auto col_idx = column_ids[i];
				if (col_idx == COLUMN_IDENTIFIER_ROW_ID) {
					continue;
				}
				auto &col = columns.GetColumn(LogicalIndex(col_idx));
				auto &col_name = col.Name();

				bool found = false;
				for (idx_t id_idx = 0; id_idx < identity_cols.size(); id_idx++) {
					if (col_name == identity_cols[id_idx]) {
						auto capture_idx = parser.pattern().capture_index(col_name);
						if (capture_idx >= 0 &&
						    static_cast<size_t>(capture_idx) < lstate.current_identity->size()) {
							output.data[i].SetValue(count, Value((*lstate.current_identity)[capture_idx]));
						} else {
							output.data[i].SetValue(count, Value());
						}
						found = true;
						break;
					}
				}

				if (!found) {
					auto it = lstate.current_attrs.find(col_name);
					if (it != lstate.current_attrs.end()) {
						output.data[i].SetValue(count, Value(it->second));
					} else {
						output.data[i].SetValue(count, Value());
					}
				}
			}
			count++;
			lstate.current_identity.reset();
			lstate.current_attrs.clear();
			gstate.done = true;
			break;
		}

		if (!lstate.iterator || !lstate.iterator->valid()) {
			gstate.done = true;
			break;
		}
	}

	output.SetCardinality(count);
}

static void RawScan(LevelPivotTableEntry &table_entry, LevelPivotScanLocalState &lstate,
                    LevelPivotScanGlobalState &gstate, DataChunk &output, const vector<column_t> &column_ids) {
	auto &connection = *table_entry.GetConnection();
	auto &prefix = table_entry.GetRawKeyPrefix();

	if (!lstate.initialized) {
		lstate.iterator = std::make_unique<level_pivot::LevelDBIterator>(connection.iterator());
		lstate.prefix = prefix;
		if (prefix.empty()) {
			lstate.iterator->seek_to_first();
		} else {
			lstate.iterator->seek(prefix);
		}
		lstate.initialized = true;
	}

	idx_t count = 0;
	while (count < STANDARD_VECTOR_SIZE && lstate.iterator && lstate.iterator->valid()) {
		std::string_view key_sv = lstate.iterator->key_view();

		// Check prefix bounds
		if (!prefix.empty() && !IsWithinPrefix(key_sv, prefix)) {
			gstate.done = true;
			break;
		}

		std::string_view val_sv = lstate.iterator->value_view();
		// Strip the prefix from the key for user-facing output
		std::string_view user_key = prefix.empty() ? key_sv : key_sv.substr(prefix.size());

		for (idx_t i = 0; i < column_ids.size(); i++) {
			auto col_idx = column_ids[i];
			if (col_idx == COLUMN_IDENTIFIER_ROW_ID) {
				continue;
			}
			if (col_idx == 0) {
				// Key column (with prefix stripped)
				output.data[i].SetValue(count, Value(std::string(user_key)));
			} else if (col_idx == 1) {
				// Value column
				output.data[i].SetValue(count, Value(std::string(val_sv)));
			}
		}
		count++;
		lstate.iterator->next();
	}

	if (!lstate.iterator || !lstate.iterator->valid()) {
		gstate.done = true;
	}

	output.SetCardinality(count);
}

static void LevelPivotScanFunc(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<LevelPivotScanData>();
	auto &gstate = data.global_state->Cast<LevelPivotScanGlobalState>();
	auto &lstate = data.local_state->Cast<LevelPivotScanLocalState>();
	auto &table_entry = *bind_data.table_entry;

	if (gstate.done) {
		output.SetCardinality(0);
		return;
	}

	auto &column_ids = gstate.column_ids;

	if (table_entry.GetTableMode() == LevelPivotTableMode::PIVOT) {
		PivotScan(table_entry, lstate, gstate, output, column_ids);
	} else {
		RawScan(table_entry, lstate, gstate, output, column_ids);
	}
}

TableFunction LevelPivotScanFunction() {
	TableFunction func("level_pivot_scan", {}, LevelPivotScanFunc);
	func.init_global = LevelPivotInitGlobal;
	func.init_local = LevelPivotInitLocal;
	func.projection_pushdown = true;
	return func;
}

} // namespace duckdb
