#include "level_pivot_scan.hpp"
#include "level_pivot_table_entry.hpp"
#include "level_pivot_utils.hpp"
#include "key_parser.hpp"
#include "level_pivot_storage.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include <unordered_set>

namespace duckdb {

LevelPivotScanGlobalState::LevelPivotScanGlobalState() : done(false) {
}

struct ColumnMapping {
	enum Role { IDENTITY, ATTR, ROW_ID };
	Role role;
	idx_t capture_index; // for IDENTITY: index into identity values
	std::string name;    // column name (for ATTR lookup)
	LogicalType type;
};

struct LevelPivotScanLocalState : public LocalTableFunctionState {
	// Pivot mode state
	std::unique_ptr<level_pivot::LevelDBIterator> iterator;
	std::string prefix;
	bool initialized = false;

	// Current row accumulation (pivot mode)
	std::optional<std::vector<std::string>> current_identity;
	std::unordered_map<std::string, std::string> current_attrs;

	// Pre-computed column mapping (built once per scan init)
	std::vector<ColumnMapping> column_map;
	std::unordered_set<std::string> known_attrs;

	// Raw mode state
	bool raw_done = false;
};

static unique_ptr<FunctionData> LevelPivotBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
	// This is called from GetScanFunction, bind_data is already set
	throw InternalException("LevelPivot scan should not be bound directly");
}

// Called during optimization to extract equality filters on identity columns.
// We inspect the expressions and store a narrowed prefix in bind_data for the scan to use.
// We leave all filters in place so DuckDB still applies them as a post-filter.
static void LevelPivotPushdownComplexFilter(ClientContext &context, LogicalGet &get, FunctionData *bind_data,
                                            vector<unique_ptr<Expression>> &filters) {
	if (!bind_data) {
		return;
	}
	auto &scan_data = bind_data->Cast<LevelPivotScanData>();
	// Always reset prefix - bind_data may be reused across queries via Copy()
	scan_data.filter_prefix.clear();
	auto *table_entry = scan_data.table_entry;
	if (!table_entry || table_entry->GetTableMode() != LevelPivotTableMode::PIVOT) {
		return;
	}

	auto &parser = table_entry->GetKeyParser();
	auto &pattern = parser.pattern();
	auto &capture_names = pattern.capture_names();

	// Build a map: column_name -> equality_value from the filter expressions
	std::unordered_map<std::string, std::string> eq_values;
	for (idx_t i = 0; i < filters.size(); i++) {
		auto &filter = filters[i];
		if (filter->expression_class != ExpressionClass::BOUND_COMPARISON) {
			continue;
		}
		auto &comp = filter->Cast<BoundComparisonExpression>();
		if (comp.type != ExpressionType::COMPARE_EQUAL) {
			continue;
		}

		BoundColumnRefExpression *col_ref = nullptr;
		BoundConstantExpression *const_ref = nullptr;

		if (comp.left->expression_class == ExpressionClass::BOUND_COLUMN_REF &&
		    comp.right->expression_class == ExpressionClass::BOUND_CONSTANT) {
			col_ref = &comp.left->Cast<BoundColumnRefExpression>();
			const_ref = &comp.right->Cast<BoundConstantExpression>();
		} else if (comp.right->expression_class == ExpressionClass::BOUND_COLUMN_REF &&
		           comp.left->expression_class == ExpressionClass::BOUND_CONSTANT) {
			col_ref = &comp.right->Cast<BoundColumnRefExpression>();
			const_ref = &comp.left->Cast<BoundConstantExpression>();
		}

		if (!col_ref || !const_ref) {
			continue;
		}
		if (col_ref->binding.table_index != get.table_index) {
			continue;
		}
		if (const_ref->value.IsNull()) {
			continue;
		}

		// Map from output position through column_ids to actual table column index
		auto output_idx = col_ref->binding.column_index;
		auto &col_ids = get.GetColumnIds();
		if (output_idx >= col_ids.size()) {
			continue;
		}
		auto table_col_idx = col_ids[output_idx].GetPrimaryIndex();
		if (table_col_idx < get.names.size()) {
			eq_values[get.names[table_col_idx]] = const_ref->value.ToString();
		}
	}

	// Build prefix from consecutive identity column equality matches
	std::vector<std::string> capture_values;
	for (auto &cap_name : capture_names) {
		auto it = eq_values.find(cap_name);
		if (it == eq_values.end()) {
			break;
		}
		capture_values.push_back(it->second);
	}

	if (!capture_values.empty()) {
		scan_data.filter_prefix = parser.build_prefix(capture_values);
	}
}

static unique_ptr<GlobalTableFunctionState> LevelPivotInitGlobal(ClientContext &context,
                                                                 TableFunctionInitInput &input) {
	auto result = make_uniq<LevelPivotScanGlobalState>();
	result->column_ids = input.column_ids;

	// Copy filter prefix from bind_data (set by pushdown_complex_filter during optimization)
	if (input.bind_data) {
		auto &bind_data = input.bind_data->Cast<LevelPivotScanData>();
		result->filter_prefix = bind_data.filter_prefix;
	}

	return std::move(result);
}

static unique_ptr<LocalTableFunctionState> LevelPivotInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                               GlobalTableFunctionState *global_state) {
	return make_uniq<LevelPivotScanLocalState>();
}

static void EmitPivotRow(LevelPivotScanLocalState &lstate, DataChunk &output, idx_t row_idx) {
	for (idx_t i = 0; i < lstate.column_map.size(); i++) {
		auto &mapping = lstate.column_map[i];
		if (mapping.role == ColumnMapping::ROW_ID) {
			continue;
		}
		if (mapping.role == ColumnMapping::IDENTITY) {
			if (mapping.capture_index < lstate.current_identity->size()) {
				output.data[i].SetValue(
				    row_idx, StringToTypedValue((*lstate.current_identity)[mapping.capture_index], mapping.type));
			} else {
				output.data[i].SetValue(row_idx, Value());
			}
		} else {
			// ATTR
			auto it = lstate.current_attrs.find(mapping.name);
			if (it != lstate.current_attrs.end()) {
				output.data[i].SetValue(row_idx, StringToTypedValue(it->second, mapping.type));
			} else {
				output.data[i].SetValue(row_idx, Value());
			}
		}
	}
}

static void PivotScan(LevelPivotTableEntry &table_entry, LevelPivotScanLocalState &lstate,
                      LevelPivotScanGlobalState &gstate, DataChunk &output, const vector<column_t> &column_ids) {
	auto &parser = table_entry.GetKeyParser();
	auto &connection = *table_entry.GetConnection();
	auto &columns = table_entry.GetColumns();

	if (!lstate.initialized) {
		// Use filter-narrowed prefix if available, otherwise use the full table prefix
		lstate.prefix = gstate.filter_prefix.empty() ? parser.build_prefix() : gstate.filter_prefix;
		lstate.iterator = std::make_unique<level_pivot::LevelDBIterator>(connection.iterator());
		if (lstate.prefix.empty()) {
			lstate.iterator->seek_to_first();
		} else {
			lstate.iterator->seek(lstate.prefix);
		}

		// Pre-compute column mapping and known attrs set
		auto &identity_cols = table_entry.GetIdentityColumns();
		auto &attr_cols = table_entry.GetAttrColumns();
		std::unordered_set<std::string> identity_set(identity_cols.begin(), identity_cols.end());

		lstate.column_map.resize(column_ids.size());
		for (idx_t i = 0; i < column_ids.size(); i++) {
			auto col_idx = column_ids[i];
			if (col_idx == COLUMN_IDENTIFIER_ROW_ID) {
				lstate.column_map[i].role = ColumnMapping::ROW_ID;
				continue;
			}
			auto &col = columns.GetColumn(LogicalIndex(col_idx));
			auto &col_name = col.Name();
			lstate.column_map[i].name = col_name;
			lstate.column_map[i].type = col.Type();

			if (identity_set.count(col_name)) {
				lstate.column_map[i].role = ColumnMapping::IDENTITY;
				auto capture_idx = parser.pattern().capture_index(col_name);
				lstate.column_map[i].capture_index = capture_idx >= 0 ? static_cast<idx_t>(capture_idx) : 0;
			} else {
				lstate.column_map[i].role = ColumnMapping::ATTR;
			}
		}

		lstate.known_attrs = std::unordered_set<std::string>(attr_cols.begin(), attr_cols.end());
		lstate.initialized = true;
	}

	idx_t count = 0;
	while (count < STANDARD_VECTOR_SIZE) {
		// Try to complete a row from accumulated state
		bool need_more_keys = true;

		while (need_more_keys && lstate.iterator && lstate.iterator->valid()) {
			std::string_view key_sv = lstate.iterator->key_view();

			if (!IsWithinPrefix(key_sv, lstate.prefix)) {
				// Past prefix - if we have accumulated identity, fall through to
				// the post-loop emission code (need_more_keys stays true).
				// If no identity, we're just done.
				if (!lstate.current_identity.has_value()) {
					gstate.done = true;
					output.SetCardinality(count);
					return;
				}
				break;
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

				EmitPivotRow(lstate, output, count);
				count++;

				// Start next row
				lstate.current_identity = std::move(new_identity);
				lstate.current_attrs.clear();

				// Accumulate the current key's attr into the new row
				std::string attr_str(attr_name);
				if (lstate.known_attrs.count(attr_str)) {
					lstate.current_attrs[attr_str] = std::string(lstate.iterator->value_view());
				}
				lstate.iterator->next();
				continue;
			}

			// Same identity - accumulate attr
			std::string attr_str(parsed->attr_name);
			if (lstate.known_attrs.count(attr_str)) {
				lstate.current_attrs[attr_str] = std::string(lstate.iterator->value_view());
			}
			lstate.iterator->next();
		}

		// If iterator exhausted or past prefix, emit remaining accumulated row
		if (lstate.current_identity.has_value() && need_more_keys) {
			EmitPivotRow(lstate, output, count);
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

		auto &columns = table_entry.GetColumns();
		for (idx_t i = 0; i < column_ids.size(); i++) {
			auto col_idx = column_ids[i];
			if (col_idx == COLUMN_IDENTIFIER_ROW_ID) {
				continue;
			}
			auto &col_type = columns.GetColumn(LogicalIndex(col_idx)).Type();
			if (col_idx == 0) {
				// Key column (with prefix stripped)
				output.data[i].SetValue(count, StringToTypedValue(std::string(user_key), col_type));
			} else if (col_idx == 1) {
				// Value column
				output.data[i].SetValue(count, StringToTypedValue(std::string(val_sv), col_type));
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
	func.filter_pushdown = false;
	func.pushdown_complex_filter = LevelPivotPushdownComplexFilter;
	return func;
}

} // namespace duckdb
