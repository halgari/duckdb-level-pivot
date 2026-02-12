#pragma once

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/value.hpp"
#include <string>
#include <string_view>
#include <vector>

namespace duckdb {

inline Value StringToTypedValue(std::string_view str_value, const LogicalType &type) {
	if (type.id() == LogicalTypeId::VARCHAR) {
		return Value(std::string(str_value));
	}
	return Value(std::string(str_value)).DefaultCastAs(type);
}

inline bool IsWithinPrefix(std::string_view key, std::string_view prefix) {
	if (prefix.empty()) {
		return true;
	}
	if (key.size() < prefix.size()) {
		return false;
	}
	return key.compare(0, prefix.size(), prefix.data(), prefix.size()) == 0;
}

inline bool IdentityMatches(const std::vector<std::string> &identity, const std::string_view *captures, size_t count) {
	if (identity.size() != count) {
		return false;
	}
	for (size_t i = 0; i < count; ++i) {
		if (identity[i] != captures[i]) {
			return false;
		}
	}
	return true;
}

inline void ExtractIdentityValues(std::vector<std::string> &out, DataChunk &chunk, idx_t row, idx_t col_offset,
                                  idx_t num_cols) {
	out.clear();
	for (idx_t i = 0; i < num_cols; i++) {
		auto val = chunk.data[col_offset + i].GetValue(row);
		out.push_back(val.IsNull() ? "" : val.ToString());
	}
}

} // namespace duckdb
