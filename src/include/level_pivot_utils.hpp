#pragma once

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/value.hpp"
#include <string>
#include <string_view>
#include <vector>

namespace duckdb {

inline Value StringToTypedValue(const std::string &str_value, const LogicalType &type) {
	if (type.id() == LogicalTypeId::VARCHAR) {
		return Value(str_value);
	}
	return Value(str_value).DefaultCastAs(type);
}

inline bool IsWithinPrefix(std::string_view key, const std::string &prefix) {
	if (prefix.empty()) {
		return true;
	}
	if (key.size() < prefix.size()) {
		return false;
	}
	return key.substr(0, prefix.size()) == prefix;
}

inline bool IdentityMatches(const std::vector<std::string> &identity, const std::vector<std::string_view> &views) {
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

inline std::vector<std::string> MaterializeIdentity(const std::vector<std::string_view> &views) {
	std::vector<std::string> result;
	result.reserve(views.size());
	for (const auto &sv : views) {
		result.emplace_back(sv);
	}
	return result;
}

inline std::vector<std::string> ExtractIdentityValues(DataChunk &chunk, idx_t row, idx_t col_offset,
                                                      idx_t num_cols) {
	std::vector<std::string> identity_values;
	identity_values.reserve(num_cols);
	for (idx_t i = 0; i < num_cols; i++) {
		auto val = chunk.data[col_offset + i].GetValue(row);
		identity_values.push_back(val.IsNull() ? "" : val.ToString());
	}
	return identity_values;
}

} // namespace duckdb
