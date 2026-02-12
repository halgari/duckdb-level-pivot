#pragma once

#include "key_pattern.hpp"
#include "simd_parser.hpp"
#include <string>
#include <string_view>
#include <vector>
#include <optional>
#include <unordered_map>
#include <memory>

namespace level_pivot {

struct ParsedKey {
	std::vector<std::string> capture_values;
	std::string attr_name;

	bool operator==(const ParsedKey &other) const {
		return capture_values == other.capture_values && attr_name == other.attr_name;
	}
};

struct ParsedKeyView {
	std::vector<std::string_view> capture_values;
	std::string_view attr_name;

	bool operator==(const ParsedKeyView &other) const {
		return capture_values == other.capture_values && attr_name == other.attr_name;
	}

	ParsedKey to_owned() const {
		ParsedKey result;
		result.capture_values.reserve(capture_values.size());
		for (const auto &sv : capture_values) {
			result.capture_values.emplace_back(sv);
		}
		result.attr_name = std::string(attr_name);
		return result;
	}
};

class KeyParser {
public:
	explicit KeyParser(const KeyPattern &pattern);
	explicit KeyParser(const std::string &pattern);

	const KeyPattern &pattern() const {
		return pattern_;
	}

	bool matches(std::string_view key) const;
	std::optional<ParsedKey> parse(std::string_view key) const;
	std::optional<ParsedKeyView> parse_view(std::string_view key) const;

	// Zero-alloc parse into pre-allocated buffers. Returns false if key doesn't match.
	// captures must point to an array with at least pattern().capture_count() elements.
	bool parse_fast(std::string_view key, std::string_view *captures, std::string_view &attr) const;

	std::string build(const std::vector<std::string> &capture_values, const std::string &attr_name) const;
	std::string build(const std::unordered_map<std::string, std::string> &captures, const std::string &attr_name) const;

	std::string build_prefix() const;
	std::string build_prefix(const std::vector<std::string> &capture_values) const;
	bool starts_with_prefix(std::string_view key) const;

private:
	KeyPattern pattern_;
	size_t estimated_key_size_;

	// SIMD parser for uniform delimiter patterns (optional)
	std::unique_ptr<SimdKeyParser> simd_parser_;
	std::string simd_prefix_;
	std::string simd_delimiter_;

	void compute_estimated_key_size();
	void try_init_simd_parser();
	std::optional<std::string> try_get_uniform_delimiter() const;
};

} // namespace level_pivot
