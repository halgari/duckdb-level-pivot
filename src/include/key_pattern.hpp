#pragma once

#include <string>
#include <vector>
#include <variant>
#include <optional>
#include <stdexcept>

namespace level_pivot {

class KeyPatternError : public std::runtime_error {
public:
	explicit KeyPatternError(const std::string &msg) : std::runtime_error(msg) {
	}
};

struct LiteralSegment {
	std::string text;
	bool operator==(const LiteralSegment &other) const {
		return text == other.text;
	}
};

struct CaptureSegment {
	std::string name;
	bool operator==(const CaptureSegment &other) const {
		return name == other.name;
	}
};

struct AttrSegment {
	bool operator==(const AttrSegment &) const {
		return true;
	}
};

using PatternSegment = std::variant<LiteralSegment, CaptureSegment, AttrSegment>;

class KeyPattern {
public:
	explicit KeyPattern(const std::string &pattern);

	const std::string &pattern() const {
		return pattern_;
	}
	const std::vector<PatternSegment> &segments() const {
		return segments_;
	}
	const std::vector<std::string> &capture_names() const {
		return capture_names_;
	}
	const std::string &literal_prefix() const {
		return literal_prefix_;
	}
	bool has_attr() const {
		return has_attr_;
	}
	int attr_index() const {
		return attr_index_;
	}
	size_t capture_count() const {
		return capture_names_.size();
	}
	bool has_capture(const std::string &name) const;
	int capture_index(const std::string &name) const;

private:
	std::string pattern_;
	std::vector<PatternSegment> segments_;
	std::vector<std::string> capture_names_;
	std::string literal_prefix_;
	bool has_attr_ = false;
	int attr_index_ = -1;

	void parse(const std::string &pattern);
	void compute_literal_prefix();
	void validate() const;
};

} // namespace level_pivot
