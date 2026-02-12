#pragma once

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "key_parser.hpp"
#include "level_pivot_storage.hpp"
#include <memory>

namespace duckdb {

enum class LevelPivotTableMode : uint8_t { PIVOT, RAW };

class LevelPivotCatalog;

class LevelPivotTableEntry : public TableCatalogEntry {
public:
	// Pivot mode constructor
	LevelPivotTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info,
	                     std::shared_ptr<level_pivot::LevelDBConnection> connection,
	                     std::unique_ptr<level_pivot::KeyParser> parser, vector<string> identity_columns,
	                     vector<string> attr_columns);

	// Raw mode constructor
	LevelPivotTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info,
	                     std::shared_ptr<level_pivot::LevelDBConnection> connection);

	LevelPivotTableMode GetTableMode() const {
		return mode_;
	}

	level_pivot::KeyParser &GetKeyParser() {
		return *parser_;
	}
	const level_pivot::KeyParser &GetKeyParser() const {
		return *parser_;
	}

	std::shared_ptr<level_pivot::LevelDBConnection> GetConnection() {
		return connection_;
	}

	const vector<string> &GetIdentityColumns() const {
		return identity_columns_;
	}
	const vector<string> &GetAttrColumns() const {
		return attr_columns_;
	}

	const string &GetRawKeyPrefix() const {
		return raw_key_prefix_;
	}

	// Map column name to its index in the column list
	idx_t GetColumnIndex(const string &name) const;

	// --- TableCatalogEntry interface ---
	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;
	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override;
	TableStorageInfo GetStorageInfo(ClientContext &context) override;
	vector<column_t> GetRowIdColumns() const override;
	virtual_column_map_t GetVirtualColumns() const override;

private:
	LevelPivotTableMode mode_;
	std::shared_ptr<level_pivot::LevelDBConnection> connection_;
	std::unique_ptr<level_pivot::KeyParser> parser_; // nullptr for raw mode
	vector<string> identity_columns_;
	vector<string> attr_columns_;
	string raw_key_prefix_; // For raw mode: table_name + "##"
};

} // namespace duckdb
