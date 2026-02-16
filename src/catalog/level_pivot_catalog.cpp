#include "level_pivot_catalog.hpp"
#include "level_pivot_schema.hpp"
#include "level_pivot_table_entry.hpp"
#include "level_pivot_insert.hpp"
#include "level_pivot_delete.hpp"
#include "level_pivot_update.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/storage/database_size.hpp"
#include "key_parser.hpp"

namespace duckdb {

LevelPivotCatalog::LevelPivotCatalog(AttachedDatabase &db, std::shared_ptr<level_pivot::LevelDBConnection> connection)
    : Catalog(db), connection_(std::move(connection)) {
}

LevelPivotCatalog::~LevelPivotCatalog() = default;

void LevelPivotCatalog::Initialize(bool load_builtin) {
	CreateSchemaInfo info;
	info.schema = DEFAULT_SCHEMA;
	info.internal = true;
	main_schema_ = make_uniq<LevelPivotSchemaEntry>(*this, info);
}

optional_ptr<CatalogEntry> LevelPivotCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) {
	throw NotImplementedException("Cannot create schemas in a level_pivot database");
}

optional_ptr<SchemaCatalogEntry> LevelPivotCatalog::LookupSchema(CatalogTransaction transaction,
                                                                 const EntryLookupInfo &schema_lookup,
                                                                 OnEntryNotFound if_not_found) {
	const auto &schema_name = schema_lookup.GetEntryName();
	if (schema_name == DEFAULT_SCHEMA || schema_name == "level_pivot") {
		return main_schema_.get();
	}
	if (if_not_found == OnEntryNotFound::THROW_EXCEPTION) {
		throw CatalogException("Schema '%s' not found in level_pivot database", schema_name);
	}
	return nullptr;
}

void LevelPivotCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
	callback(*main_schema_);
}

void LevelPivotCatalog::DropSchema(ClientContext &context, DropInfo &info) {
	throw NotImplementedException("Cannot drop schemas in a level_pivot database");
}

PhysicalOperator &LevelPivotCatalog::PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner,
                                                       LogicalCreateTable &op, PhysicalOperator &plan) {
	throw NotImplementedException("CREATE TABLE AS is not supported for level_pivot databases");
}

PhysicalOperator &LevelPivotCatalog::PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner,
                                                LogicalInsert &op, optional_ptr<PhysicalOperator> plan) {
	auto &insert = planner.Make<LevelPivotInsert>(op.types, op.table, op.estimated_cardinality);
	if (plan) {
		insert.children.push_back(*plan);
	}
	return insert;
}

PhysicalOperator &LevelPivotCatalog::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner,
                                                LogicalDelete &op, PhysicalOperator &plan) {
	auto &del = planner.Make<LevelPivotDelete>(op.types, op.table, op.estimated_cardinality);
	del.children.push_back(plan);
	return del;
}

PhysicalOperator &LevelPivotCatalog::PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner,
                                                LogicalUpdate &op, PhysicalOperator &plan) {
	auto &upd = planner.Make<LevelPivotUpdate>(op.types, op.table, op.columns, op.estimated_cardinality);
	upd.children.push_back(plan);
	return upd;
}

DatabaseSize LevelPivotCatalog::GetDatabaseSize(ClientContext &context) {
	DatabaseSize size;
	size.free_blocks = 0;
	size.total_blocks = 0;
	size.used_blocks = 0;
	size.wal_size = 0;
	size.block_size = 0;
	size.bytes = 0;
	return size;
}

bool LevelPivotCatalog::InMemory() {
	return false;
}

string LevelPivotCatalog::GetDBPath() {
	return connection_->path();
}

void LevelPivotCatalog::CreatePivotTable(const string &table_name, const string &pattern,
                                         const vector<string> &column_names, const vector<LogicalType> &column_types,
                                         const vector<bool> &column_json) {
	// Parse the key pattern
	auto key_pattern = std::make_unique<level_pivot::KeyPattern>(pattern);
	auto key_parser = std::make_unique<level_pivot::KeyParser>(*key_pattern);

	// Separate identity columns from attr columns
	vector<string> identity_columns;
	vector<string> attr_columns;
	auto &capture_names = key_pattern->capture_names();

	for (idx_t i = 0; i < column_names.size(); i++) {
		auto &col_name = column_names[i];
		bool is_identity = false;
		for (auto &cap : capture_names) {
			if (col_name == cap) {
				is_identity = true;
				break;
			}
		}
		if (is_identity) {
			if (column_json[i]) {
				throw InvalidInputException("Identity column '%s' cannot be JSON-encoded", col_name);
			}
			identity_columns.push_back(col_name);
		} else {
			attr_columns.push_back(col_name);
		}
	}

	// Create the CreateTableInfo with specified column types
	auto info = make_uniq<CreateTableInfo>();
	info->table = table_name;
	info->schema = DEFAULT_SCHEMA;
	for (idx_t i = 0; i < column_names.size(); i++) {
		info->columns.AddColumn(ColumnDefinition(column_names[i], column_types[i]));
	}

	auto table_entry = make_uniq<LevelPivotTableEntry>(*this, *main_schema_, *info, connection_, std::move(key_parser),
	                                                   identity_columns, attr_columns, vector<bool>(column_json));
	main_schema_->AddTable(std::move(table_entry));
}

void LevelPivotCatalog::CreateRawTable(const string &table_name, const vector<string> &column_names,
                                       const vector<LogicalType> &column_types, const vector<bool> &column_json) {
	if (column_names.size() != 2) {
		throw InvalidInputException("Raw tables must have exactly 2 columns (key, value)");
	}
	if (column_json[0]) {
		throw InvalidInputException("Key column cannot be JSON-encoded");
	}

	auto info = make_uniq<CreateTableInfo>();
	info->table = table_name;
	info->schema = DEFAULT_SCHEMA;
	for (idx_t i = 0; i < column_names.size(); i++) {
		info->columns.AddColumn(ColumnDefinition(column_names[i], column_types[i]));
	}

	auto table_entry = make_uniq<LevelPivotTableEntry>(*this, *main_schema_, *info, connection_, vector<bool>(column_json));
	main_schema_->AddTable(std::move(table_entry));
}

void LevelPivotCatalog::DropTable(const string &table_name) {
	main_schema_->DropTable(table_name);
}

} // namespace duckdb
