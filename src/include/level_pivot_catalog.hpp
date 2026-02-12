#pragma once

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/mutex.hpp"
#include "level_pivot_storage.hpp"
#include <memory>

namespace duckdb {

class LevelPivotSchemaEntry;
class LevelPivotTableEntry;

class LevelPivotCatalog : public Catalog {
public:
	LevelPivotCatalog(AttachedDatabase &db, std::shared_ptr<level_pivot::LevelDBConnection> connection);
	~LevelPivotCatalog() override;

	std::shared_ptr<level_pivot::LevelDBConnection> GetConnection() {
		return connection_;
	}

	// --- Catalog interface ---
	void Initialize(bool load_builtin) override;
	string GetCatalogType() override {
		return "level_pivot";
	}
	optional_ptr<CatalogEntry> CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) override;
	optional_ptr<SchemaCatalogEntry> LookupSchema(CatalogTransaction transaction, const EntryLookupInfo &schema_lookup,
	                                              OnEntryNotFound if_not_found) override;
	void ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) override;
	void DropSchema(ClientContext &context, DropInfo &info) override;

	PhysicalOperator &PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner, LogicalCreateTable &op,
	                                    PhysicalOperator &plan) override;
	PhysicalOperator &PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
	                             optional_ptr<PhysicalOperator> plan) override;
	PhysicalOperator &PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
	                             PhysicalOperator &plan) override;
	PhysicalOperator &PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
	                             PhysicalOperator &plan) override;

	DatabaseSize GetDatabaseSize(ClientContext &context) override;
	bool InMemory() override;
	string GetDBPath() override;

	// Table management (called by level_pivot_create_table function)
	void CreatePivotTable(const string &table_name, const string &pattern, const vector<string> &column_names,
	                      const vector<LogicalType> &column_types);
	void CreateRawTable(const string &table_name, const vector<string> &column_names,
	                    const vector<LogicalType> &column_types);
	void DropTable(const string &table_name);

private:
	std::shared_ptr<level_pivot::LevelDBConnection> connection_;
	unique_ptr<LevelPivotSchemaEntry> main_schema_;
};

} // namespace duckdb
