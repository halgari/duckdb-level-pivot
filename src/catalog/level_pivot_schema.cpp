#include "level_pivot_schema.hpp"
#include "level_pivot_table_entry.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/catalog/entry_lookup_info.hpp"

namespace duckdb {

LevelPivotSchemaEntry::LevelPivotSchemaEntry(Catalog &catalog, CreateSchemaInfo &info)
    : SchemaCatalogEntry(catalog, info) {
}

LevelPivotSchemaEntry::~LevelPivotSchemaEntry() = default;

void LevelPivotSchemaEntry::AddTable(unique_ptr<LevelPivotTableEntry> table) {
	auto name = table->name;
	tables_[name] = std::move(table);
}

void LevelPivotSchemaEntry::DropTable(const string &name) {
	tables_.erase(name);
}

optional_ptr<LevelPivotTableEntry> LevelPivotSchemaEntry::GetTable(const string &name) {
	auto it = tables_.find(name);
	if (it == tables_.end()) {
		return nullptr;
	}
	return it->second.get();
}

optional_ptr<CatalogEntry> LevelPivotSchemaEntry::CreateTable(CatalogTransaction transaction,
                                                              BoundCreateTableInfo &info) {
	throw NotImplementedException("Use level_pivot_create_table() to create tables in a level_pivot database");
}

optional_ptr<CatalogEntry> LevelPivotSchemaEntry::CreateFunction(CatalogTransaction transaction,
                                                                 CreateFunctionInfo &info) {
	throw NotImplementedException("Cannot create functions in a level_pivot database");
}

optional_ptr<CatalogEntry> LevelPivotSchemaEntry::CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
                                                              TableCatalogEntry &table) {
	throw NotImplementedException("Cannot create indexes in a level_pivot database");
}

optional_ptr<CatalogEntry> LevelPivotSchemaEntry::CreateView(CatalogTransaction transaction, CreateViewInfo &info) {
	throw NotImplementedException("Cannot create views in a level_pivot database");
}

optional_ptr<CatalogEntry> LevelPivotSchemaEntry::CreateSequence(CatalogTransaction transaction,
                                                                 CreateSequenceInfo &info) {
	throw NotImplementedException("Cannot create sequences in a level_pivot database");
}

optional_ptr<CatalogEntry> LevelPivotSchemaEntry::CreateTableFunction(CatalogTransaction transaction,
                                                                      CreateTableFunctionInfo &info) {
	throw NotImplementedException("Cannot create table functions in a level_pivot database");
}

optional_ptr<CatalogEntry> LevelPivotSchemaEntry::CreateCopyFunction(CatalogTransaction transaction,
                                                                     CreateCopyFunctionInfo &info) {
	throw NotImplementedException("Cannot create copy functions in a level_pivot database");
}

optional_ptr<CatalogEntry> LevelPivotSchemaEntry::CreatePragmaFunction(CatalogTransaction transaction,
                                                                       CreatePragmaFunctionInfo &info) {
	throw NotImplementedException("Cannot create pragma functions in a level_pivot database");
}

optional_ptr<CatalogEntry> LevelPivotSchemaEntry::CreateCollation(CatalogTransaction transaction,
                                                                  CreateCollationInfo &info) {
	throw NotImplementedException("Cannot create collations in a level_pivot database");
}

optional_ptr<CatalogEntry> LevelPivotSchemaEntry::CreateType(CatalogTransaction transaction, CreateTypeInfo &info) {
	throw NotImplementedException("Cannot create types in a level_pivot database");
}

optional_ptr<CatalogEntry> LevelPivotSchemaEntry::LookupEntry(CatalogTransaction transaction,
                                                              const EntryLookupInfo &lookup_info) {
	if (lookup_info.GetCatalogType() != CatalogType::TABLE_ENTRY) {
		return nullptr;
	}
	auto table = GetTable(lookup_info.GetEntryName());
	if (!table) {
		return nullptr;
	}
	return optional_ptr<CatalogEntry>(table.get());
}

void LevelPivotSchemaEntry::DropEntry(ClientContext &context, DropInfo &info) {
	if (info.type == CatalogType::TABLE_ENTRY) {
		DropTable(info.name);
	}
}

void LevelPivotSchemaEntry::Alter(CatalogTransaction transaction, AlterInfo &info) {
	throw NotImplementedException("Cannot alter entries in a level_pivot database");
}

void LevelPivotSchemaEntry::Scan(ClientContext &context, CatalogType type,
                                 const std::function<void(CatalogEntry &)> &callback) {
	if (type != CatalogType::TABLE_ENTRY) {
		return;
	}
	for (auto &kv : tables_) {
		callback(*kv.second);
	}
}

void LevelPivotSchemaEntry::Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
	if (type != CatalogType::TABLE_ENTRY) {
		return;
	}
	for (auto &kv : tables_) {
		callback(*kv.second);
	}
}

} // namespace duckdb
