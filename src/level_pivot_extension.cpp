#define DUCKDB_EXTENSION_MAIN

#include "level_pivot_extension.hpp"
#include "level_pivot_catalog.hpp"
#include "level_pivot_transaction.hpp"
#include "level_pivot_storage.hpp"
#include "duckdb.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/common/enums/access_mode.hpp"
#include "duckdb/main/config.hpp"
#include <type_traits>

namespace duckdb {

// Forward declarations for functions
TableFunction GetCreateTableFunction();
TableFunction GetDropTableFunction();
TableFunction GetDirtyTablesFunction();

static unique_ptr<Catalog> LevelPivotAttach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
                                            AttachedDatabase &db, const string &name, AttachInfo &info,
                                            AttachOptions &options) {
	// Parse options
	level_pivot::ConnectionOptions conn_opts;
	conn_opts.db_path = info.path;

	// Access mode
	if (options.access_mode == AccessMode::READ_ONLY) {
		conn_opts.read_only = true;
	} else {
		conn_opts.read_only = false;
	}

	// Additional options from ATTACH statement
	for (auto &kv : info.options) {
		auto key = StringUtil::Lower(kv.first);
		if (key == "read_only") {
			conn_opts.read_only = kv.second.GetValue<bool>();
		} else if (key == "create_if_missing") {
			conn_opts.create_if_missing = kv.second.GetValue<bool>();
		} else if (key == "block_cache_size") {
			conn_opts.block_cache_size = kv.second.GetValue<int64_t>();
		} else if (key == "write_buffer_size") {
			conn_opts.write_buffer_size = kv.second.GetValue<int64_t>();
		}
	}

	// Open LevelDB
	auto connection = std::make_shared<level_pivot::LevelDBConnection>(conn_opts);

	return make_uniq<LevelPivotCatalog>(db, std::move(connection));
}

static unique_ptr<TransactionManager>
LevelPivotCreateTransactionManager(optional_ptr<StorageExtensionInfo> storage_info, AttachedDatabase &db,
                                   Catalog &catalog) {
	return make_uniq<LevelPivotTransactionManager>(db);
}

// DuckDB API compatibility: In v1.4.4, storage extensions are registered by
// assigning into the public DBConfig::storage_extensions map. In later versions,
// that map became private and StorageExtension::Register() is the public API.
// We use SFINAE to detect which API is available at compile time.
template <typename SE, typename = void>
struct has_se_register : std::false_type {};

template <typename SE>
struct has_se_register<SE, std::void_t<decltype(SE::Register(std::declval<DBConfig &>(), std::declval<const string &>(),
                                                             std::declval<shared_ptr<SE>>()))>> : std::true_type {};

template <typename SE = StorageExtension, std::enable_if_t<has_se_register<SE>::value, int> = 0>
static void RegisterStorageExt(DBConfig &config, unique_ptr<StorageExtension> ext) {
	auto shared = shared_ptr<SE>(ext.release());
	SE::Register(config, "level_pivot", std::move(shared));
}

template <typename SE = StorageExtension, std::enable_if_t<!has_se_register<SE>::value, int> = 0>
static void RegisterStorageExt(DBConfig &config, unique_ptr<StorageExtension> ext) {
	config.storage_extensions["level_pivot"] = std::move(ext);
}

static void LoadInternal(ExtensionLoader &loader) {
	// Register storage extension
	auto storage_ext = make_uniq<StorageExtension>();
	storage_ext->attach = LevelPivotAttach;
	storage_ext->create_transaction_manager = LevelPivotCreateTransactionManager;
	auto &db = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(db);
	RegisterStorageExt(config, std::move(storage_ext));

	// Register utility table functions
	loader.RegisterFunction(GetCreateTableFunction());
	loader.RegisterFunction(GetDropTableFunction());
	loader.RegisterFunction(GetDirtyTablesFunction());
}

void LevelPivotExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string LevelPivotExtension::Name() {
	return "level_pivot";
}

std::string LevelPivotExtension::Version() const {
#ifdef EXT_VERSION_LEVEL_PIVOT
	return EXT_VERSION_LEVEL_PIVOT;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(level_pivot, loader) {
	duckdb::LoadInternal(loader);
}
}
