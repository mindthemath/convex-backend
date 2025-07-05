use std::{
    collections::HashSet,
    io::{self, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result};
use clap::Parser;
use cmd_util::env::config_tool;
use common::{
    identity::Identity,
    runtime::Runtime,
    types::ObjectKey,
};
use db_connection::connect_persistence;
use futures::{StreamExt, TryStreamExt};
use model::{
    database_globals::{types::StorageType, DatabaseGlobalsModel},
    file_storage::{self, FILE_STORAGE_TABLE},
    initialize_application_system_tables,
    IndexModel,
};
use runtime::prod::ProdRuntime;
use search::{searcher::InProcessSearcher, Searcher};
use storage::{create_storage, StorageUseCase, Storage};
use walkdir::WalkDir;
use database::Database;
use events::usage::NoOpUsageEventLogger;

#[derive(Parser, Debug)]
struct CleanupConfig {
    #[clap(long, value_enum, default_value_t = clusters::DbDriverTag::Sqlite)]
    db: clusters::DbDriverTag,
    #[clap(long, default_value = "convex_local_backend.sqlite3")]
    db_spec: String,
    #[clap(long, default_value = "convex_local_storage")]
    local_storage: String,
    #[clap(long)]
    s3_storage: bool,
    #[clap(long)]
    do_not_require_ssl: bool,
    #[clap(long)]
    confirm: bool,
}

fn main() -> Result<()> {
    let _guard = config_tool();
    let config = CleanupConfig::parse();
    let tokio = ProdRuntime::init_tokio()?;
    let runtime = ProdRuntime::new(&tokio);
    runtime.block_on("file_storage_cleanup", run(config, runtime))
}

async fn run(config: CleanupConfig, runtime: ProdRuntime) -> Result<()> {
    let persistence = connect_persistence(
        config.db,
        &config.db_spec,
        !config.do_not_require_ssl,
        false,
        "file-storage-cleanup",
        runtime.clone(),
        common::shutdown::ShutdownSignal::panic(),
    )
    .await?;
    let searcher: Arc<dyn Searcher> = Arc::new(InProcessSearcher::new(runtime.clone()).await?);
    let database = Database::load(
        persistence.clone(),
        runtime.clone(),
        searcher,
        common::shutdown::ShutdownSignal::panic(),
        model::virtual_system_mapping().clone(),
        Arc::new(NoOpUsageEventLogger),
    )
    .await?;
    initialize_application_system_tables(&database).await?;

    let storage_type = {
        let mut tx = database.begin_system().await?;
        let globals = DatabaseGlobalsModel::new(&mut tx).database_globals().await?;
        globals
            .value
            .storage_type
            .context("storage_type not set")?
    };
    let files_storage = create_storage(runtime.clone(), &storage_type, StorageUseCase::Files).await?;

    let active_keys = list_active_keys(&database).await?;
    let stored_keys = match &storage_type {
        StorageType::Local { dir } => {
            let base = PathBuf::from(dir).join(StorageUseCase::Files.to_string());
            list_local_objects(&base)?
        }
        StorageType::S3 { s3_prefix } => {
            let bucket = aws_s3::storage::s3_bucket_name(&StorageUseCase::Files)?;
            list_s3_objects(s3_prefix, bucket).await?
        }
    };

    let to_delete: Vec<ObjectKey> = stored_keys
        .difference(&active_keys)
        .cloned()
        .collect();

    println!("Found {} unreferenced files", to_delete.len());
    if to_delete.is_empty() {
        return Ok(());
    }
    if !config.confirm {
        println!("Run again with --confirm to delete these files.");
        return Ok(());
    }
    println!("This will permanently delete {} files. Type 'yes' to proceed:", to_delete.len());
    io::stdout().flush()?;
    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    if input.trim() != "yes" {
        println!("Aborted");
        return Ok(());
    }
    for key in &to_delete {
        files_storage.delete_object(key).await?;
    }
    println!("Deleted {} files", to_delete.len());
    Ok(())
}

async fn list_active_keys<RT: Runtime>(db: &Database<RT>) -> Result<HashSet<ObjectKey>> {
    let (tablet_ids, snapshot_ts) = {
        let mut tx = db.begin(Identity::system()).await?;
        let by_id_indexes = IndexModel::new(&mut tx).by_id_indexes().await?;
        let table_mapping = tx.table_mapping();
        let tablet_ids: HashSet<_> = table_mapping
            .iter()
            .filter(|(tablet_id, _, _, table_name)| {
                **table_name == *FILE_STORAGE_TABLE && table_mapping.is_active(*tablet_id)
            })
            .map(|(tablet_id, ..)| *tablet_id)
            .collect();
        let snapshot_ts = tx.begin_timestamp();
        (tablet_ids, snapshot_ts)
    };
    let mut keys = HashSet::new();
    for tablet_id in tablet_ids {
        let table_iterator = db.table_iterator(snapshot_ts, 100);
        let by_id = db.index_registry().must_get_by_id(tablet_id)?.id;
        let mut stream = Box::pin(table_iterator.stream_documents_in_table(tablet_id, by_id, None));
        while let Some(doc) = stream.try_next().await? {
            let entry: file_storage::FileStorageEntry = doc.value.parse()?;
            keys.insert(entry.storage_key);
        }
    }
    Ok(keys)
}

fn list_local_objects(base: &Path) -> Result<HashSet<ObjectKey>> {
    let mut keys = HashSet::new();
    for entry in WalkDir::new(base).into_iter().filter_map(Result::ok) {
        if entry.file_type().is_file() {
            let rel = entry.path().strip_prefix(base)?;
            let mut s = rel.to_string_lossy().replace('\\', "/");
            if let Some(stripped) = s.strip_suffix(".blob") {
                s = stripped.to_string();
            }
            keys.insert(s.try_into()?);
        }
    }
    Ok(keys)
}

async fn list_s3_objects(prefix: &str, bucket: String) -> Result<HashSet<ObjectKey>> {
    let client = aws_utils::s3::S3Client::new(true).await?;
    let mut stream = client.list_all_s3_files_from_bucket(bucket, Some(prefix.to_string()));
    let mut keys = HashSet::new();
    while let Some(obj) = stream.try_next().await? {
        if let Some(key) = obj.key() {
            if let Some(stripped) = key.strip_prefix(prefix) {
                keys.insert(stripped.to_string().try_into()?);
            }
        }
    }
    Ok(keys)
}
