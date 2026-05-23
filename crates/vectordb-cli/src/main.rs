use anyhow::Context;
use clap::{Parser, Subcommand};
use vectordb_client::{cosine_collection, VectorDbClient};
use vectordb_proto::vectordb::v1::VectorPoint;

#[derive(Parser, Debug)]
#[command(name = "vectordb", about = "VectorDB command-line client")]
struct Cli {
    #[arg(long, default_value = "http://127.0.0.1:6334")]
    endpoint: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    Health,
    Collections {
        #[command(subcommand)]
        action: CollectionAction,
    },
    Upsert {
        collection: String,
        id: String,
        /// Comma-separated floats
        vector: String,
        /// Optional JSON payload, e.g. '{"category":"books","price":29.5}'
        #[arg(long)]
        payload: Option<String>,
    },
    Search {
        collection: String,
        /// Comma-separated query vector
        vector: String,
        #[arg(long, default_value_t = 10)]
        top_k: u32,
        /// Optional JSON filter (Filter DSL)
        #[arg(long)]
        filter: Option<String>,
    },
    Snapshots {
        #[command(subcommand)]
        action: SnapshotAction,
    },
    /// Re-shard a collection across the current cluster topology.
    ///
    /// For every point on every shard, recompute the target shard via the
    /// consistent-hash ring and migrate orphaned points to their new owner.
    /// Safe to run repeatedly — already-correct points are no-ops.
    Rebalance {
        collection: String,
        /// Comma-separated list of shard gRPC endpoints, e.g.
        /// `http://node-a:6334,http://node-b:6334`. Direct shard endpoints,
        /// not the gateway.
        #[arg(long, value_delimiter = ',')]
        shards: Vec<String>,
        /// Page size when scrolling each shard.
        #[arg(long, default_value_t = 256)]
        page: u32,
        /// Plan only — print what would move without writing.
        #[arg(long)]
        dry_run: bool,
    },
}

#[derive(Subcommand, Debug)]
enum CollectionAction {
    List,
    Create {
        name: String,
        #[arg(long)]
        dim: u32,
    },
    Describe {
        name: String,
    },
    Delete {
        name: String,
    },
}

#[derive(Subcommand, Debug)]
enum SnapshotAction {
    Create,
    List,
    Delete { id: String },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let mut client = VectorDbClient::connect(cli.endpoint).await?;

    match cli.command {
        Commands::Health => {
            let status = client.health().await?;
            println!("status: {status}");
        }
        Commands::Collections { action } => match action {
            CollectionAction::List => {
                for name in client.list_collections().await? {
                    println!("{name}");
                }
            }
            CollectionAction::Create { name, dim } => {
                client
                    .create_collection(cosine_collection(&name, dim))
                    .await?;
                println!("created collection {name}");
            }
            CollectionAction::Describe { name } => {
                let (spec, count) = client.describe_collection(&name).await?;
                println!("{spec:?}\nvectors: {count}");
            }
            CollectionAction::Delete { name } => {
                client.delete_collection(&name).await?;
                println!("deleted {name}");
            }
        },
        Commands::Upsert {
            collection,
            id,
            vector,
            payload,
        } => {
            let values = parse_vector(&vector)?;
            let payload_bytes = match payload {
                Some(p) => {
                    serde_json::from_str::<serde_json::Value>(&p)
                        .context("invalid JSON payload")?;
                    p.into_bytes()
                }
                None => vec![],
            };
            let n = client
                .upsert(
                    &collection,
                    vec![VectorPoint {
                        id,
                        values,
                        payload: payload_bytes,
                        sparse: None,
                    }],
                )
                .await?;
            println!("upserted {n}");
        }
        Commands::Search {
            collection,
            vector,
            top_k,
            filter,
        } => {
            let query = parse_vector(&vector)?;
            let filter_json = match filter {
                Some(f) => {
                    serde_json::from_str::<serde_json::Value>(&f).context("invalid filter JSON")?;
                    f
                }
                None => String::new(),
            };
            let hits = client
                .search_with(&collection, query, top_k, vec![], filter_json)
                .await?;
            for hit in hits {
                println!("{}  score={:.6}", hit.id, hit.score);
            }
        }
        Commands::Rebalance {
            collection,
            shards,
            page,
            dry_run,
        } => {
            rebalance(&collection, &shards, page, dry_run).await?;
        }
        Commands::Snapshots { action } => match action {
            SnapshotAction::Create => {
                let snap = client.create_snapshot().await?;
                println!("{}\t{}", snap.id, snap.path);
            }
            SnapshotAction::List => {
                for snap in client.list_snapshots().await? {
                    println!("{}\t{}\t{}", snap.id, snap.created_at_ms, snap.path);
                }
            }
            SnapshotAction::Delete { id } => {
                client.delete_snapshot(&id).await?;
                println!("deleted {id}");
            }
        },
    }

    Ok(())
}

fn parse_vector(s: &str) -> anyhow::Result<Vec<f32>> {
    s.split(',')
        .map(|p| p.trim().parse::<f32>().context("invalid float"))
        .collect()
}

/// Rebalance a single collection across `shards`. For each shard we scroll
/// every local point, recompute its target shard via the same xxh64 hash
/// the router uses, and if the target differs we upsert into the new owner
/// and delete from the source. Idempotent — safe to retry on failure.
async fn rebalance(
    collection: &str,
    shards: &[String],
    page: u32,
    dry_run: bool,
) -> anyhow::Result<()> {
    use vectordb_cluster::shard_for_point;
    if shards.len() < 2 {
        anyhow::bail!("rebalance needs at least 2 shard endpoints");
    }
    let shard_count = shards.len() as u32;
    let mut clients: Vec<VectorDbClient> = Vec::with_capacity(shards.len());
    for ep in shards {
        clients.push(
            VectorDbClient::connect(ep.clone())
                .await
                .with_context(|| format!("connect shard {ep}"))?,
        );
    }

    let mut total_moved = 0u64;
    let mut total_kept = 0u64;
    let mut total_failed = 0u64;
    for (src_idx, ep) in shards.iter().enumerate() {
        let mut cursor = String::new();
        loop {
            let (points, next) = clients[src_idx]
                .scroll(collection, &cursor, page)
                .await
                .with_context(|| format!("scroll {ep}"))?;
            if points.is_empty() && next.is_empty() {
                break;
            }
            for p in points {
                let target = shard_for_point(&p.id, shard_count);
                if target as usize == src_idx {
                    total_kept += 1;
                    continue;
                }
                if dry_run {
                    println!(
                        "would move id={} shard={} -> shard={}",
                        p.id, src_idx, target
                    );
                    total_moved += 1;
                    continue;
                }
                let id = p.id.clone();
                match clients[target as usize]
                    .upsert(collection, vec![p])
                    .await
                {
                    Ok(_) => {
                        if let Err(e) = clients[src_idx]
                            .delete(collection, vec![id.clone()])
                            .await
                        {
                            // Upserted at target but couldn't delete source —
                            // a re-run will reconcile. Don't lose data.
                            eprintln!(
                                "warning: id={id} upserted to shard {target} but delete on shard {src_idx} failed: {e}"
                            );
                            total_failed += 1;
                        } else {
                            total_moved += 1;
                        }
                    }
                    Err(e) => {
                        eprintln!("error: id={id} upsert to shard {target} failed: {e}");
                        total_failed += 1;
                    }
                }
            }
            if next.is_empty() {
                break;
            }
            cursor = next;
        }
    }

    println!(
        "rebalance done: moved={} kept={} failed={} (dry_run={})",
        total_moved, total_kept, total_failed, dry_run
    );
    if total_failed > 0 {
        anyhow::bail!("{total_failed} migrations failed; re-run to retry");
    }
    Ok(())
}
