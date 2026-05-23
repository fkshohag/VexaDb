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
    },
    Search {
        collection: String,
        /// Comma-separated query vector
        vector: String,
        #[arg(long, default_value_t = 10)]
        top_k: u32,
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
    Describe { name: String },
    Delete { name: String },
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
        } => {
            let values = parse_vector(&vector)?;
            let n = client
                .upsert(
                    &collection,
                    vec![VectorPoint {
                        id,
                        values,
                        payload: vec![],
                    }],
                )
                .await?;
            println!("upserted {n}");
        }
        Commands::Search {
            collection,
            vector,
            top_k,
        } => {
            let query = parse_vector(&vector)?;
            let hits = client.search(&collection, query, top_k).await?;
            for hit in hits {
                println!("{}  score={:.6}", hit.id, hit.score);
            }
        }
    }

    Ok(())
}

fn parse_vector(s: &str) -> anyhow::Result<Vec<f32>> {
    s.split(',')
        .map(|p| p.trim().parse::<f32>().context("invalid float"))
        .collect()
}
