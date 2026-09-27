#[tokio::main]
async fn main() {
    wyvern::cli::init_tracing();
    if let Err(err) = wyvern::cli::run().await {
        eprintln!("{err}");
        std::process::exit(1);
    }
}
