use std::{env, net::SocketAddr, path::Path, sync::Arc, time::Duration};

use ethers::signers::Signer;
use hyperliquid_executor::{
    api,
    engine::ShadowExecutionEngine,
    key_vault::{encrypt_key_interactive, unlock_key_interactive},
    live_execution::LiveExecutionWorker,
    live_gateway::HyperliquidGateway,
    mainnet_read::{MainnetReadAdapter, start_mainnet_read_service},
    risk::{DEFAULT_RISK_POLICY_VERSION, RiskEngine, RiskPolicy, load_risk_policy},
    store::ExecutionStore,
};
use hyperliquid_rust_sdk::BaseUrl;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() {
    handle_command();
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "hyperliquid_executor=info,tower_http=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let bind = env::var("HL_EXECUTOR_BIND").unwrap_or_else(|_| "127.0.0.1:31800".into());
    let database_path = env::var("HL_EXECUTOR_DB").unwrap_or_else(|_| "data/executor.db".into());
    if let Some(parent) = Path::new(&database_path).parent() {
        std::fs::create_dir_all(parent).expect("failed to create executor database directory");
    }
    let key_material = env::var("HL_EXECUTOR_KEY_FILE").ok().map(|path| {
        unlock_key_interactive(Path::new(&path)).expect("failed to unlock HL_EXECUTOR_KEY_FILE")
    });
    let execution_mode = env::var("HL_EXECUTOR_MODE").unwrap_or_else(|_| "shadow".into());
    if !matches!(
        execution_mode.as_str(),
        "shadow" | "testnet" | "tiny-mainnet"
    ) {
        panic!("HL_EXECUTOR_MODE must be shadow, testnet, or tiny-mainnet");
    }

    let store =
        Arc::new(ExecutionStore::open(&database_path).expect("failed to open execution store"));
    let custom_risk_policy = env::var("HL_EXECUTOR_RISK_POLICY").ok();
    let risk_policy = match custom_risk_policy.as_deref() {
        Some(path) => load_risk_policy(path).expect("failed to load HL_EXECUTOR_RISK_POLICY"),
        None => RiskPolicy::shadow_tiny_default(),
    };
    let risk_policy_version = risk_policy.version.clone();
    let risk_engine = RiskEngine::new(risk_policy).expect("risk policy must be valid");
    let mut reconciliation_account = None;
    let mainnet_read = env::var("HL_MAINNET_ACCOUNT_ADDRESS").ok().map(|address| {
        let adapter = MainnetReadAdapter::new(address)
            .expect("HL_MAINNET_ACCOUNT_ADDRESS must be a 0x-prefixed EVM address");
        reconciliation_account = Some(adapter.account_address().to_owned());
        start_mainnet_read_service(
            adapter,
            Arc::clone(&store),
            Duration::from_secs(5),
            Duration::from_secs(15),
        )
    });
    let mut engine = ShadowExecutionEngine::new(Arc::clone(&store), risk_engine);
    if let Some(account) = reconciliation_account.clone() {
        engine = engine.with_mainnet_reconciliation(account);
    }
    if key_material.is_some() {
        engine = engine.with_wallet_loaded();
    }
    if execution_mode != "shadow" {
        let key = key_material
            .as_ref()
            .expect("live modes require HL_EXECUTOR_KEY_FILE");
        let wallet = key
            .to_wallet()
            .expect("decrypted API wallet key is invalid");
        let derived_wallet = format!("{:#x}", wallet.address());
        let configured_wallet = env::var("HL_API_WALLET_ADDRESS")
            .expect("live modes require HL_API_WALLET_ADDRESS")
            .to_ascii_lowercase();
        assert_eq!(
            derived_wallet, configured_wallet,
            "HL_API_WALLET_ADDRESS does not match the encrypted key"
        );
        let account = env::var("HL_ACCOUNT_ADDRESS")
            .or_else(|_| env::var("HL_MAINNET_ACCOUNT_ADDRESS"))
            .expect("live modes require HL_ACCOUNT_ADDRESS");
        assert_eq!(
            store
                .unresolved_live_execution_count(&account)
                .expect("failed to inspect unresolved live executions"),
            0,
            "live worker is locked: unresolved execution outcomes require reconciliation"
        );
        if execution_mode == "tiny-mainnet" {
            assert!(
                custom_risk_policy.is_some(),
                "tiny-mainnet requires HL_EXECUTOR_RISK_POLICY"
            );
            assert_ne!(
                risk_policy_version, DEFAULT_RISK_POLICY_VERSION,
                "tiny-mainnet refuses the example shadow risk policy"
            );
            assert_eq!(
                env::var("HL_TINY_MAINNET_ACK").ok().as_deref(),
                Some("I_UNDERSTAND_REAL_FUNDS_ARE_AT_RISK"),
                "tiny-mainnet requires the exact HL_TINY_MAINNET_ACK"
            );
            assert_eq!(
                reconciliation_account.as_deref(),
                Some(account.as_str()),
                "mainnet read and execution accounts must match"
            );
        }
        let base_url = if execution_mode == "testnet" {
            BaseUrl::Testnet
        } else {
            BaseUrl::Mainnet
        };
        let gateway = HyperliquidGateway::new(wallet, base_url, &account)
            .await
            .expect("failed to initialize Hyperliquid write gateway");
        let worker = LiveExecutionWorker::new(Arc::clone(&store), gateway, account.clone());
        tokio::spawn(async move {
            loop {
                match worker.run_once().await {
                    Ok(true) => continue,
                    Ok(false) => tokio::time::sleep(Duration::from_millis(250)).await,
                    Err(error) => {
                        tracing::error!(%error, "live execution worker stopped after storage failure");
                        break;
                    }
                }
            }
        });
        engine = engine.with_live_execution(account);
    }
    let app = api::router_with_mainnet(engine, mainnet_read);
    let address: SocketAddr = bind
        .parse()
        .expect("HL_EXECUTOR_BIND must be a socket address");
    let listener = tokio::net::TcpListener::bind(address)
        .await
        .expect("failed to bind executor API");

    tracing::info!(%address, %database_path, %risk_policy_version, wallet_loaded = key_material.is_some(), mode = %execution_mode, "Hyperliquid executor started");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .expect("executor API failed");
}

fn handle_command() {
    let mut arguments = env::args_os();
    let executable = arguments.next().unwrap_or_default();
    let Some(command) = arguments.next() else {
        return;
    };
    if command == "encrypt-key" {
        let Some(path) = arguments.next() else {
            eprintln!(
                "usage: {} encrypt-key <encrypted-key-file>",
                Path::new(&executable).display()
            );
            std::process::exit(2);
        };
        if arguments.next().is_some() {
            eprintln!("encrypt-key accepts exactly one file path");
            std::process::exit(2);
        }
        let path = Path::new(&path);
        encrypt_key_interactive(path).expect("failed to encrypt API wallet key");
        println!("encrypted key written to {}", path.display());
        std::process::exit(0);
    }
    eprintln!("unknown command: {}", command.to_string_lossy());
    std::process::exit(2);
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };
    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
