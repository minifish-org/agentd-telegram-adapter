use agentd_telegram_adapter::runtime::{
    install_redacted_panic_hook, run_with_config_loader, shutdown_signal,
};
use agentd_telegram_adapter::Config;
use std::process::ExitCode;

#[tokio::main]
async fn main() -> ExitCode {
    install_redacted_panic_hook();
    let result = match shutdown_signal() {
        Ok(shutdown) => run_with_config_loader(Config::from_env, shutdown, None).await,
        Err(error) => Err(error),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{}", error.log_record());
            ExitCode::FAILURE
        }
    }
}
