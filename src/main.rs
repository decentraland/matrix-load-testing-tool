use std::time::Duration;

use config::ConfigError;
use matrix_reloaded::{configuration::SimulationConfig, Simulation};
use miette::Result;
use tokio_graceful_shutdown::SubsystemHandle;
use tokio_graceful_shutdown::Toplevel;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    // graceful shutdown
    Toplevel::new()
        .start("Simulation", simulation)
        .catch_signals()
        .handle_shutdown_requests(Duration::from_secs(1))
        .await
        .map_err(Into::into)
}

async fn simulation(_: SubsystemHandle) -> Result<(), ConfigError> {
    log::info!("Simulation started.");

    let mut simulation = Simulation::with_config(SimulationConfig::new()?);
    simulation.run().await;

    log::info!("Simulation stopped.");

    Ok(())
}
