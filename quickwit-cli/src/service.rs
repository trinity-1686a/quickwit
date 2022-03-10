// Copyright (C) 2021 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use std::path::PathBuf;

use anyhow::bail;
use clap::{arg, ArgMatches, Command};
use quickwit_common::run_checklist;
use quickwit_common::uri::Uri;
use quickwit_indexing::actors::IndexingServer;
use quickwit_metastore::quickwit_metastore_uri_resolver;
use quickwit_serve::run_searcher;
use quickwit_storage::quickwit_storage_uri_resolver;
use quickwit_telemetry::payload::TelemetryEvent;
use tracing::debug;

use crate::load_quickwit_config;

pub fn build_service_command<'a>() -> Command<'a> {
    Command::new("service")
        .about("Launches services.")
        .subcommand(
            Command::new("run")
            .about("Starts a service. Currently, the only services available are `indexer` and `searcher`.")
            .args(&[
                arg!(--config).env("QW_CONFIG"),
                arg!(--"data-dir" <DATA_DIR> "Where data is persisted. Override data-dir defined in config file, default is `./qwdata`.")
                            .env("QW_DATA_DIR")
                            .required(false),
                arg!(--"service" <SERVICE> "Services (searcher|indexer) to run. If unspecified run both `searcher` and `indexer`.")
                    .required(false)
            ])
        )
        .arg_required_else_help(true)
}

#[derive(Debug, PartialEq)]
pub struct ServiceCliCommand {
    pub config_uri: Uri,
    pub data_dir_path: Option<PathBuf>,
    pub service: Option<Service>
}

#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub enum Service {
    Indexer,
    Searcher
}

impl TryFrom<&str> for Service {
    type Error = anyhow::Error;

    fn try_from(service_str: &str) -> Result<Self, Self::Error> {
        match service_str{
            "indexer" => {
                Ok(Service::Indexer)
            }
            "searcher" => {
                Ok(Service::Searcher)
            }
            _ => {
                bail!("Service `{service_str}` unknown");
            }
        }
    }
}

impl ServiceCliCommand {
    pub fn parse_cli_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let (subcommand, submatches) = matches
            .subcommand()
            .ok_or_else(|| anyhow::anyhow!("Failed to parse sub-matches."))?;
        match subcommand {
            "run" => Self::parse_run_args(submatches),
            _ => bail!("Service subcommand `{}` is not implemented.", subcommand),
        }
    }

    fn parse_run_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let config_uri = matches
            .value_of("config")
            .map(Uri::try_new)
            .expect("`config` is a required arg.")?;
        let data_dir_path = matches.value_of("data-dir").map(PathBuf::from);
        let service: Option<Service> =
            if let Some(service_str) = matches.value_of("service") {
                let service = Service::try_from(service_str)?;
                Some(service)
            } else {
                None
            };
        Ok(ServiceCliCommand {
            config_uri,
            data_dir_path,
            service
        })
    }

    pub async fn execute(&self) -> anyhow::Result<()> {
        debug!(args = ?self, "run-service");
        let service_str = self.service.map(|service| format!("{service:?}"))
            .unwrap_or_else(|| "all".to_string());
        let telemetry_event = TelemetryEvent::RunService(service_str);
        quickwit_telemetry::send_telemetry_event(telemetry_event).await;

        let config = load_quickwit_config(&self.config_uri, self.data_dir_path.clone()).await?;
        let metastore = quickwit_metastore_uri_resolver()
            .resolve(&config.metastore_uri())
            .await?;
        let storage_resolver = quickwit_storage_uri_resolver().clone();

        Ok(())

    }
}

// async fn run_indexer_cli(args: RunIndexerArgs) -> anyhow::Result<()> {
//     debug!(args = ?args, "run-indexer");
//     let telemetry_event = TelemetryEvent::RunService("indexer".to_string());
//     quickwit_telemetry::send_telemetry_event(telemetry_event).await;

//     let config = load_quickwit_config(args.config_uri, args.data_dir_path).await?;
//     let metastore = quickwit_metastore_uri_resolver()
//         .resolve(&config.metastore_uri())
//         .await?;
//     let storage_resolver = quickwit_storage_uri_resolver().clone();
//     let client = IndexingServer::spawn(
//         config.data_dir_path,
//         config.indexer_config,
//         metastore,
//         storage_resolver,
//     );
//     for index_id in args.index_ids {
//         client.spawn_pipelines(index_id).await?;
//     }
//     let (exit_status, _) = client.join_server().await;
//     if exit_status.is_success() {
//         bail!(exit_status)
//     }
//     Ok(())
// }

// async fn run_searcher_cli(args: RunSearcherArgs) -> anyhow::Result<()> {
//     debug!(args = ?args, "run-searcher");
//     let telemetry_event = TelemetryEvent::RunService("searcher".to_string());
//     quickwit_telemetry::send_telemetry_event(telemetry_event).await;

//     let config = load_quickwit_config(args.config_uri, args.data_dir_path).await?;
//     let metastore_uri_resolver = quickwit_metastore_uri_resolver();
//     let metastore = metastore_uri_resolver
//         .resolve(&config.metastore_uri())
//         .await?;
//     run_checklist(vec![("metastore", metastore.check_connectivity().await)]);
//     run_searcher(config, metastore).await?;
//     Ok(())
// }

#[cfg(test)]
mod tests {

    use super::*;
    use crate::cli::{build_cli, CliCommand};

    #[test]
    fn test_parse_run_searcher_args() -> anyhow::Result<()> {
        let command = build_cli().no_binary_name(true);
        let matches = command.try_get_matches_from(vec![
            "service",
            "run",
            "searcher",
            "--config",
            "/config.yaml",
        ])?;
        let command = CliCommand::parse_cli_args(&matches)?;
        let expected_config_uri = Uri::try_new("file:///config.yaml").unwrap();
        assert!(matches!(
            command,
            CliCommand::Service(ServiceCliCommand::RunSearcher(RunSearcherArgs {
                config_uri,
                data_dir_path: None,
            })) if config_uri == expected_config_uri
        ));
        Ok(())
    }

    #[test]
    fn test_parse_run_indexer_args() -> anyhow::Result<()> {
        let command = build_cli().no_binary_name(true);
        let matches = command.try_get_matches_from(vec![
            "service",
            "run",
            "indexer",
            "--config",
            "/config.yaml",
            "--indexes",
            "foo",
            "bar",
        ])?;
        let command = CliCommand::parse_cli_args(&matches)?;
        let expected_config_uri = Uri::try_new("file:///config.yaml").unwrap();
        assert!(matches!(
            command,
            CliCommand::Service(ServiceCliCommand::RunIndexer(RunIndexerArgs {
                config_uri,
                data_dir_path: None,
                index_ids,
            })) if config_uri == expected_config_uri && index_ids == ["foo", "bar"]
        ));
        Ok(())
    }
}
