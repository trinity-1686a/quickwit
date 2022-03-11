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

use std::collections::HashSet;
use std::iter;
use std::path::PathBuf;

use anyhow::bail;
use clap::{arg, ArgMatches, Command};
use itertools::Itertools;
use quickwit_common::uri::Uri;
use quickwit_serve::{serve_quickwit, QService};
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
    pub services: HashSet<QService>,
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
        let services: HashSet<QService> = if let Some(service_str) = matches.value_of("service") {
            let service = QService::try_from(service_str)?;
            iter::once(service).collect()
        } else {
            [QService::Indexer, QService::Searcher]
                .into_iter()
                .collect()
        };
        Ok(ServiceCliCommand {
            config_uri,
            data_dir_path,
            services,
        })
    }

    pub async fn execute(&self) -> anyhow::Result<()> {
        debug!(args = ?self, "run-service");
        let service_str = self
            .services
            .iter()
            .map(|service| format!("{service:?}"))
            .join(",");
        let telemetry_event = TelemetryEvent::RunService(service_str);
        quickwit_telemetry::send_telemetry_event(telemetry_event).await;

        let config = load_quickwit_config(&self.config_uri, self.data_dir_path.clone()).await?;
        serve_quickwit(&config, &self.services).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::cli::{build_cli, CliCommand};

    #[test]
    fn test_parse_service_run_ars() -> anyhow::Result<()> {
        let command = build_cli().no_binary_name(true);
        let matches = command.try_get_matches_from(vec![
            "service",
            "run",
            "indexer",
            "--config",
            "/config.yaml",
        ])?;
        let command = CliCommand::parse_cli_args(&matches)?;
        let expected_config_uri = Uri::try_new("file:///config.yaml").unwrap();
        assert!(matches!(
            command,
            CliCommand::Service(ServiceCliCommand {
                config_uri,
                data_dir_path: None,
                services
            })
            if config_uri == expected_config_uri && services.is_empty()
        ));
        Ok(())
    }

    #[test]
    fn test_parse_service_run_indexer_only_ars() -> anyhow::Result<()> {
        let command = build_cli().no_binary_name(true);
        let matches = command.try_get_matches_from(vec![
            "service",
            "run",
            "--service",
            "indexer",
            "--config",
            "/config.yaml",
        ])?;
        let command = CliCommand::parse_cli_args(&matches)?;
        let expected_config_uri = Uri::try_new("file:///config.yaml").unwrap();
        assert!(matches!(
            command,
            CliCommand::Service(ServiceCliCommand {
                config_uri,
                data_dir_path: None,
                services
            })
            if config_uri == expected_config_uri && services.len() == 1 && services.contains(&QService::Indexer)
        ));
        Ok(())
    }
}
