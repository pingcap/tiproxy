// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Linux virtual-IP address operations behind a replaceable owner boundary.

use std::future::Future;
use std::net::IpAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use control_config::VipConfig;
use tokio::process::Command;

const COMMAND_TIMEOUT: Duration = Duration::from_secs(2);

type NetworkFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, NetworkError>> + Send + 'a>>;

/// The VIP actor uses the same interface for a real Linux host and a fake.
pub(super) trait NetworkOperation: Send + Sync {
    fn has_ip(&self) -> NetworkFuture<'_, bool>;
    fn add_ip(&self) -> NetworkFuture<'_, ()>;
    fn delete_ip(&self) -> NetworkFuture<'_, ()>;
    fn send_arp(&self) -> NetworkFuture<'_, ()>;
}

/// Bounded class of one failed Linux VIP operation; no command output leaks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct NetworkError {
    pub operation: &'static str,
    pub class: &'static str,
}

/// IP address and interface configured for this process's VIP owner.
pub(super) struct LinuxNetwork {
    ip: IpAddr,
    prefix_len: u8,
    interface: Arc<str>,
    burst_count: u64,
}

impl LinuxNetwork {
    pub fn new(config: &VipConfig) -> Result<Self, NetworkError> {
        if !cfg!(target_os = "linux") {
            return Err(NetworkError {
                operation: "initialize",
                class: "unsupported_os",
            });
        }
        Ok(Self {
            ip: config.ip,
            prefix_len: config.prefix_len,
            interface: Arc::clone(&config.interface),
            burst_count: config.garp_burst_count,
        })
    }

    fn cidr(&self) -> String {
        format!("{}/{}", self.ip, self.prefix_len)
    }

    async fn ip_output(
        &self,
        args: &[&str],
        operation: &'static str,
    ) -> Result<Output, NetworkError> {
        run_command("ip", args, operation).await
    }

    async fn mutate(&self, verb: &'static str) -> Result<(), NetworkError> {
        let cidr = self.cidr();
        let args = ["addr", verb, cidr.as_str(), "dev", self.interface.as_ref()];
        let output = self.ip_output(&args, verb).await?;
        if output.success {
            return Ok(());
        }
        // Go retries netlink AddrAdd/AddrDel through sudo only for EPERM.
        // The `ip` CLI emits this stable English text with LC_ALL=C.
        if !output.stderr.contains("Operation not permitted")
            && !output.stderr.contains("Permission denied")
        {
            return Err(NetworkError {
                operation: verb,
                class: "ip_failed",
            });
        }
        let sudo_args = [
            "-n",
            "ip",
            "addr",
            verb,
            cidr.as_str(),
            "dev",
            self.interface.as_ref(),
        ];
        let retry = run_command("sudo", &sudo_args, verb).await?;
        retry.success.then_some(()).ok_or(NetworkError {
            operation: verb,
            class: "sudo_ip_failed",
        })
    }
}

impl NetworkOperation for LinuxNetwork {
    fn has_ip(&self) -> NetworkFuture<'_, bool> {
        Box::pin(async move {
            let args = ["-o", "addr", "show", "dev", self.interface.as_ref()];
            let output = self.ip_output(&args, "list").await?;
            if !output.success {
                return Err(NetworkError {
                    operation: "list",
                    class: "ip_failed",
                });
            }
            Ok(contains_exact_address(
                &output.stdout,
                self.ip,
                self.prefix_len,
            ))
        })
    }

    fn add_ip(&self) -> NetworkFuture<'_, ()> {
        Box::pin(async move {
            if self.has_ip().await? {
                return Ok(());
            }
            self.mutate("add").await
        })
    }

    fn delete_ip(&self) -> NetworkFuture<'_, ()> {
        Box::pin(async move {
            if !self.has_ip().await? {
                return Ok(());
            }
            self.mutate("del").await
        })
    }

    fn send_arp(&self) -> NetworkFuture<'_, ()> {
        Box::pin(async move {
            if !self.ip.is_ipv4() {
                return Err(NetworkError {
                    operation: "garp",
                    class: "not_ipv4",
                });
            }
            let ip = self.ip.to_string();
            let args = ["-c", "1", "-U", "-I", self.interface.as_ref(), ip.as_str()];
            for _ in 0..self.burst_count {
                let direct = run_command("arping", &args, "garp").await;
                if matches!(direct, Ok(Output { success: true, .. })) {
                    continue;
                }
                // Go tries both a library GARP and sudo arping for each packet.
                // The CLI path is retained here; a direct arping success or
                // noninteractive sudo success counts as one logical packet.
                let sudo_args = [
                    "-n",
                    "arping",
                    "-c",
                    "1",
                    "-U",
                    "-I",
                    self.interface.as_ref(),
                    ip.as_str(),
                ];
                let sudo = run_command("sudo", &sudo_args, "garp").await?;
                if !sudo.success {
                    return Err(NetworkError {
                        operation: "garp",
                        class: "arping_failed",
                    });
                }
            }
            Ok(())
        })
    }
}

struct Output {
    success: bool,
    stdout: String,
    stderr: String,
}

async fn run_command(
    program: &str,
    args: &[&str],
    operation: &'static str,
) -> Result<Output, NetworkError> {
    let mut command = Command::new(program);
    command.args(args).env("LC_ALL", "C").kill_on_drop(true);
    let output = tokio::time::timeout(COMMAND_TIMEOUT, command.output())
        .await
        .map_err(|_| NetworkError {
            operation,
            class: "timeout",
        })?
        .map_err(|error| NetworkError {
            operation,
            class: if error.kind() == std::io::ErrorKind::NotFound {
                "command_missing"
            } else {
                "command_io"
            },
        })?;
    Ok(Output {
        success: output.status.success(),
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
    })
}

fn contains_exact_address(output: &str, ip: IpAddr, prefix_len: u8) -> bool {
    output.lines().any(|line| {
        let mut words = line.split_whitespace();
        while let Some(word) = words.next() {
            if word != "inet" && word != "inet6" {
                continue;
            }
            let Some((address, prefix)) = words.next().and_then(|value| value.split_once('/'))
            else {
                continue;
            };
            if address.parse::<IpAddr>().ok() == Some(ip)
                && prefix.parse::<u8>().ok() == Some(prefix_len)
            {
                return true;
            }
        }
        false
    })
}

#[cfg(test)]
mod tests {
    use super::contains_exact_address;
    use std::net::IpAddr;

    #[test]
    fn exact_ip_and_prefix_match_is_strict() {
        let output = "2: eth0 inet 192.0.2.5/24 brd 192.0.2.255 scope global eth0\n\
                      2: eth0 inet6 2001:db8::5/64 scope global\n";
        let v4: IpAddr = "192.0.2.5"
            .parse()
            .unwrap_or_else(|error| unreachable!("test IP: {error}"));
        let v6: IpAddr = "2001:db8::5"
            .parse()
            .unwrap_or_else(|error| unreachable!("test IP: {error}"));
        assert!(contains_exact_address(output, v4, 24));
        assert!(!contains_exact_address(output, v4, 32));
        assert!(contains_exact_address(output, v6, 64));
        assert!(!contains_exact_address(output, v6, 128));
    }
}
