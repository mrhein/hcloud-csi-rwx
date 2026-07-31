//! Abstraction over running external commands and spawning long-lived
//! processes.
//!
//! Everything that shells out (`mount`, `mkfs.ext4`, `blkid`, `ganesha.nfsd`)
//! goes through these traits, so the surrounding logic is testable without a
//! privileged container. The traits use boxed futures rather than `async fn`
//! so they stay dyn-compatible — no extra dependency needed.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Result of running a command to completion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Outcome {
    pub success: bool,
    /// Exit code, `None` if the process was killed by a signal.
    pub code: Option<i32>,
    pub stdout: String,
}

impl Outcome {
    pub fn ok() -> Self {
        Self { success: true, code: Some(0), stdout: String::new() }
    }

    pub fn failed(code: i32) -> Self {
        Self { success: false, code: Some(code), stdout: String::new() }
    }

    pub fn with_stdout(mut self, s: &str) -> Self {
        self.stdout = s.to_string();
        self
    }
}

/// Why a command did not complete successfully.
#[derive(Debug)]
pub enum ExecError {
    /// The binary could not be started at all.
    Spawn(String),
    /// The command exceeded its timeout and was killed.
    Timeout { program: String, secs: u64 },
}

impl fmt::Display for ExecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExecError::Spawn(e) => write!(f, "failed to spawn: {e}"),
            ExecError::Timeout { program, secs } => {
                write!(f, "{program} timed out after {secs}s")
            }
        }
    }
}

impl std::error::Error for ExecError {}

/// Runs external commands to completion.
pub trait CommandRunner: Send + Sync + fmt::Debug {
    /// Run `program` with `args`, killing it after `timeout` if set.
    fn run<'a>(
        &'a self,
        program: &'a str,
        args: &'a [String],
        timeout: Option<Duration>,
    ) -> BoxFuture<'a, Result<Outcome, ExecError>>;
}

/// A spawned, still-running process.
pub trait ProcessHandle: Send + fmt::Debug {
    /// True while the process has not exited.
    fn is_running(&mut self) -> bool;
    /// Request termination (SIGKILL).
    fn start_kill(&mut self);
    /// Wait for the process to exit, at most `timeout`.
    fn wait_with_timeout<'a>(&'a mut self, timeout: Duration) -> BoxFuture<'a, bool>;
}

/// Spawns long-lived processes (ganesha.nfsd).
pub trait ProcessSpawner: Send + Sync + fmt::Debug {
    fn spawn(
        &self,
        program: &str,
        args: &[String],
    ) -> Result<Box<dyn ProcessHandle>, ExecError>;
}

// ── Real implementations ──

/// Runs commands via `tokio::process`.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SystemExec;

impl CommandRunner for SystemExec {
    fn run<'a>(
        &'a self,
        program: &'a str,
        args: &'a [String],
        timeout: Option<Duration>,
    ) -> BoxFuture<'a, Result<Outcome, ExecError>> {
        Box::pin(async move {
            let mut cmd = tokio::process::Command::new(program);
            cmd.args(args).kill_on_drop(true);
            let fut = cmd.output();
            let out = match timeout {
                Some(d) => match tokio::time::timeout(d, fut).await {
                    Ok(r) => r,
                    Err(_) => {
                        return Err(ExecError::Timeout {
                            program: program.to_string(),
                            secs: d.as_secs(),
                        })
                    }
                },
                None => fut.await,
            }
            .map_err(|e| ExecError::Spawn(e.to_string()))?;

            Ok(Outcome {
                success: out.status.success(),
                code: out.status.code(),
                stdout: String::from_utf8_lossy(&out.stdout).into_owned(),
            })
        })
    }
}

#[derive(Debug)]
struct TokioChild(tokio::process::Child);

impl ProcessHandle for TokioChild {
    fn is_running(&mut self) -> bool {
        self.0.try_wait().ok().flatten().is_none()
    }

    fn start_kill(&mut self) {
        let _ = self.0.start_kill();
    }

    fn wait_with_timeout<'a>(&'a mut self, timeout: Duration) -> BoxFuture<'a, bool> {
        Box::pin(async move { matches!(tokio::time::timeout(timeout, self.0.wait()).await, Ok(Ok(_))) })
    }
}

impl ProcessSpawner for SystemExec {
    fn spawn(
        &self,
        program: &str,
        args: &[String],
    ) -> Result<Box<dyn ProcessHandle>, ExecError> {
        let child = tokio::process::Command::new(program)
            .args(args)
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| ExecError::Spawn(e.to_string()))?;
        Ok(Box::new(TokioChild(child)))
    }
}

// ── Test doubles ──

/// One recorded invocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Invocation {
    pub program: String,
    pub args: Vec<String>,
}

impl Invocation {
    pub fn joined(&self) -> String {
        format!("{} {}", self.program, self.args.join(" "))
    }
}

type Rule = Box<dyn Fn(&Invocation) -> Option<Result<Outcome, ExecError>> + Send + Sync>;

/// Scriptable [`CommandRunner`] / [`ProcessSpawner`] for tests: records every
/// invocation and answers from rules, defaulting to success.
#[derive(Default)]
pub struct FakeExec {
    calls: Arc<Mutex<Vec<Invocation>>>,
    rules: Arc<Mutex<Vec<Rule>>>,
    spawn_fails: Arc<Mutex<bool>>,
    /// How many `is_running()` calls a spawned process reports as alive.
    alive_polls: Arc<Mutex<usize>>,
}

impl fmt::Debug for FakeExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FakeExec").field("calls", &self.calls()).finish()
    }
}

impl FakeExec {
    pub fn new() -> Self {
        Self {
            alive_polls: Arc::new(Mutex::new(usize::MAX)),
            ..Default::default()
        }
    }

    /// Record of every command run so far.
    pub fn calls(&self) -> Vec<Invocation> {
        self.calls.lock().unwrap().clone()
    }

    /// Flattened `"program arg arg"` strings — handy for assertions.
    pub fn call_lines(&self) -> Vec<String> {
        self.calls().iter().map(Invocation::joined).collect()
    }

    pub fn ran(&self, needle: &str) -> bool {
        self.call_lines().iter().any(|c| c.contains(needle))
    }

    /// Answer invocations whose joined form contains `needle` with `outcome`.
    pub fn on(self, needle: &str, outcome: Outcome) -> Self {
        let n = needle.to_string();
        self.rules.lock().unwrap().push(Box::new(move |inv| {
            inv.joined().contains(&n).then(|| Ok(outcome.clone()))
        }));
        self
    }

    /// Make invocations matching `needle` fail to spawn.
    pub fn on_error(self, needle: &str) -> Self {
        let n = needle.to_string();
        self.rules.lock().unwrap().push(Box::new(move |inv| {
            inv.joined()
                .contains(&n)
                .then(|| Err(ExecError::Spawn("fake spawn failure".into())))
        }));
        self
    }

    /// Let `spawn()` fail.
    pub fn failing_spawn(self) -> Self {
        *self.spawn_fails.lock().unwrap() = true;
        self
    }

    /// Spawned processes report "alive" for `n` polls, then exit.
    pub fn alive_for(self, n: usize) -> Self {
        *self.alive_polls.lock().unwrap() = n;
        self
    }

    fn record(&self, program: &str, args: &[String]) -> Invocation {
        let inv = Invocation { program: program.to_string(), args: args.to_vec() };
        self.calls.lock().unwrap().push(inv.clone());
        inv
    }
}

impl CommandRunner for FakeExec {
    fn run<'a>(
        &'a self,
        program: &'a str,
        args: &'a [String],
        _timeout: Option<Duration>,
    ) -> BoxFuture<'a, Result<Outcome, ExecError>> {
        let inv = self.record(program, args);
        Box::pin(async move {
            for rule in self.rules.lock().unwrap().iter() {
                if let Some(r) = rule(&inv) {
                    return r;
                }
            }
            Ok(Outcome::ok())
        })
    }
}

#[derive(Debug)]
struct FakeChild {
    remaining: usize,
    killed: Arc<Mutex<bool>>,
}

impl ProcessHandle for FakeChild {
    fn is_running(&mut self) -> bool {
        if *self.killed.lock().unwrap() || self.remaining == 0 {
            return false;
        }
        if self.remaining != usize::MAX {
            self.remaining -= 1;
        }
        true
    }

    fn start_kill(&mut self) {
        *self.killed.lock().unwrap() = true;
    }

    fn wait_with_timeout<'a>(&'a mut self, _t: Duration) -> BoxFuture<'a, bool> {
        Box::pin(async { true })
    }
}

impl ProcessSpawner for FakeExec {
    fn spawn(
        &self,
        program: &str,
        args: &[String],
    ) -> Result<Box<dyn ProcessHandle>, ExecError> {
        self.record(program, args);
        if *self.spawn_fails.lock().unwrap() {
            return Err(ExecError::Spawn("fake spawn failure".into()));
        }
        Ok(Box::new(FakeChild {
            remaining: *self.alive_polls.lock().unwrap(),
            killed: Arc::new(Mutex::new(false)),
        }))
    }
}

/// Convenience for building argument vectors.
pub fn args(list: &[&str]) -> Vec<String> {
    list.iter().map(|s| s.to_string()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn fake_records_and_defaults_to_success() {
        let fake = FakeExec::new();
        let out = fake.run("mount", &args(&["-t", "nfs4"]), None).await.unwrap();
        assert!(out.success);
        assert_eq!(fake.call_lines(), vec!["mount -t nfs4"]);
        assert!(fake.ran("nfs4"));
    }

    #[tokio::test]
    async fn fake_rules_match_by_substring() {
        let fake = FakeExec::new().on("blkid", Outcome::failed(2));
        assert!(!fake.run("blkid", &args(&["/dev/sda"]), None).await.unwrap().success);
        assert!(fake.run("mount", &args(&["/dev/sda"]), None).await.unwrap().success);
    }

    #[tokio::test]
    async fn fake_can_fail_to_spawn() {
        let fake = FakeExec::new().on_error("mkfs");
        assert!(fake.run("mkfs.ext4", &args(&["-F"]), None).await.is_err());
    }

    #[test]
    fn fake_process_lifecycle() {
        let fake = FakeExec::new().alive_for(2);
        let mut p = fake.spawn("ganesha.nfsd", &args(&["-F"])).unwrap();
        assert!(p.is_running());
        assert!(p.is_running());
        assert!(!p.is_running(), "exits after the scripted number of polls");
        assert!(fake.ran("ganesha.nfsd"));
    }

    #[test]
    fn fake_process_kill_stops_it() {
        let fake = FakeExec::new();
        let mut p = fake.spawn("ganesha.nfsd", &[]).unwrap();
        assert!(p.is_running());
        p.start_kill();
        assert!(!p.is_running());
    }

    #[test]
    fn fake_spawn_failure() {
        let fake = FakeExec::new().failing_spawn();
        assert!(fake.spawn("ganesha.nfsd", &[]).is_err());
    }

    #[tokio::test]
    async fn system_exec_runs_and_reports_failure() {
        let e = SystemExec;
        let ok = e.run("true", &[], None).await.unwrap();
        assert!(ok.success);
        let bad = e.run("false", &[], None).await.unwrap();
        assert!(!bad.success);
        let out = e.run("echo", &args(&["hi"]), None).await.unwrap();
        assert_eq!(out.stdout.trim(), "hi");
        assert!(matches!(
            e.run("definitely-not-a-real-binary-xyz", &[], None).await,
            Err(ExecError::Spawn(_))
        ));
    }

    #[tokio::test]
    async fn system_exec_honours_timeout() {
        let e = SystemExec;
        let r = e
            .run("sleep", &args(&["5"]), Some(Duration::from_millis(150)))
            .await;
        assert!(matches!(r, Err(ExecError::Timeout { .. })), "got {r:?}");
    }

    #[test]
    fn outcome_helpers_and_error_display() {
        assert!(Outcome::ok().success);
        assert_eq!(Outcome::failed(3).code, Some(3));
        assert_eq!(Outcome::ok().with_stdout("x").stdout, "x");
        assert!(ExecError::Spawn("boom".into()).to_string().contains("boom"));
        assert!(
            ExecError::Timeout { program: "mount".into(), secs: 5 }
                .to_string()
                .contains("mount")
        );
    }
}

#[cfg(test)]
mod system_process_tests {
    use super::*;

    #[tokio::test]
    async fn system_spawner_runs_and_reports_a_real_process() {
        let e = SystemExec;
        let mut p = e.spawn("sleep", &args(&["30"])).unwrap();
        assert!(p.is_running());
        p.start_kill();
        assert!(p.wait_with_timeout(Duration::from_secs(5)).await);
        assert!(!p.is_running());
        assert!(format!("{p:?}").contains("TokioChild"));
    }

    #[tokio::test]
    async fn system_spawner_sees_a_finished_process() {
        let e = SystemExec;
        let mut p = e.spawn("true", &[]).unwrap();
        assert!(p.wait_with_timeout(Duration::from_secs(5)).await);
        assert!(!p.is_running());
    }

    #[tokio::test]
    async fn system_spawner_reports_missing_binaries() {
        let e = SystemExec;
        assert!(matches!(
            e.spawn("definitely-not-a-real-binary-xyz", &[]),
            Err(ExecError::Spawn(_))
        ));
    }

    #[tokio::test]
    async fn wait_with_timeout_gives_up() {
        let e = SystemExec;
        let mut p = e.spawn("sleep", &args(&["30"])).unwrap();
        assert!(!p.wait_with_timeout(Duration::from_millis(100)).await);
        p.start_kill();
    }

    #[test]
    fn fake_debug_shows_recorded_calls() {
        let fake = FakeExec::new();
        assert!(format!("{fake:?}").contains("FakeExec"));
        assert!(format!("{:?}", Invocation { program: "x".into(), args: vec![] }).contains('x'));
        assert_eq!(SystemExec.clone(), SystemExec);
    }
}
