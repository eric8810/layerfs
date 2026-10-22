//! Open an installed Store in this process and run the public Store table,
//! plus the Tier 1 Workspace calls over the shared session layer.

use crate::config::{Config, Deploy};
use crate::error::LayerFsError;
use crate::value;
use layerfs_bridge::control::{Answer as ControlAnswer, Reply, SdkValue};
use layerfs_bridge::provision::{StoreManifest, StoreProfile};
use serde_json::Value;
use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};

const RECORD_LIMIT: u64 = 65_535;
/// Bytes per spooled-output page while filling a sink.
const PAGE: usize = 64 * 1024;

const STORE_METHODS: &[&str] = &[
    "project.list_branches",
    "fs.read",
    "fs.list",
    "fs.stat",
    "fs.grep",
    "fs.glob",
    "fs.diff_files",
    "fs.blame",
    "fs.write",
    "fs.edit",
    "fs.apply",
    "fs.mkdir",
    "fs.rm",
    "fs.cp",
    "fs.mv",
    "branch.fork",
    "branch.list_commits",
    "branch.diff_commits",
    "branch.diff_branches",
    "branch.merge",
    "branch.publish",
];

/// Public integration state for one project.
#[derive(Clone, Debug)]
pub struct Info {
    /// LayerFS project name.
    pub project: String,
}

#[derive(Default)]
struct State {
    closed: bool,
    inflight: usize,
}

struct Inner {
    config: Config,
    opening: Mutex<()>,
    opened: AtomicBool,
    state: Mutex<State>,
    drained: tokio::sync::Notify,
    /// One lane per `{sandbox, path}`; weak entries are pruned on lookup.
    lanes: Mutex<HashMap<String, Weak<tokio::sync::Mutex<()>>>>,
}

/// One call that `close` waits for before releasing the Store.
struct InFlight {
    inner: Arc<Inner>,
}

impl Drop for InFlight {
    fn drop(&mut self) {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.inflight -= 1;
        let drained = state.inflight == 0;
        drop(state);
        if drained {
            self.inner.drained.notify_waiters();
        }
    }
}

/// One project's Store and Workspace service. The Store is opened lazily on
/// the first call; `close` disables new calls and releases it.
#[derive(Clone)]
pub struct LayerFs {
    inner: Arc<Inner>,
}

impl LayerFs {
    /// Validates configuration; performs no Store I/O.
    pub fn new(config: Config) -> Result<Self, LayerFsError> {
        config.validate()?;
        Ok(Self {
            inner: Arc::new(Inner {
                config,
                opening: Mutex::new(()),
                opened: AtomicBool::new(false),
                state: Mutex::new(State::default()),
                drained: tokio::sync::Notify::new(),
                lanes: Mutex::new(HashMap::new()),
            }),
        })
    }

    /// The project name. Does not open the Store and remains available after
    /// `close`.
    pub async fn info(&self) -> Info {
        Info {
            project: self.inner.config.project.clone(),
        }
    }

    /// Runs one public Store method once.
    pub async fn call(&self, method: String, args: Value) -> Result<Value, LayerFsError> {
        let inflight = self.enter()?;
        let method = exact_method(&method)?;
        let args = self.session_args(args)?;
        let inner = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || {
            let _inflight = inflight;
            ensure_store(&inner)?;
            let args = with_project(args, &inner.config.project)?;
            let value = layerfs_session::dispatch(method, args)?;
            Ok(value::to_json(&value))
        })
        .await
        .map_err(|error| LayerFsError::Join(error.to_string()))?
    }

    /// `workspace_mount({ branch })`: a new Workspace of the Branch.
    pub async fn workspace_mount(&self, args: Value) -> Result<Value, LayerFsError> {
        self.workspace_call("workspace.mount", args, true, false)
            .await
    }

    /// `workspace_status({ sandbox, path })`.
    pub async fn workspace_status(&self, args: Value) -> Result<Value, LayerFsError> {
        self.workspace_call("workspace.status", args, true, true)
            .await
    }

    /// `workspace_commit({ sandbox, path, message?, force? })`.
    pub async fn workspace_commit(&self, args: Value) -> Result<Value, LayerFsError> {
        self.workspace_call("workspace.commit", args, false, true)
            .await
    }

    /// `workspace_unmount({ sandbox, path, discard? })`.
    pub async fn workspace_unmount(&self, args: Value) -> Result<Value, LayerFsError> {
        self.workspace_call("workspace.unmount", args, false, true)
            .await
    }

    /// `workspace_exec({ sandbox, path, command, message?, sink? })`.
    ///
    /// Without `sink` the SDK result is returned unchanged; a spooled stream
    /// is released and refused because Tier 1 has no page method. With
    /// `sink` (`{ stdout?: path, stderr?: path }`, distinct absolute paths),
    /// the files are opened before the command runs, every page is written in
    /// order after it ends, and the result carries the byte counts. Output is
    /// spooled while the command runs; the sink does not hold it back.
    pub async fn workspace_exec(&self, args: Value) -> Result<Value, LayerFsError> {
        let inflight = self.enter()?;
        self.ensure_deploy("workspace.exec")?;
        check_exec_args(&args)?;
        let lane = self.lane(&args);
        let lane = lane.lock_owned().await;
        let (args, sink) = take_sink(args)?;
        let args = self.session_args(args)?;
        tokio::task::spawn_blocking(move || {
            let _inflight = inflight;
            let _lane = lane;
            match sink {
                None => {
                    let value = layerfs_session::dispatch("workspace.exec", args)?;
                    if let Some(SdkValue::Text(output)) = value.get("output") {
                        let release = SdkValue::map([("output", SdkValue::Text(output.clone()))]);
                        let _ = layerfs_session::dispatch("workspace.exec_release", release);
                        return Err(LayerFsError::invalid(
                            "workspace.exec output exceeds the inline window; use a sink",
                        ));
                    }
                    Ok(value::to_json(&value))
                }
                Some(sink) => exec_into_sink(args, sink),
            }
        })
        .await
        .map_err(|error| LayerFsError::Join(error.to_string()))?
    }

    /// Disables new calls and releases the Store when the last in-flight call
    /// ends. Idempotent.
    pub async fn close(&self) -> Result<(), LayerFsError> {
        {
            let mut state = self.state();
            state.closed = true;
        }
        // Calls that entered before the flag finish before the Store goes.
        wait_drained(&self.inner.state, &self.inner.drained).await;
        let inner = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || {
            let _opening = inner
                .opening
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if !inner.opened.swap(false, Ordering::AcqRel) {
                return Ok(());
            }
            release_store(&inner.config)
        })
        .await
        .map_err(|error| LayerFsError::Join(error.to_string()))?
    }

    async fn workspace_call(
        &self,
        method: &'static str,
        args: Value,
        store: bool,
        lane: bool,
    ) -> Result<Value, LayerFsError> {
        let inflight = self.enter()?;
        self.ensure_deploy(method)?;
        let lane = if lane { Some(self.lane(&args)) } else { None };
        let lane = match lane {
            Some(lane) => Some(lane.lock_owned().await),
            None => None,
        };
        let args = self.session_args(args)?;
        let inner = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || {
            let _inflight = inflight;
            let _lane = lane;
            if store {
                ensure_store(&inner)?;
            }
            let value = layerfs_session::dispatch(method, args)?;
            Ok(value::to_json(&value))
        })
        .await
        .map_err(|error| LayerFsError::Join(error.to_string()))?
    }

    fn ensure_deploy(&self, method: &str) -> Result<(), LayerFsError> {
        if self.inner.config.deploy.is_none() {
            return Err(LayerFsError::invalid(format!(
                "{method} needs deploy.docker, deploy.image, deploy.volume and deploy.daemon"
            )));
        }
        Ok(())
    }

    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.inner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Marks one call in flight; `close` waits for every such call.
    fn enter(&self) -> Result<InFlight, LayerFsError> {
        let mut state = self.state();
        if state.closed {
            return Err(LayerFsError::invalid("layerfs service is closed"));
        }
        state.inflight += 1;
        Ok(InFlight {
            inner: Arc::clone(&self.inner),
        })
    }

    /// One lane per `{sandbox, path}`; different Workspaces run in parallel.
    fn lane(&self, args: &Value) -> Arc<tokio::sync::Mutex<()>> {
        let sandbox = args.get("sandbox").and_then(Value::as_str).unwrap_or("");
        let path = args.get("path").and_then(Value::as_str).unwrap_or("");
        let key = format!("{sandbox}\u{0}{path}");
        let mut lanes = self
            .inner
            .lanes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        lanes.retain(|_, lane| lane.strong_count() > 0);
        if let Some(lane) = lanes.get(&key).and_then(Weak::upgrade) {
            return lane;
        }
        let lane = Arc::new(tokio::sync::Mutex::new(()));
        lanes.insert(key, Arc::downgrade(&lane));
        lane
    }

    fn session_args(&self, args: Value) -> Result<SdkValue, LayerFsError> {
        let SdkValue::Map(mut fields) = value::to_sdk(args)? else {
            return Err(LayerFsError::invalid("arguments must be an object"));
        };
        let config = &self.inner.config;
        let fixed = [
            ("project", SdkValue::Text(config.project.clone())),
            ("home", SdkValue::Text(config.home.display().to_string())),
            (
                "storePath",
                SdkValue::Text(config.store_path.display().to_string()),
            ),
        ];
        for (name, value) in fixed {
            set(&mut fields, name, value);
        }
        if let Some(deploy) = &config.deploy {
            set(&mut fields, "deploy", deploy_value(deploy));
        }
        Ok(SdkValue::Map(fields))
    }
}

/// Waits until no call entered before the close; registration precedes the
/// check so a drain notice cannot be lost between them.
async fn wait_drained(state: &Mutex<State>, drained: &tokio::sync::Notify) {
    loop {
        let notified = drained.notified();
        let mut notified = std::pin::pin!(notified);
        notified.as_mut().enable();
        if state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .inflight
            == 0
        {
            break;
        }
        notified.await;
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        if let Ok(state) = self.state.get_mut() {
            state.closed = true;
        }
        if !self.opened.swap(false, Ordering::AcqRel) {
            return;
        }
        let config = self.config.clone();
        let job = move || {
            let _ = release_store(&config);
        };
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                drop(handle.spawn_blocking(job));
            }
            Err(_) => job(),
        }
    }
}

/// Opens the Store once per plugin, inside the call's blocking task.
fn ensure_store(inner: &Inner) -> Result<(), LayerFsError> {
    if inner.opened.load(Ordering::Acquire) {
        return Ok(());
    }
    let _opening = inner
        .opening
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if inner.opened.load(Ordering::Acquire) {
        return Ok(());
    }
    open_store(&inner.config)?;
    inner.opened.store(true, Ordering::Release);
    Ok(())
}

fn open_store(config: &Config) -> Result<(), LayerFsError> {
    read_record(config)?;
    let args = base_args(config);
    layerfs_session::acquire(&args)?;
    Ok(())
}

fn release_store(config: &Config) -> Result<(), LayerFsError> {
    let args = base_args(config);
    layerfs_session::release(&args)?;
    Ok(())
}

fn base_args(config: &Config) -> SdkValue {
    let mut fields = vec![
        ("project".to_owned(), SdkValue::Text(config.project.clone())),
        (
            "home".to_owned(),
            SdkValue::Text(config.home.display().to_string()),
        ),
        (
            "storePath".to_owned(),
            SdkValue::Text(config.store_path.display().to_string()),
        ),
    ];
    if let Some(deploy) = &config.deploy {
        fields.push(("deploy".to_owned(), deploy_value(deploy)));
    }
    SdkValue::Map(fields)
}

fn deploy_value(deploy: &Deploy) -> SdkValue {
    let mut fields = vec![
        (
            "docker".to_owned(),
            SdkValue::Text(deploy.docker.display().to_string()),
        ),
        ("image".to_owned(), SdkValue::Text(deploy.image.clone())),
        ("volume".to_owned(), SdkValue::Text(deploy.volume.clone())),
        (
            "daemon".to_owned(),
            SdkValue::Text(deploy.daemon.display().to_string()),
        ),
    ];
    if let Some(host) = &deploy.daemon_host {
        fields.push(("daemonHost".to_owned(), SdkValue::Text(host.clone())));
    }
    if let Some(prefix) = &deploy.container_prefix {
        fields.push(("containerPrefix".to_owned(), SdkValue::Text(prefix.clone())));
    }
    SdkValue::Map(fields)
}

fn set(fields: &mut Vec<(String, SdkValue)>, name: &str, value: SdkValue) {
    match fields.iter_mut().find(|(known, _)| known == name) {
        Some((_, current)) => *current = value,
        None => fields.push((name.to_owned(), value)),
    }
}

fn exact_method(method: &str) -> Result<&'static str, LayerFsError> {
    STORE_METHODS
        .iter()
        .find(|known| **known == method)
        .copied()
        .ok_or_else(|| {
            LayerFsError::refused(
                "UnknownMethod",
                format!("no Store method {method}"),
                Value::Null,
            )
        })
}

fn with_project(args: SdkValue, project: &str) -> Result<SdkValue, LayerFsError> {
    let SdkValue::Map(mut fields) = args else {
        return Err(LayerFsError::invalid("arguments must be an object"));
    };
    set(&mut fields, "project", SdkValue::Text(project.to_owned()));
    Ok(SdkValue::Map(fields))
}

struct Record;

fn read_record(config: &Config) -> Result<Record, LayerFsError> {
    let project = config.home.join(&config.project);
    let path = project.join("project.record");
    let bytes = read_private(&path)?;
    crate::config::private_dir(&config.home)?;
    crate::config::private_dir(&project)?;
    let answer =
        ControlAnswer::decode(&bytes).map_err(|error| LayerFsError::Control(error.to_string()))?;
    let Reply::Sdk(value) = answer.reply else {
        return Err(LayerFsError::invalid("project record is not an SDK value"));
    };
    let SdkValue::Map(fields) = value else {
        return Err(LayerFsError::invalid("project record is not a map"));
    };
    let manifest = decode_manifest(manifest_bytes(&fields)?)?;
    manifest
        .check()
        .map_err(|error| LayerFsError::Control(error.into()))?;
    if manifest.profile != StoreProfile::Disposable {
        return Err(LayerFsError::invalid("Disposable Store required"));
    }
    if manifest.daemon_sqlite.is_some() {
        return Err(LayerFsError::Control(
            "sealed manifest already names a daemon SQLite".into(),
        ));
    }
    let installed = match fields
        .iter()
        .find(|(name, _)| name == "installed")
        .map(|(_, value)| value)
    {
        None | Some(SdkValue::Null) => {
            return Err(LayerFsError::refused(
                "NoStore",
                "the project has no installed Store",
                Value::Null,
            ));
        }
        Some(SdkValue::Bytes(bytes)) if !bytes.is_empty() => decode_manifest(bytes)?,
        Some(SdkValue::Bytes(_)) => {
            return Err(LayerFsError::invalid(
                "the installed manifest is an empty value",
            ));
        }
        Some(_) => {
            return Err(LayerFsError::invalid(
                "the installed manifest has the wrong type",
            ));
        }
    };
    installed
        .check()
        .map_err(|error| LayerFsError::Control(error.into()))?;
    if installed.daemon_sqlite.is_none() {
        return Err(LayerFsError::Control(
            "the installed manifest has no daemon SQLite".into(),
        ));
    }
    let mut expected = manifest.clone();
    expected.daemon_sqlite = installed.daemon_sqlite.clone();
    if installed != expected {
        return Err(LayerFsError::Control(
            "installed manifest does not match the project".into(),
        ));
    }
    Ok(Record)
}

fn manifest_bytes(fields: &[(String, SdkValue)]) -> Result<&[u8], LayerFsError> {
    match fields
        .iter()
        .find(|(name, _)| name == "manifest")
        .map(|(_, value)| value)
    {
        Some(SdkValue::Bytes(bytes)) if !bytes.is_empty() => Ok(bytes),
        _ => Err(LayerFsError::refused(
            "NoSuchProject",
            "project record has no sealed manifest",
            Value::Null,
        )),
    }
}

fn decode_manifest(bytes: &[u8]) -> Result<StoreManifest, LayerFsError> {
    StoreManifest::decode(bytes).map_err(|error| LayerFsError::Control(error.to_string()))
}

fn read_private(path: &Path) -> Result<Vec<u8>, LayerFsError> {
    let meta = match std::fs::symlink_metadata(path) {
        Ok(meta) => meta,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(LayerFsError::refused(
                "NoSuchProject",
                format!("no project record at {}", path.display()),
                Value::Null,
            ));
        }
        Err(error) => return Err(error.into()),
    };
    if meta.file_type().is_symlink() || !meta.is_file() {
        return Err(LayerFsError::refused(
            "Unauthorized",
            format!("{} must be a regular file", path.display()),
            Value::Null,
        ));
    }
    if meta.len() > RECORD_LIMIT {
        return Err(LayerFsError::invalid("project record is too large"));
    }
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
        // SAFETY: geteuid has no preconditions.
        let owner = unsafe { libc::geteuid() };
        if meta.uid() != owner || meta.mode() & 0o077 != 0 {
            return Err(LayerFsError::refused(
                "Unauthorized",
                format!("{} must be private", path.display()),
                Value::Null,
            ));
        }
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(path)?;
        let mut bytes = Vec::new();
        std::io::Read::by_ref(&mut file)
            .take(RECORD_LIMIT + 1)
            .read_to_end(&mut bytes)?;
        if bytes.len() as u64 > RECORD_LIMIT {
            return Err(LayerFsError::invalid("project record is too large"));
        }
        Ok(bytes)
    }
    #[cfg(not(target_os = "linux"))]
    {
        let mut bytes = Vec::new();
        std::fs::File::open(path)?
            .take(RECORD_LIMIT + 1)
            .read_to_end(&mut bytes)?;
        if bytes.len() as u64 > RECORD_LIMIT {
            return Err(LayerFsError::invalid("project record is too large"));
        }
        Ok(bytes)
    }
}

/// One exec sink: absolute files that receive the streams in order.
#[derive(Debug)]
struct Sink {
    stdout: Option<PathBuf>,
    stderr: Option<PathBuf>,
}

/// Sink files opened before the command runs, so a bad sink fails first.
#[derive(Debug)]
struct OpenedSink {
    stdout: Option<File>,
    stderr: Option<File>,
}

impl Sink {
    fn open(&self) -> Result<OpenedSink, LayerFsError> {
        if matches!((&self.stdout, &self.stderr), (Some(out), Some(err)) if out == err) {
            return Err(LayerFsError::invalid(
                "sink.stdout and sink.stderr must be different files",
            ));
        }
        let opened = OpenedSink {
            stdout: self.stdout.as_deref().map(open_sink).transpose()?,
            stderr: self.stderr.as_deref().map(open_sink).transpose()?,
        };
        #[cfg(unix)]
        if let (Some(out), Some(err)) = (&opened.stdout, &opened.stderr) {
            use std::os::unix::fs::MetadataExt;
            let (out, err) = (out.metadata()?, err.metadata()?);
            if out.dev() == err.dev() && out.ino() == err.ino() {
                return Err(LayerFsError::invalid(
                    "sink.stdout and sink.stderr must be different files",
                ));
            }
        }
        // A hard-linked pair is rejected above before either file is cleared.
        for file in [&opened.stdout, &opened.stderr].into_iter().flatten() {
            file.set_len(0)?;
        }
        Ok(opened)
    }
}

fn open_sink(path: &Path) -> Result<File, LayerFsError> {
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
        // A FIFO or device must not block the worker before its type is known.
        #[cfg(target_os = "linux")]
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    let file = match options.open(path) {
        Ok(file) => file,
        Err(error) if not_regular_open(&error) => {
            return Err(LayerFsError::invalid(
                "a sink path must name a regular file",
            ))
        }
        Err(error) => return Err(error.into()),
    };
    let meta = file.metadata()?;
    if !meta.is_file() {
        return Err(LayerFsError::invalid(
            "a sink path must name a regular file",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        if meta.nlink() > 1 {
            return Err(LayerFsError::invalid("a sink path must not be a hard link"));
        }
        file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
    }
    Ok(file)
}

/// A FIFO or device that refuses a non-blocking writer before its type can be
/// inspected.
#[cfg(target_os = "linux")]
fn not_regular_open(error: &std::io::Error) -> bool {
    matches!(
        error.raw_os_error(),
        Some(libc::ENXIO)
            | Some(libc::ENODEV)
            | Some(libc::ELOOP)
            | Some(libc::EISDIR)
            | Some(libc::ENOENT)
            | Some(libc::ENOTDIR)
    )
}

#[cfg(not(target_os = "linux"))]
fn not_regular_open(_: &std::io::Error) -> bool {
    false
}

/// Releases a spooled output once, even when filling a sink fails.
struct OutputGuard(Option<String>);

impl OutputGuard {
    fn new(output: Option<String>) -> Self {
        Self(output)
    }

    fn id(&self) -> Option<&str> {
        self.0.as_deref()
    }

    fn release(&mut self) -> Result<(), LayerFsError> {
        let Some(output) = self.0.take() else {
            return Ok(());
        };
        let release = SdkValue::map([("output", SdkValue::Text(output))]);
        layerfs_session::dispatch("workspace.exec_release", release)?;
        Ok(())
    }
}

impl Drop for OutputGuard {
    fn drop(&mut self) {
        if let Some(output) = self.0.take() {
            let release = SdkValue::map([("output", SdkValue::Text(output))]);
            let _ = layerfs_session::dispatch("workspace.exec_release", release);
        }
    }
}

fn take_sink(args: Value) -> Result<(Value, Option<Sink>), LayerFsError> {
    let Value::Object(mut fields) = args else {
        return Err(LayerFsError::invalid("arguments must be an object"));
    };
    let Some(sink) = fields.remove("sink") else {
        return Ok((Value::Object(fields), None));
    };
    if sink.is_null() {
        return Ok((Value::Object(fields), None));
    }
    let Value::Object(streams) = sink else {
        return Err(LayerFsError::invalid(
            "sink must be an object with stdout/stderr paths",
        ));
    };
    let path = |name: &str| -> Result<Option<PathBuf>, LayerFsError> {
        match streams.get(name) {
            None | Some(Value::Null) => Ok(None),
            Some(Value::String(path)) => {
                let path = PathBuf::from(path);
                if !path.is_absolute() {
                    return Err(LayerFsError::invalid(format!(
                        "sink.{name} must be absolute"
                    )));
                }
                Ok(Some(path))
            }
            Some(_) => Err(LayerFsError::invalid(format!("sink.{name} must be a path"))),
        }
    };
    let sink = Sink {
        stdout: path("stdout")?,
        stderr: path("stderr")?,
    };
    if sink.stdout.is_none() && sink.stderr.is_none() {
        return Err(LayerFsError::invalid("sink needs a stdout or stderr path"));
    }
    if matches!((&sink.stdout, &sink.stderr), (Some(out), Some(err)) if out == err) {
        return Err(LayerFsError::invalid(
            "sink.stdout and sink.stderr must be different files",
        ));
    }
    Ok((Value::Object(fields), Some(sink)))
}

/// Validates the text every exec needs before a sink is opened, so a
/// malformed call cannot truncate the caller's files.
fn check_exec_args(args: &Value) -> Result<(), LayerFsError> {
    let Value::Object(fields) = args else {
        return Err(LayerFsError::invalid("arguments must be an object"));
    };
    for name in ["sandbox", "path", "command"] {
        match fields.get(name) {
            Some(Value::String(text)) if !text.is_empty() => {}
            _ => {
                return Err(LayerFsError::invalid(format!(
                    "{name} must be a non-empty string"
                )))
            }
        }
    }
    if fields
        .get("background")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return Err(LayerFsError::invalid(
            "background launch requires the operator process table",
        ));
    }
    if let Some(message) = fields.get("message") {
        match message {
            Value::Null => {}
            Value::String(text) => {
                let mut meta = layerfs_history::CommitMeta::new(layerfs_history::CommitVia::Files);
                meta.message = Some(text.clone());
                meta.check()
                    .map_err(|error| LayerFsError::invalid(error.to_string()))?;
            }
            _ => return Err(LayerFsError::invalid("message must be text")),
        }
    }
    Ok(())
}

fn exec_into_sink(args: SdkValue, sink: Sink) -> Result<Value, LayerFsError> {
    let SdkValue::Map(mut fields) = args else {
        return Err(LayerFsError::invalid("arguments must be an object"));
    };
    // Resolve the Sandbox and mount before any sink file is touched.
    layerfs_session::exec_preflight(&SdkValue::Map(fields.clone()))?;
    let mut files = sink.open()?;
    set(&mut fields, "spool", SdkValue::Bool(true));
    let result = layerfs_session::dispatch("workspace.exec", SdkValue::Map(fields))?;
    let SdkValue::Map(fields) = result else {
        return Err(LayerFsError::invalid("workspace.exec returned no result"));
    };
    let output = match fields.iter().find(|(name, _)| name == "output") {
        Some((_, SdkValue::Text(output))) => Some(output.clone()),
        _ => None,
    };
    let mut output = OutputGuard::new(output);
    let mut streams = Vec::new();
    for (name, file, inline) in [
        ("stdout", files.stdout.as_mut(), "stdout"),
        ("stderr", files.stderr.as_mut(), "stderr"),
    ] {
        let Some(file) = file else {
            continue;
        };
        let inline = match fields.iter().find(|(key, _)| key == inline) {
            Some((_, SdkValue::Text(text))) => Some(text.as_str()),
            _ => None,
        };
        let bytes = write_stream(
            file,
            inline,
            output.id(),
            name,
            output_size(&fields, name),
            |args, page| layerfs_session::exec_output(args, page).map_err(Into::into),
        )?;
        let path = match name {
            "stdout" => sink.stdout.as_deref(),
            _ => sink.stderr.as_deref(),
        }
        .expect("requested stream has a path");
        streams.push((
            name.to_owned(),
            SdkValue::map([
                ("path", SdkValue::text(path.display().to_string())),
                (
                    "bytes",
                    SdkValue::Int(i64::try_from(bytes).unwrap_or(i64::MAX)),
                ),
            ]),
        ));
    }
    output.release()?;
    let field = |name: &str| {
        fields
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.clone())
    };
    let mut answer = Vec::new();
    for name in ["kind", "exitCode", "message", "truncated"] {
        if let Some(value) = field(name) {
            answer.push((name.to_owned(), value));
        }
    }
    for name in ["stdoutBytes", "stderrBytes"] {
        if let Some(value) = field(name) {
            answer.push((name.to_owned(), value));
        }
    }
    answer.push(("sink".to_owned(), SdkValue::Map(streams)));
    Ok(value::to_json(&SdkValue::Map(answer)))
}

/// The byte count a spooled exec result carries for `name` (`stdoutBytes`).
fn output_size(fields: &[(String, SdkValue)], name: &str) -> Option<u64> {
    let key = format!("{name}Bytes");
    match fields
        .iter()
        .find(|(field, _)| *field == key)
        .map(|(_, value)| value)
    {
        Some(SdkValue::Int(value)) => u64::try_from(*value).ok(),
        _ => None,
    }
}

fn write_stream<F>(
    file: &mut File,
    inline: Option<&str>,
    output: Option<&str>,
    stream: &str,
    size: Option<u64>,
    mut read_page: F,
) -> Result<u64, LayerFsError>
where
    F: FnMut(&SdkValue, &mut [u8]) -> Result<SdkValue, LayerFsError>,
{
    if let Some(text) = inline {
        file.write_all(text.as_bytes())?;
        let written = text.len() as u64;
        if let Some(size) = size {
            if written != size {
                return Err(LayerFsError::invalid(
                    "inline output does not match its byte count",
                ));
            }
        }
        return Ok(written);
    }
    let Some(output) = output else {
        return Ok(0);
    };
    let size = size.ok_or_else(|| LayerFsError::invalid("spooled output has no byte count"))?;
    let mut offset = 0u64;
    let mut page = [0u8; PAGE];
    while offset < size {
        let args = SdkValue::map([
            ("output", SdkValue::text(output)),
            ("stream", SdkValue::text(stream)),
            ("offset", SdkValue::Int(offset as i64)),
        ]);
        let answer = read_page(&args, &mut page)?;
        let length = answer
            .get_int("length")
            .and_then(|length| usize::try_from(length).ok())
            .ok_or_else(|| LayerFsError::invalid("spooled output page has no length"))?;
        if length == 0 {
            return Err(LayerFsError::invalid(
                "spooled output page is empty before the end",
            ));
        }
        if length as u64 > size - offset {
            return Err(LayerFsError::invalid(
                "spooled output page exceeds its byte count",
            ));
        }
        file.write_all(&page[..length])?;
        offset += length as u64;
        let end = matches!(answer.get("end"), Some(SdkValue::Bool(true)));
        if end {
            break;
        }
    }
    if offset != size {
        return Err(LayerFsError::invalid(
            "spooled output ended before its byte count",
        ));
    }
    Ok(offset)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_sink_needs_distinct_absolute_paths() {
        for sink in [
            json!({ "stdout": "relative" }),
            json!({ "stdout": "/tmp/a", "stderr": "/tmp/a" }),
            json!({}),
            json!("/tmp/a"),
        ] {
            let error = take_sink(json!({ "sink": sink })).unwrap_err();
            assert_eq!(error.code(), "InvalidInput");
        }
    }

    #[test]
    fn spool_sizes_use_the_sdk_byte_count_fields() {
        let fields = vec![
            ("stdout".to_owned(), SdkValue::Null),
            ("stdoutBytes".to_owned(), SdkValue::Int(5)),
            ("stderrBytes".to_owned(), SdkValue::Int(0)),
        ];
        assert_eq!(output_size(&fields, "stdout"), Some(5));
        assert_eq!(output_size(&fields, "stderr"), Some(0));
        assert_eq!(output_size(&fields, "other"), None);
    }

    #[tokio::test]
    async fn a_notification_after_registration_is_not_lost() {
        let drained = tokio::sync::Notify::new();
        let notified = drained.notified();
        let mut notified = std::pin::pin!(notified);
        notified.as_mut().enable();
        drained.notify_waiters();
        tokio::time::timeout(std::time::Duration::from_secs(1), &mut notified)
            .await
            .expect("registered waiter observed the notification");
    }

    #[tokio::test]
    async fn close_waiting_observes_a_drain_notice() {
        let state = Arc::new(Mutex::new(State {
            closed: true,
            inflight: 1,
        }));
        let drained = Arc::new(tokio::sync::Notify::new());
        let waiter = {
            let state = Arc::clone(&state);
            let drained = Arc::clone(&drained);
            tokio::spawn(async move { wait_drained(&state, &drained).await })
        };
        tokio::task::yield_now().await;
        state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .inflight = 0;
        drained.notify_waiters();
        tokio::time::timeout(std::time::Duration::from_secs(1), waiter)
            .await
            .expect("drain was observed")
            .unwrap();
    }

    fn page_answer(length: usize, end: bool) -> SdkValue {
        SdkValue::map([
            ("length", SdkValue::Int(length as i64)),
            ("end", SdkValue::Bool(end)),
        ])
    }

    #[test]
    fn a_malformed_exec_is_refused_before_a_sink_opens() {
        let error = check_exec_args(&json!({ "sandbox": "1", "path": "/ws" })).unwrap_err();
        assert_eq!(error.code(), "InvalidInput");
        let error = check_exec_args(&json!({
            "sandbox": "1",
            "path": "/ws",
            "command": ""
        }))
        .unwrap_err();
        assert_eq!(error.code(), "InvalidInput");
        for extra in [json!({ "message": 1 }), json!({ "background": true })] {
            let mut args = json!({ "sandbox": "1", "path": "/ws", "command": "true" });
            args.as_object_mut()
                .unwrap()
                .extend(extra.as_object().unwrap().clone());
            let error = check_exec_args(&args).unwrap_err();
            assert_eq!(error.code(), "InvalidInput");
        }
    }

    #[test]
    fn inline_streams_are_written_and_checked() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("stdout.log");
        let mut file = open_sink(&path).unwrap();
        assert_eq!(
            write_stream(
                &mut file,
                Some("hello"),
                None,
                "stdout",
                Some(5),
                |_, _| unreachable!(),
            )
            .unwrap(),
            5
        );
        drop(file);
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "hello");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }

        let mut file = open_sink(&path).unwrap();
        let error = write_stream(
            &mut file,
            Some("hello"),
            None,
            "stdout",
            Some(4),
            |_, _| unreachable!(),
        )
        .unwrap_err();
        assert_eq!(error.code(), "InvalidInput");
    }

    #[test]
    fn spooled_pages_are_written_in_order() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("stdout.log");
        let mut file = open_sink(&path).unwrap();
        let payload = b"hello world";
        let result = write_stream(
            &mut file,
            None,
            Some("out"),
            "stdout",
            Some(payload.len() as u64),
            |args, page| {
                let offset = args.get_int("offset").unwrap() as usize;
                let length = (payload.len() - offset).min(5);
                page[..length].copy_from_slice(&payload[offset..offset + length]);
                Ok(page_answer(length, offset + length == payload.len()))
            },
        )
        .unwrap();
        assert_eq!(result, payload.len() as u64);
        drop(file);
        assert_eq!(std::fs::read(&path).unwrap(), payload);
    }

    #[test]
    fn a_spool_that_ends_early_is_an_error() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("stdout.log");
        let mut file = open_sink(&path).unwrap();
        let error = write_stream(
            &mut file,
            None,
            Some("out"),
            "stdout",
            Some(5),
            |_, page| {
                page[..2].copy_from_slice(b"hi");
                Ok(page_answer(2, true))
            },
        )
        .unwrap_err();
        assert_eq!(error.code(), "InvalidInput");
    }

    #[test]
    fn an_empty_or_oversized_spool_page_is_an_error() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("stdout.log");
        for (length, end) in [(0, false), (6, false)] {
            let mut file = open_sink(&path).unwrap();
            let error = write_stream(&mut file, None, Some("out"), "stdout", Some(5), |_, _| {
                Ok(page_answer(length, end))
            })
            .unwrap_err();
            assert_eq!(error.code(), "InvalidInput");
        }
    }

    #[cfg(unix)]
    #[test]
    fn a_hard_linked_sink_pair_is_rejected_before_it_is_cleared() {
        let directory = tempfile::tempdir().unwrap();
        let stdout = directory.path().join("stdout.log");
        let stderr = directory.path().join("stderr.log");
        std::fs::write(&stdout, b"keep").unwrap();
        std::fs::hard_link(&stdout, &stderr).unwrap();
        let sink = Sink {
            stdout: Some(stdout.clone()),
            stderr: Some(stderr),
        };
        let error = sink.open().unwrap_err();
        assert_eq!(error.code(), "InvalidInput");
        assert_eq!(std::fs::read(&stdout).unwrap(), b"keep");
    }

    #[cfg(unix)]
    #[test]
    fn a_single_hard_link_is_rejected() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("linked.log");
        let other = directory.path().join("other.log");
        std::fs::write(&path, b"keep").unwrap();
        std::fs::hard_link(&path, &other).unwrap();
        let error = open_sink(&path).unwrap_err();
        assert_eq!(error.code(), "InvalidInput");
        assert_eq!(std::fs::read(&path).unwrap(), b"keep");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn sink_path_errors_are_invalid_input() {
        let directory = tempfile::tempdir().unwrap();
        let missing = directory.path().join("missing").join("out.log");
        assert_eq!(open_sink(&missing).unwrap_err().code(), "InvalidInput");
        let folder = directory.path().join("folder");
        std::fs::create_dir(&folder).unwrap();
        assert_eq!(open_sink(&folder).unwrap_err().code(), "InvalidInput");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn a_symlinked_sink_is_rejected() {
        let directory = tempfile::tempdir().unwrap();
        let target = directory.path().join("target");
        let link = directory.path().join("link");
        std::fs::write(&target, b"keep").unwrap();
        std::os::unix::fs::symlink(&target, &link).unwrap();
        let error = open_sink(&link).unwrap_err();
        assert_eq!(error.code(), "InvalidInput");
        assert_eq!(std::fs::read(&target).unwrap(), b"keep");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn a_fifo_sink_is_rejected_without_blocking() {
        let directory = tempfile::tempdir().unwrap();
        let fifo = directory.path().join("fifo");
        let path = std::ffi::CString::new(fifo.to_str().unwrap()).unwrap();
        // SAFETY: mkfifo reads the NUL-terminated path and mode only.
        assert_eq!(unsafe { libc::mkfifo(path.as_ptr(), 0o600) }, 0);
        let error = open_sink(&fifo).unwrap_err();
        assert_eq!(error.code(), "InvalidInput");
    }
}
