//! Integration test for the client-notified change mechanism (`tgrep notify`),
//! emulating an editor plugin.
//!
//! The plugin contract under test: the plugin writes files, tells the running
//! server about them with `tgrep notify`, and then searches with `tgrep
//! search`. The server must apply the notified paths to its index — either
//! synchronously, before the search runs, or, when no search arrives, after a
//! 10 s debounce that every new notification restarts.
//!
//! The server runs with `--no-watch`, which disables the native watcher,
//! polling and the periodic reconcile alike: the index can only change through
//! the notified paths, so every update observed below comes from the
//! mechanism under test. (With the watcher on, its native events would
//! reindex the files on their own and the test would pass vacuously.)
//!
//! Scenarios, in order on one server instance:
//!   1. Baseline: the built index answers searches.
//!   2. Synchronous flush: notify + an immediate search (well inside the
//!      debounce window) sees the new content; the pending list is left empty
//!      and a repeated search is stable.
//!   3. Background debounce: notify, no search, wait past the 10 s window —
//!      the background loop reindexes; the later search sees the new content
//!      and the list is empty.
//!   4. Debounce extension: file A notified, 5 s later file B notified — the
//!      window restarted by the second notification has not elapsed 10 s after
//!      the first, so both are still pending; a search then sees both files
//!      (the flush ran synchronously, before the search).

use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use tempfile::TempDir;

fn tgrep_bin() -> PathBuf {
    assert_cmd::cargo::cargo_bin("tgrep")
}

struct ServerGuard {
    child: Child,
}

impl Drop for ServerGuard {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn run_tgrep(cwd: &Path, args: &[&str]) -> Output {
    Command::new(tgrep_bin())
        .args(args)
        .current_dir(cwd)
        .output()
        .expect("failed to run tgrep")
}

fn write_file(root: &Path, name: &str, content: &str) {
    fs::write(root.join(name), content).unwrap();
}

/// `tgrep search PATTERN -l` with the working directory at the root and no
/// path argument, so the printed paths are bare file names.
///
/// Exit code 1 means "no match" (a valid answer); only 2 (an error) is a
/// failure of the search itself.
fn search_files(root: &Path, index_dir: &Path, pattern: &str) -> Vec<String> {
    let output = run_tgrep(
        root,
        &[
            "search",
            pattern,
            "-l",
            "--no-require-git",
            "--index-path",
            index_dir.to_str().unwrap(),
        ],
    );
    assert!(
        output.status.code() != Some(2),
        "search {pattern} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(str::to_string)
        .filter(|line| !line.is_empty())
        .collect()
}

/// The plugin's notification call: `tgrep notify ROOT FILE...`.
fn notify_files(root: &Path, index_dir: &Path, files: &[&str]) {
    let mut args: Vec<&str> = vec!["notify", root.to_str().unwrap()];
    args.extend_from_slice(files);
    args.extend_from_slice(&[
        "--no-require-git",
        "--index-path",
        index_dir.to_str().unwrap(),
    ]);
    let output = run_tgrep(root, &args);
    assert!(
        output.status.success(),
        "notify failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn send_request(port: u16, request: &str) -> std::io::Result<String> {
    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}"))?;
    stream.set_read_timeout(Some(Duration::from_secs(30)))?;
    writeln!(stream, "{request}")?;
    stream.flush()?;
    let mut reader = BufReader::new(stream);
    let mut response = String::new();
    reader.read_line(&mut response)?;
    Ok(response)
}

/// The server's pending-changes count, straight from the protocol.
fn pending_count(port: u16) -> u64 {
    let request =
        serde_json::json!({ "jsonrpc": "2.0", "method": "status", "id": 1 }).to_string();
    let response = send_request(port, &request).expect("status request failed");
    let value: serde_json::Value = serde_json::from_str(&response).expect("invalid JSON response");
    value
        .pointer("/result/pending_changes")
        .and_then(|v| v.as_u64())
        .unwrap_or_else(|| panic!("missing pending_changes in response: {response}"))
}

fn wait_for_port(index_dir: &Path) -> u16 {
    let serve_json = index_dir.join("serve.json");
    let start = Instant::now();
    loop {
        assert!(
            start.elapsed() <= Duration::from_secs(60),
            "tgrep serve did not start within 60 seconds; log:\n{}",
            fs::read_to_string(serve_json).unwrap_or_default()
        );
        if let Ok(data) = fs::read_to_string(&serve_json)
            && let Ok(info) = serde_json::from_str::<serde_json::Value>(&data)
            && let Some(p) = info.get("port").and_then(|v| v.as_u64())
            && TcpStream::connect(format!("127.0.0.1:{p}")).is_ok()
        {
            return p as u16;
        }
        thread::sleep(Duration::from_millis(20));
    }
}

/// Block until the startup stale check has finished, so the index is in its
/// settled pre-test state before the first notification.
fn wait_for_stale_check(log_path: &Path) {
    let start = Instant::now();
    loop {
        assert!(
            start.elapsed() <= Duration::from_secs(60),
            "stale check did not finish within 60 seconds; log:\n{}",
            fs::read_to_string(log_path).unwrap_or_default()
        );
        if let Ok(log) = fs::read_to_string(log_path)
            && (log.contains("stale check: index is up-to-date")
                || log.contains("stale check: updated"))
        {
            return;
        }
        thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn notify_emulates_plugin_updates() {
    let root_dir = TempDir::new().unwrap();
    let root = root_dir.path();
    write_file(root, "a.txt", "alpha\n");

    // Isolated index directory, outside the served tree.
    let index_dir_tmp = TempDir::new().unwrap();
    let index_dir = index_dir_tmp.path();

    // A complete index so `serve` warm-starts and the test begins from a
    // settled state.
    let output = run_tgrep(
        root,
        &[
            "index",
            root.to_str().unwrap(),
            "--index-path",
            index_dir.to_str().unwrap(),
            "--no-require-git",
        ],
    );
    assert!(
        output.status.success(),
        "initial index build failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let build_log = String::from_utf8_lossy(&output.stderr);
    assert!(
        build_log.contains("Found 1 text files"),
        "expected the fixture file to be indexed, got: {build_log}"
    );

    // The serve log lives outside the root: a log inside it would be a new
    // file the index walk could see.
    let log_dir = TempDir::new().unwrap();
    let serve_log = log_dir.path().join("serve.stderr.log");
    let child = Command::new(tgrep_bin())
        .args([
            "serve",
            root.to_str().unwrap(),
            "--index-path",
            index_dir.to_str().unwrap(),
            "--no-require-git",
            "--no-watch",
        ])
        .stderr(Stdio::from(fs::File::create(&serve_log).unwrap()))
        .spawn()
        .expect("failed to spawn tgrep serve");
    let _guard = ServerGuard { child };

    let port = wait_for_port(index_dir);
    wait_for_stale_check(&serve_log);

    // ── Baseline ──────────────────────────────────────────────────────────
    assert_eq!(search_files(root, index_dir, "alpha"), vec!["a.txt"]);
    assert_eq!(search_files(root, index_dir, "beta"), Vec::<String>::new());
    assert_eq!(pending_count(port), 0);

    // ── Synchronous flush before search ───────────────────────────────────
    // The plugin saves `beta` and notifies; a search issued immediately —
    // well inside the 10 s debounce window — must already see it.
    write_file(root, "a.txt", "beta\n");
    notify_files(root, index_dir, &["a.txt"]);
    assert_eq!(pending_count(port), 1, "the notified path must be pending");
    assert_eq!(search_files(root, index_dir, "beta"), vec!["a.txt"]);
    assert_eq!(search_files(root, index_dir, "alpha"), Vec::<String>::new());
    // The search consumed the list; a repeated search is stable.
    assert_eq!(pending_count(port), 0, "the search must clear the pending list");
    assert_eq!(search_files(root, index_dir, "beta"), vec!["a.txt"]);

    // ── Background debounce (no search until after the window) ────────────
    // The plugin saves `gamma` and notifies, then does not search. The
    // background loop must reindex the file once the 10 s window elapses.
    write_file(root, "a.txt", "gamma\n");
    notify_files(root, index_dir, &["a.txt"]);
    assert_eq!(pending_count(port), 1, "the notified path must be pending");
    thread::sleep(Duration::from_secs(12));
    assert_eq!(
        pending_count(port),
        0,
        "the background debounce must have flushed the list; log:\n{}",
        fs::read_to_string(&serve_log).unwrap_or_default()
    );
    assert_eq!(search_files(root, index_dir, "gamma"), vec!["a.txt"]);
    assert_eq!(search_files(root, index_dir, "beta"), Vec::<String>::new());
    // Stable on repeat.
    assert_eq!(search_files(root, index_dir, "gamma"), vec!["a.txt"]);

    // ── Debounce window extended by a second notification ─────────────────
    // A at t=0, B at t=5 s: the window restarted by B elapses at t=15 s, so
    // at t=10 s both paths must still be pending — and a search at t=10 s
    // must see both, because it flushes the list synchronously.
    write_file(root, "a.txt", "one\n");
    write_file(root, "b.txt", "two\n");
    notify_files(root, index_dir, &["a.txt"]);
    thread::sleep(Duration::from_secs(5));
    notify_files(root, index_dir, &["b.txt"]);
    assert_eq!(
        pending_count(port),
        2,
        "both notified paths must be pending"
    );
    thread::sleep(Duration::from_secs(5));
    assert_eq!(
        pending_count(port),
        2,
        "the second notification must have restarted the debounce window; log:\n{}",
        fs::read_to_string(&serve_log).unwrap_or_default()
    );
    assert_eq!(search_files(root, index_dir, "one"), vec!["a.txt"]);
    assert_eq!(search_files(root, index_dir, "two"), vec!["b.txt"]);
    assert_eq!(pending_count(port), 0, "the search must clear the pending list");
    // Stable on repeat.
    assert_eq!(search_files(root, index_dir, "one"), vec!["a.txt"]);
    assert_eq!(search_files(root, index_dir, "two"), vec!["b.txt"]);
}
