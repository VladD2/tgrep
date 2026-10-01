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
//!   5. Delete: a file is notified and searchable, deleted from disk and
//!      notified again — the server drops it from the index and the drop is
//!      stable on repeat.
//!   6. Rename: a rename is delete(old) + add(new); one notification carrying
//!      both paths converges the index — the old name is gone, the new name
//!      has the new content.
//!   7. Lexical normalisation of deleted paths: `./x` and `sub/../sub/x`
//!      notify a file that no longer exists — the CLI folds the path text
//!      without the filesystem and the server drops the file.
//!   8. Path cap: a `notify` with 100_001 paths is rejected with JSON-RPC
//!      error -32602 before any filesystem access, and the server keeps
//!      serving the next notify.
//!   9. Queued semantics: the same path listed twice in one notify is
//!      deduplicated — `result.queued` and `result.pending` are both 1.

use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf, MAIN_SEPARATOR};
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

/// Poll the pending count until it reaches `expected` (typically 0 after the
/// debounce flush), checking every 500 ms for up to 20 s.
///
/// A fixed sleep followed by an assert is a flake source: the flush is due
/// ~10 s after the last notification, but the flush thread can be late by more
/// than a fixed margin under load, and the assert then fails even though the
/// flush is on its way. Polling gives the flush its whole window plus slack
/// and only burns the extra time when the server is actually broken.
fn wait_for_pending_count(port: u16, expected: u64, serve_log: &Path) {
    let start = Instant::now();
    loop {
        if pending_count(port) == expected {
            return;
        }
        assert!(
            start.elapsed() <= Duration::from_secs(20),
            "pending count did not reach {expected} within 20 seconds; log:\n{}",
            fs::read_to_string(serve_log).unwrap_or_default()
        );
        thread::sleep(Duration::from_millis(500));
    }
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
    wait_for_pending_count(port, 0, &serve_log);
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

    // ── Delete: notifying a path that is gone drops it from the index ─────
    // The plugin saved `d.txt`, then the file was deleted. The index was
    // built without it, so the first notification adds it; the second one,
    // after the delete, must remove it.
    write_file(root, "d.txt", "delete-me\n");
    notify_files(root, index_dir, &["d.txt"]);
    assert_eq!(search_files(root, index_dir, "delete-me"), vec!["d.txt"]);
    assert_eq!(pending_count(port), 0);
    fs::remove_file(root.join("d.txt")).unwrap();
    notify_files(root, index_dir, &["d.txt"]);
    assert_eq!(search_files(root, index_dir, "delete-me"), Vec::<String>::new());
    assert_eq!(pending_count(port), 0);
    // Stable on repeat.
    assert_eq!(search_files(root, index_dir, "delete-me"), Vec::<String>::new());

    // ── Rename: delete(old) + add(new) in one notification ────────────────
    // There is no special rename support: notifying both paths in one call
    // converges the index — the old name is gone, the new name has the new
    // content.
    write_file(root, "r1.txt", "old-name-here\n");
    notify_files(root, index_dir, &["r1.txt"]);
    assert_eq!(search_files(root, index_dir, "old-name-here"), vec!["r1.txt"]);
    fs::rename(root.join("r1.txt"), root.join("r2.txt")).unwrap();
    write_file(root, "r2.txt", "new-name-here\n");
    notify_files(root, index_dir, &["r1.txt", "r2.txt"]);
    assert_eq!(search_files(root, index_dir, "old-name-here"), Vec::<String>::new());
    assert_eq!(search_files(root, index_dir, "new-name-here"), vec!["r2.txt"]);
    assert_eq!(pending_count(port), 0);
    // Stable on repeat.
    assert_eq!(search_files(root, index_dir, "old-name-here"), Vec::<String>::new());
    assert_eq!(search_files(root, index_dir, "new-name-here"), vec!["r2.txt"]);

    // ── Lexical normalisation of a deleted path ────────────────────────────
    // The file is gone, so `canonicalize` cannot run; the CLI folds `.`,
    // `..` and duplicate separators in the path text instead.
    write_file(root, "lex.txt", "lex-content\n");
    notify_files(root, index_dir, &["lex.txt"]);
    assert_eq!(search_files(root, index_dir, "lex-content"), vec!["lex.txt"]);
    fs::remove_file(root.join("lex.txt")).unwrap();
    let output = run_tgrep(
        root,
        &[
            "notify",
            root.to_str().unwrap(),
            "./lex.txt",
            "--no-require-git",
            "--index-path",
            index_dir.to_str().unwrap(),
        ],
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "dot-relative notify of a deleted file failed: {stderr}"
    );
    assert!(!stderr.contains("invalid path"), "unexpected invalid path: {stderr}");
    assert_eq!(search_files(root, index_dir, "lex-content"), Vec::<String>::new());

    write_file(root, "lex2.txt", "lex2-content\n");
    notify_files(root, index_dir, &["lex2.txt"]);
    assert_eq!(search_files(root, index_dir, "lex2-content"), vec!["lex2.txt"]);
    fs::remove_file(root.join("lex2.txt")).unwrap();

    // A `..`-folding path needs a directory to fold back through, so the
    // second case lives in a subdirectory: `sub/../sub/lex3.txt` folds to
    // `sub/lex3.txt`.
    fs::create_dir(root.join("sub")).unwrap();
    write_file(root, "sub/lex3.txt", "lex3-content\n");
    notify_files(root, index_dir, &["sub/lex3.txt"]);
    // The search CLI renders nested paths with the native separator.
    let lex3_display = format!("sub{MAIN_SEPARATOR}lex3.txt");
    assert_eq!(search_files(root, index_dir, "lex3-content"), vec![lex3_display]);
    fs::remove_file(root.join("sub").join("lex3.txt")).unwrap();
    let output = run_tgrep(
        root,
        &[
            "notify",
            root.to_str().unwrap(),
            "sub/../sub/lex3.txt",
            "--no-require-git",
            "--index-path",
            index_dir.to_str().unwrap(),
        ],
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "dot-parent notify of a deleted file failed: {stderr}"
    );
    assert!(!stderr.contains("invalid path"), "unexpected invalid path: {stderr}");
    assert_eq!(search_files(root, index_dir, "lex3-content"), Vec::<String>::new());

    // ── Path cap: an oversized notify is rejected before filesystem access ─
    // 100_001 paths exceed the server's limit; the entries need not exist,
    // the check happens up front. The server must keep serving afterwards.
    let mut paths_json = String::with_capacity(100_001 * 16);
    for i in 0..100_001 {
        if i > 0 {
            paths_json.push(',');
        }
        paths_json.push_str(&format!("\"limit-{i}.txt\""));
    }
    let response = send_request(
        port,
        &format!(
            "{{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"notify\",\"params\":{{\"paths\":[{paths_json}]}}}}"
        ),
    )
    .expect("over-limit notify request failed");
    let value: serde_json::Value = serde_json::from_str(&response).expect("invalid JSON response");
    assert_eq!(
        value.pointer("/error/code").and_then(|v| v.as_i64()),
        Some(-32602),
        "the over-limit notify must be rejected with -32602, got: {response}"
    );
    // The server is still healthy: a small valid notify is accepted and a
    // search flushes it as usual.
    let response = send_request(
        port,
        r#"{"jsonrpc":"2.0","id":2,"method":"notify","params":{"paths":["a.txt"]}}"#,
    )
    .expect("follow-up notify request failed");
    let value: serde_json::Value = serde_json::from_str(&response).expect("invalid JSON response");
    assert_eq!(
        value.pointer("/result/queued").and_then(|v| v.as_u64()),
        Some(1),
        "the follow-up notify must be accepted, got: {response}"
    );
    assert_eq!(search_files(root, index_dir, "one"), vec!["a.txt"]);
    assert_eq!(pending_count(port), 0);

    // ── Queued semantics: duplicates within one notify are counted once ────
    assert_eq!(pending_count(port), 0);
    let response = send_request(
        port,
        r#"{"jsonrpc":"2.0","id":3,"method":"notify","params":{"paths":["a.txt","a.txt"]}}"#,
    )
    .expect("duplicate notify request failed");
    let value: serde_json::Value = serde_json::from_str(&response).expect("invalid JSON response");
    assert_eq!(
        value.pointer("/result/queued").and_then(|v| v.as_u64()),
        Some(1),
        "a duplicate path must be queued once, got: {response}"
    );
    assert_eq!(
        value.pointer("/result/pending").and_then(|v| v.as_u64()),
        Some(1),
        "a duplicate path must be pending once, got: {response}"
    );
    assert_eq!(search_files(root, index_dir, "one"), vec!["a.txt"]);
    assert_eq!(pending_count(port), 0);
}
