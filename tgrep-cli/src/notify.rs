/// `tgrep notify` — tell a running search server which files changed.
///
/// The server adds the paths to its pending list and restarts a short debounce
/// window. The list is applied to the index before the next search and, when
/// no search arrives, by a background loop once the window elapses. This is
/// the mechanism an editor plugin uses to keep the index in sync on
/// filesystems whose native change notifications it cannot rely on.
use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use tgrep_core::builder;
use tgrep_core::meta::IndexMeta;

use crate::serve::ServerInfo;

/// Lexically normalise the candidate path without touching the filesystem, so
/// the result is independent of whether the path exists. Joins `file` onto
/// `root` when it is relative (an absolute `file` is used as-is), then folds
/// `.` and `..` segments and collapses duplicate separators while preserving
/// the absolute prefix. A `..` that would climb past the absolute root is left
/// in place so the caller's `.`/`..` check still rejects an escape.
fn lexical_normalize(root: &Path, file: &Path) -> PathBuf {
    use std::path::Component;
    let candidate = if file.is_absolute() {
        file.to_path_buf()
    } else {
        root.join(file)
    };
    let mut folded: Vec<Component<'_>> = Vec::new();
    for component in candidate.components() {
        match component {
            // `.` adds nothing; drop it.
            Component::CurDir => {}
            // `..` pops a real segment, but never the absolute root.
            Component::ParentDir => match folded.last() {
                Some(Component::RootDir) | Some(Component::Prefix(_)) | None => {
                    folded.push(Component::ParentDir)
                }
                Some(Component::Normal(_)) => {
                    folded.pop();
                }
                // A `..` stacked on a `..` we already kept (an escape): keep it.
                Some(_) => folded.push(Component::ParentDir),
            },
            other => folded.push(other),
        }
    }
    folded.iter().copied().collect()
}

/// Resolve a changed file to the index-relative path the server stores.
///
/// Absolute paths are canonicalised; relative ones are resolved against the
/// served root, not the caller's working directory, so the same command works
/// from anywhere. A path that no longer exists (the file was deleted) is
/// normalised lexically rather than canonicalised, so `./x`, `sub/../x` and
/// `x//y` all yield the same clean relative path as the existing case; the
/// server then drops it from the index, which is what a delete notification is
/// for. The final `.`/`..` check is a safety net for genuinely malformed input
/// that survives normalisation.
fn index_relative_path(index_root: &Path, root: &Path, file: &Path) -> Result<String> {
    let candidate = if file.is_absolute() {
        file.to_path_buf()
    } else {
        root.join(file)
    };
    let canonical = match std::fs::canonicalize(&candidate) {
        Ok(path) => path,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            lexical_normalize(root, file)
        }
        Err(error) => {
            return Err(error).with_context(|| format!("cannot resolve {}", file.display()));
        }
    };
    let relative = canonical
        .strip_prefix(index_root)
        .with_context(|| {
            format!(
                "{} is not inside the served tree {}",
                file.display(),
                index_root.display()
            )
        })?;
    let relative = relative.to_string_lossy().replace('\\', "/");
    anyhow::ensure!(
        !relative.is_empty()
            && !relative.split('/').any(|part| matches!(part, "." | "..")),
        "invalid path: {relative}"
    );
    Ok(relative)
}

pub fn run(root: &Path, index_path: Option<&Path>, files: &[PathBuf]) -> Result<()> {
    let root = std::fs::canonicalize(root)
        .with_context(|| format!("cannot resolve root {}", root.display()))?;
    let index_dir = index_path
        .map(Path::to_path_buf)
        .unwrap_or_else(|| builder::default_index_dir(&root));
    let info = ServerInfo::load(&index_dir).with_context(|| {
        format!(
            "no running server for index directory `{}`; start one with \
             `tgrep serve --index-path {} <root>`",
            index_dir.display(),
            index_dir.display()
        )
    })?;
    // The index root comes from the index's own metadata; fall back to the
    // served root when it is unreadable, as `search` does. When it is readable
    // it must match the served root: two roots that share an index directory
    // would otherwise send this notify to the wrong server.
    let meta_root = IndexMeta::load(&index_dir)
        .ok()
        .and_then(|meta| std::fs::canonicalize(meta.root_path).ok());
    let index_root = match meta_root {
        Some(meta_root) if meta_root != root => {
            anyhow::bail!(
                "index directory `{}` belongs to root `{}`, not `{}`; \
                 refusing to notify the wrong server",
                index_dir.display(),
                meta_root.display(),
                root.display()
            );
        }
        Some(meta_root) => meta_root,
        None => {
            eprintln!(
                "warning: could not verify the index root from `{}`; \
                 assuming `{}`",
                index_dir.display(),
                root.display()
            );
            root.clone()
        }
    };

    let mut relative_paths: Vec<String> = Vec::with_capacity(files.len());
    for file in files {
        relative_paths.push(index_relative_path(&index_root, &root, file)?);
    }
    relative_paths.sort();
    relative_paths.dedup();

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", info.port))
        .with_context(|| format!("cannot reach server on port {}", info.port))?;
    stream.set_read_timeout(Some(std::time::Duration::from_secs(30)))?;
    let request = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "notify",
        "params": { "paths": relative_paths },
        "id": 1,
    });
    writeln!(stream, "{}", request)?;
    stream.flush()?;

    let mut reader = BufReader::new(&stream);
    let mut line = String::new();
    reader.read_line(&mut line)?;
    let response: serde_json::Value = serde_json::from_str(&line)?;
    if let Some(error) = response.get("error") {
        let message = error
            .get("message")
            .and_then(|m| m.as_str())
            .unwrap_or("unknown error");
        anyhow::bail!("server error: {message}");
    }
    let result = response
        .get("result")
        .ok_or_else(|| anyhow::anyhow!("no result in response"))?;
    let queued = result.get("queued").and_then(|v| v.as_u64()).unwrap_or(0);
    let pending = result.get("pending").and_then(|v| v.as_u64()).unwrap_or(0);
    println!(
        "Notified {queued} file(s); {pending} pending. \
         The index is applied before the next search, or within the server's \
         ~10 s debounce after the last notification.",
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::lexical_normalize;
    use std::path::{Path, PathBuf};

    #[cfg(windows)]
    fn root() -> PathBuf {
        PathBuf::from(r"C:\root")
    }

    #[cfg(not(windows))]
    fn root() -> PathBuf {
        PathBuf::from("/root")
    }

    #[test]
    fn normalizer_folds_dot_parent_and_duplicate_separators() {
        let root = root();
        // A plain relative path is joined onto the root unchanged.
        assert_eq!(lexical_normalize(&root, Path::new("a.txt")), root.join("a.txt"));
        // `.` segments are dropped.
        assert_eq!(
            lexical_normalize(&root, Path::new("./a.txt")),
            root.join("a.txt")
        );
        // `sub/..` collapses back to the root.
        assert_eq!(
            lexical_normalize(&root, Path::new("sub/../a.txt")),
            root.join("a.txt")
        );
        // Duplicate separators collapse to one.
        assert_eq!(
            lexical_normalize(&root, Path::new("a//b.txt")),
            root.join("a").join("b.txt")
        );
    }

    #[test]
    fn normalizer_keeps_a_root_escape_rejectable() {
        let root = root();
        // `a/..` returns to the root and the second `..` climbs past it, so the
        // result no longer sits under `root` and `strip_prefix` rejects it.
        let escaped = lexical_normalize(&root, Path::new("a/../../etc/passwd"));
        assert!(
            escaped.strip_prefix(&root).is_err(),
            "expected the escape to leave the root, got {}",
            escaped.display()
        );
    }
}
