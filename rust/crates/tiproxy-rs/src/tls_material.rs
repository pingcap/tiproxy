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

//! Safe, capability-confined TLS-material (PEM) reads in the composition root.
//!
//! This is the single read that binds a TLS file's bytes into a generation's
//! prepared artifact, so it must not be tricked into reading a file outside the
//! allowed roots.
//!
//! Each allowed root is opened once at startup into a frozen directory
//! capability by walking its canonical components from `/` with `NOFOLLOW` (see
//! [`open_tls_roots`]). A read then selects the capability whose canonical
//! directory contains the material's canonical *parent* (only the parent is
//! canonicalized, so a final-component symlink is never resolved away), and
//! traverses the root-relative parent components from that frozen capability
//! with `openat(.., NOFOLLOW)`, opening the final basename `NOFOLLOW`. The raw
//! path is never opened ambiently, so a parent-directory swapped for a symlink
//! after the root was selected is rejected by `NOFOLLOW` rather than followed
//! out of the root. The final descriptor is checked to be a regular file and
//! only a bounded prefix is read. It uses `rustix::fs::openat` (a safe,
//! cross-platform API), so it needs no `unsafe`.

use std::ffi::OsString;
use std::fs::File;
use std::io::Read;
use std::os::fd::{AsFd, BorrowedFd, OwnedFd};
use std::path::{Component, Path, PathBuf};

use rustix::fs::{Mode, OFlags, open, openat};

/// Upper bound on one TLS material file, matching CP-CFG's serving reader.
const MAX_TLS_FILE_BYTES: u64 = 16 * 1024 * 1024;

/// A frozen directory capability for one allowed TLS root.
struct Root {
    canonical: PathBuf,
    capability: OwnedFd,
}

/// The allowed TLS roots, each held as an open directory capability frozen at
/// startup. Not `Clone`: share it behind an `Arc`.
pub struct TlsRoots {
    roots: Vec<Root>,
}

/// Opens each allowed root as a frozen directory capability. A root that cannot
/// be canonicalized or opened (or whose path contains a symlink component) is
/// dropped, so later reads confine against stable, symlink-free capabilities.
#[must_use]
pub fn open_tls_roots(roots: &[PathBuf]) -> TlsRoots {
    let mut opened = Vec::new();
    for root in roots {
        if let Ok(canonical) = std::fs::canonicalize(root)
            && let Ok(capability) = open_dir_capability(&canonical)
        {
            opened.push(Root {
                canonical,
                capability,
            });
        }
    }
    TlsRoots { roots: opened }
}

/// Opens a canonical (absolute, symlink-free) directory into a capability by
/// walking each component from `/` with `NOFOLLOW`.
fn open_dir_capability(canonical_dir: &Path) -> Result<OwnedFd, &'static str> {
    let mut fd = open(
        "/",
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(|_| "tls root is unavailable")?;
    for component in canonical_dir.components() {
        match component {
            Component::RootDir => {}
            Component::Normal(name) => {
                fd = openat(
                    &fd,
                    name,
                    OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                    Mode::empty(),
                )
                .map_err(|_| "tls root component is a symlink or unavailable")?;
            }
            _ => return Err("tls root path is not canonical"),
        }
    }
    Ok(fd)
}

/// Reads one TLS material file safely and returns its bytes.
///
/// # Errors
///
/// Returns a bounded, payload-free reason (never the path or the material) when
/// the path cannot be resolved, escapes a configured root, has a symlink
/// component inside a root, is not a regular file, or exceeds 16 MiB. A
/// relative path is resolved against the process working directory. An empty
/// root list does not add a jail.
pub fn read_tls_material(path: &Path, roots: &TlsRoots) -> Result<Vec<u8>, &'static str> {
    let path = absolute_tls_path(path)?;
    if roots.roots.is_empty() {
        // Go has no TLS-root jail. An empty allowlist reads the path the
        // process working directory resolved, with the same size bound.
        return read_unconfined(&path);
    }
    let selection = select_root(&path, roots)?;
    traverse_and_read(&selection)
}

fn absolute_tls_path(path: &Path) -> Result<PathBuf, &'static str> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        Ok(std::env::current_dir()
            .map_err(|_| "tls material path is unavailable")?
            .join(path))
    }
}

fn read_unconfined(path: &Path) -> Result<Vec<u8>, &'static str> {
    let mut file = File::open(path).map_err(|_| "tls material is unavailable")?;
    let metadata = file.metadata().map_err(|_| "tls material is unavailable")?;
    if !metadata.is_file() {
        return Err("tls material must be a regular file");
    }
    read_bounded_prefix(&mut file)
}

/// The frozen root plus the root-relative parent components and the
/// (uncanonicalized) final basename that a read will traverse.
struct Selection<'a> {
    root: &'a Root,
    relative_parent: Vec<OsString>,
    basename: OsString,
}

/// Selects the frozen root capability whose canonical directory contains the
/// material's canonical parent. Only the parent is canonicalized, so the final
/// component's symlink status is preserved for the `NOFOLLOW` open.
fn select_root<'a>(path: &Path, roots: &'a TlsRoots) -> Result<Selection<'a>, &'static str> {
    if !path.is_absolute() {
        return Err("tls material path is not absolute");
    }
    let parent = path.parent().ok_or("tls material path has no parent")?;
    let basename = path
        .file_name()
        .ok_or("tls material path has no file name")?;
    let canonical_parent =
        std::fs::canonicalize(parent).map_err(|_| "tls material parent is unavailable")?;
    for root in &roots.roots {
        let Ok(relative) = canonical_parent.strip_prefix(&root.canonical) else {
            continue;
        };
        let mut relative_parent = Vec::new();
        for component in relative.components() {
            match component {
                Component::Normal(name) => relative_parent.push(name.to_os_string()),
                _ => return Err("tls material parent is not canonical"),
            }
        }
        return Ok(Selection {
            root,
            relative_parent,
            basename: basename.to_os_string(),
        });
    }
    Err("tls material is outside the configured TLS roots")
}

/// Traverses the parent components from the frozen root capability with
/// `NOFOLLOW`, opens the final basename `NOFOLLOW`, verifies a regular file, and
/// reads a bounded prefix. The raw path is never opened ambiently.
fn traverse_and_read(selection: &Selection<'_>) -> Result<Vec<u8>, &'static str> {
    let mut walked: Option<OwnedFd> = None;
    for component in &selection.relative_parent {
        let parent_fd = current_fd(selection.root, walked.as_ref());
        let child = openat(
            parent_fd,
            component.as_os_str(),
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(|_| "tls material component is a symlink or unavailable")?;
        walked = Some(child);
    }
    let parent_fd = current_fd(selection.root, walked.as_ref());
    // NONBLOCK avoids hanging if the final component was raced into a FIFO; the
    // regular-file check below then fails closed.
    let file_fd = openat(
        parent_fd,
        selection.basename.as_os_str(),
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(|_| "tls material is a symlink or unavailable")?;
    let mut file = File::from(file_fd);
    let metadata = file.metadata().map_err(|_| "tls material is unavailable")?;
    if !metadata.is_file() {
        return Err("tls material must be a regular file");
    }
    read_bounded_prefix(&mut file)
}

/// The directory fd to open the next component from: the walked child if any,
/// else the frozen root capability.
fn current_fd<'a>(root: &'a Root, walked: Option<&'a OwnedFd>) -> BorrowedFd<'a> {
    walked.map_or_else(|| root.capability.as_fd(), AsFd::as_fd)
}

/// Reads at most `MAX_TLS_FILE_BYTES + 1` bytes from an already-opened
/// descriptor, so a file that grows after its size was observed cannot cause an
/// unbounded read; a full `MAX + 1` bytes means the file is too large.
fn read_bounded_prefix(file: &mut File) -> Result<Vec<u8>, &'static str> {
    let mut contents = Vec::new();
    file.take(MAX_TLS_FILE_BYTES + 1)
        .read_to_end(&mut contents)
        .map_err(|_| "tls material cannot be read")?;
    if u64::try_from(contents.len()).unwrap_or(u64::MAX) > MAX_TLS_FILE_BYTES {
        return Err("tls material is larger than 16 MiB");
    }
    Ok(contents)
}

#[cfg(test)]
mod tests {
    use super::{
        MAX_TLS_FILE_BYTES, open_tls_roots, read_tls_material, select_root, traverse_and_read,
    };
    use std::io::Write;
    use std::path::PathBuf;

    fn unique_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("cptopo-tls-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap_or_else(|error| unreachable!("create dir: {error}"));
        dir
    }

    fn write(path: &std::path::Path, bytes: &[u8]) {
        let mut file =
            std::fs::File::create(path).unwrap_or_else(|error| unreachable!("create: {error}"));
        file.write_all(bytes)
            .unwrap_or_else(|error| unreachable!("write: {error}"));
    }

    #[test]
    fn a_regular_file_inside_an_allowed_root_reads() {
        let dir = unique_dir("ok");
        let path = dir.join("ca.pem");
        write(&path, b"ca-bytes");
        let roots = open_tls_roots(std::slice::from_ref(&dir));
        let bytes =
            read_tls_material(&path, &roots).unwrap_or_else(|error| unreachable!("read: {error}"));
        assert_eq!(bytes, b"ca-bytes");
    }

    #[test]
    fn a_missing_relative_path_is_unavailable() {
        let roots = open_tls_roots(&[]);
        assert!(read_tls_material(std::path::Path::new("ca.pem"), &roots).is_err());
    }

    #[test]
    fn a_relative_path_with_no_roots_reads_from_the_working_directory() {
        let name = format!("cptopo-rel-{}.pem", std::process::id());
        let absolute = std::env::current_dir()
            .unwrap_or_else(|error| unreachable!("cwd: {error}"))
            .join(&name);
        let _ = std::fs::remove_file(&absolute);
        write(&absolute, b"rel-ca");
        let roots = open_tls_roots(&[]);
        let bytes = read_tls_material(std::path::Path::new(&name), &roots)
            .unwrap_or_else(|error| unreachable!("relative read: {error}"));
        assert_eq!(bytes, b"rel-ca");
        let _ = std::fs::remove_file(&absolute);
    }

    #[test]
    fn a_file_outside_the_allowed_roots_is_rejected() {
        let dir = unique_dir("outside-file");
        let path = dir.join("ca.pem");
        write(&path, b"ca-bytes");
        let roots = open_tls_roots(std::slice::from_ref(&unique_dir("outside-root")));
        assert!(read_tls_material(&path, &roots).is_err());
    }

    #[test]
    fn a_same_root_final_symlink_is_rejected() {
        let dir = unique_dir("symlink");
        let real = dir.join("real.pem");
        write(&real, b"ca-bytes");
        let link = dir.join("link.pem");
        std::os::unix::fs::symlink(&real, &link)
            .unwrap_or_else(|error| unreachable!("symlink: {error}"));
        let roots = open_tls_roots(std::slice::from_ref(&dir));
        assert!(
            read_tls_material(&link, &roots).is_err(),
            "a same-root final symlink must be rejected"
        );
        assert!(read_tls_material(&real, &roots).is_ok());
    }

    #[test]
    fn a_directory_is_rejected_as_non_regular() {
        let dir = unique_dir("dir");
        let sub = dir.join("sub");
        std::fs::create_dir_all(&sub).unwrap_or_else(|error| unreachable!("mkdir: {error}"));
        let roots = open_tls_roots(std::slice::from_ref(&dir));
        assert!(read_tls_material(&sub, &roots).is_err());
    }

    #[test]
    fn a_file_larger_than_the_limit_is_rejected() {
        let dir = unique_dir("toobig");
        let path = dir.join("ca.pem");
        let file =
            std::fs::File::create(&path).unwrap_or_else(|error| unreachable!("create: {error}"));
        file.set_len(MAX_TLS_FILE_BYTES + 2)
            .unwrap_or_else(|error| unreachable!("set_len: {error}"));
        let roots = open_tls_roots(std::slice::from_ref(&dir));
        assert!(read_tls_material(&path, &roots).is_err());
    }

    #[test]
    fn a_parent_swapped_to_an_outside_symlink_after_selection_never_escapes() {
        // Deterministic capability test: select the root and root-relative parent
        // against the real directory, then swap the parent directory for a symlink
        // pointing outside the root. The traversal walks from the frozen root
        // capability with NOFOLLOW, so it rejects the swapped-in symlink component
        // and never returns the outside sentinel.
        let root = unique_dir("swap-root");
        let inside = root.join("d");
        std::fs::create_dir_all(&inside).unwrap_or_else(|error| unreachable!("mkdir: {error}"));
        write(&inside.join("ca.pem"), b"inside-bytes");
        let outside = unique_dir("swap-outside");
        let outside_d = outside.join("d");
        std::fs::create_dir_all(&outside_d).unwrap_or_else(|error| unreachable!("mkdir: {error}"));
        write(&outside_d.join("ca.pem"), b"OUTSIDE-SENTINEL");

        let roots = open_tls_roots(std::slice::from_ref(&root));
        let material = inside.join("ca.pem");
        let selection =
            select_root(&material, &roots).unwrap_or_else(|error| unreachable!("select: {error}"));

        // Swap root/d (a real dir) for a symlink to outside/d.
        std::fs::remove_dir_all(&inside).unwrap_or_else(|error| unreachable!("rm: {error}"));
        std::os::unix::fs::symlink(&outside_d, &inside)
            .unwrap_or_else(|error| unreachable!("symlink: {error}"));

        let result = traverse_and_read(&selection);
        assert!(
            result.is_err(),
            "a parent swapped to an outside symlink must be rejected"
        );
        if let Ok(bytes) = result {
            assert_ne!(
                bytes, b"OUTSIDE-SENTINEL",
                "must never read outside the root"
            );
        }
    }
}
