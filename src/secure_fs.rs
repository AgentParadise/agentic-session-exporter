//! Directory-relative filesystem access beneath a trusted root.
//!
//! Durable exporter state lives next to directories an agent can write. A
//! path that is joined and reopened on every access can be redirected by a
//! symlink planted at any component, so every durable root is opened once,
//! component by component, with no-follow semantics, and every later access is
//! made relative to the held directory descriptor.
//!
//! Trust rules for the chain that leads to a root:
//! - a symlink is followed only when it and the directory holding it are owned
//!   by root and that directory is not writable by group or other (this admits
//!   system links such as macOS `/var` and `/tmp`, never a user-planted one);
//! - every directory is owned by root or the effective user, and one writable
//!   by group or other must carry the sticky bit (as `/tmp` does);
//! - the root itself is owned by the effective user and never group or other
//!   writable; a newly created or opened-for-write root is narrowed to 0700.
//!
//! SQLite can only open by path, so a database is opened through the canonical
//! (symlink-free) path of its verified directory with `SQLITE_OPEN_NOFOLLOW`,
//! after its file and sidecars are checked to be absent or single-link regular
//! files, and the directory identity is verified again afterwards.
//!
//! On platforms without `openat` the same checks run against paths, which
//! narrows but cannot close the replacement window.

use std::fs::File;
use std::io;

/// Sidecars SQLite may create beside a database, all of which must be checked.
const SQLITE_SIDECARS: [&str; 4] = ["", "-wal", "-shm", "-journal"];

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

/// A single path component that cannot escape the directory it names.
fn check_name(name: &str) -> io::Result<()> {
    if name.is_empty() || name == "." || name == ".." || name.contains(['/', '\\', '\0']) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid file name",
        ));
    }
    Ok(())
}

/// An uncommitted object file. The temporary name is always unlinked on drop,
/// whether or not it was published under its final name.
pub struct TempFile<'a> {
    dir: &'a SecureDir,
    name: String,
    file: File,
}

impl TempFile<'_> {
    pub fn file(&mut self) -> &mut File {
        &mut self.file
    }

    /// Link the durable file under `name` unless that name already exists.
    /// Returns false when it does, leaving the existing file untouched.
    pub fn publish_noclobber(&self, name: &str) -> io::Result<bool> {
        check_name(name)?;
        self.dir.link_noclobber(&self.name, name)
    }
}

impl Drop for TempFile<'_> {
    fn drop(&mut self) {
        let _ = self.dir.remove(&self.name);
    }
}

fn temp_name() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!(
        ".tmp-{}-{nanos}-{}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    )
}

#[cfg(unix)]
pub use unix::SecureDir;

#[cfg(unix)]
mod unix {
    use super::{check_name, invalid, temp_name, TempFile, SQLITE_SIDECARS};
    use rustix::fs::{AtFlags, Mode, OFlags, Stat, CWD};
    use rustix::io::Errno;
    use std::collections::VecDeque;
    use std::ffi::OsString;
    use std::fs::File;
    use std::io;
    use std::os::fd::OwnedFd;
    use std::os::unix::ffi::OsStringExt;
    use std::path::{Component, Path, PathBuf};

    const S_IFMT: u32 = 0o170_000;
    const S_IFLNK: u32 = 0o120_000;
    const S_IFREG: u32 = 0o100_000;
    const MAX_LINKS: usize = 40;

    // The stat field widths differ by platform and backend (u16 mode on macOS,
    // u32 on Linux), so the casts are necessary on one and redundant on another.
    #[allow(clippy::unnecessary_cast)]
    fn mode(stat: &Stat) -> u32 {
        stat.st_mode as u32
    }
    #[allow(clippy::unnecessary_cast)]
    fn identity(stat: &Stat) -> (u64, u64) {
        (stat.st_dev as u64, stat.st_ino as u64)
    }
    #[allow(clippy::unnecessary_cast)]
    fn links(stat: &Stat) -> u64 {
        stat.st_nlink as u64
    }
    fn euid() -> u32 {
        rustix::process::geteuid().as_raw()
    }

    fn untrusted(message: &'static str) -> io::Error {
        io::Error::new(io::ErrorKind::PermissionDenied, message)
    }

    fn open_dir(parent: impl rustix::fd::AsFd, name: &Path) -> io::Result<OwnedFd> {
        Ok(rustix::fs::openat(
            parent,
            name,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )?)
    }

    /// Every directory on the way to a root: owned by root or us, and not
    /// writable by anyone else unless the sticky bit stops them renaming it.
    fn check_ancestor(stat: &Stat) -> io::Result<()> {
        let foreign = stat.st_uid != 0 && stat.st_uid != euid();
        let shared = mode(stat) & 0o022 != 0 && mode(stat) & 0o1000 == 0;
        if foreign || shared {
            return Err(untrusted("directory on the trusted path is not trusted"));
        }
        Ok(())
    }

    /// The root or a child of it: ours alone.
    fn check_private(stat: &Stat) -> io::Result<()> {
        if stat.st_uid != euid() || mode(stat) & 0o022 != 0 {
            return Err(untrusted("trusted directory is not privately owned"));
        }
        Ok(())
    }

    /// A directory opened beneath a trusted root. `path` is canonical: it names
    /// the same directory with no symlink in any component.
    pub struct SecureDir {
        fd: OwnedFd,
        path: PathBuf,
        identity: (u64, u64),
    }

    impl SecureDir {
        /// Open (and, with `create`, make) an absolute root without following
        /// any symlink that an unprivileged user could have planted.
        pub fn open_root(path: &Path, create: bool) -> io::Result<Self> {
            if !path.is_absolute() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "trusted root must be absolute",
                ));
            }
            let mut pending: VecDeque<OsString> = split(path);
            let root = open_dir(CWD, Path::new("/"))?;
            let mut stack: Vec<(OwnedFd, PathBuf)> = vec![(root, PathBuf::from("/"))];
            let mut followed = 0;
            while let Some(name) = pending.pop_front() {
                if name == ".." {
                    if stack.len() > 1 {
                        stack.pop();
                    }
                    continue;
                }
                let (dir, dir_path) = stack.last().expect("the filesystem root is never popped");
                let name = Path::new(&name);
                match rustix::fs::statat(dir, name, AtFlags::SYMLINK_NOFOLLOW) {
                    Ok(stat) if mode(&stat) & S_IFMT == S_IFLNK => {
                        followed += 1;
                        let holder = rustix::fs::fstat(dir)?;
                        if followed > MAX_LINKS
                            || stat.st_uid != 0
                            || holder.st_uid != 0
                            || mode(&holder) & 0o022 != 0
                        {
                            return Err(untrusted("refusing an untrusted symlink"));
                        }
                        let target = rustix::fs::readlinkat(dir, name, Vec::new())?;
                        let target = PathBuf::from(OsString::from_vec(target.into_bytes()));
                        if target.is_absolute() {
                            stack.truncate(1);
                        }
                        for part in split(&target).into_iter().rev() {
                            pending.push_front(part);
                        }
                        continue;
                    }
                    Ok(_) => {}
                    Err(Errno::NOENT) if create => {
                        match rustix::fs::mkdirat(dir, name, Mode::from_raw_mode(0o700)) {
                            Ok(()) | Err(Errno::EXIST) => {}
                            Err(error) => return Err(error.into()),
                        }
                    }
                    Err(error) => return Err(error.into()),
                }
                let fd = open_dir(dir, name)?;
                check_ancestor(&rustix::fs::fstat(&fd)?)?;
                let joined = dir_path.join(name);
                stack.push((fd, joined));
            }
            let (fd, path) = stack.pop().expect("the root is always present");
            if stack.is_empty() {
                return Err(untrusted("the filesystem root cannot be a private root"));
            }
            let stat = rustix::fs::fstat(&fd)?;
            check_private(&stat)?;
            if create && mode(&stat) & 0o077 != 0 {
                rustix::fs::fchmod(&fd, Mode::from_raw_mode(0o700))?;
            }
            Ok(Self {
                fd,
                path,
                identity: identity(&stat),
            })
        }

        pub fn path(&self) -> &Path {
            &self.path
        }

        /// Open (and, with `create`, make) a private child directory.
        pub fn child(&self, name: &str, create: bool) -> io::Result<Self> {
            check_name(name)?;
            if create {
                match rustix::fs::mkdirat(&self.fd, name, Mode::from_raw_mode(0o700)) {
                    Ok(()) | Err(Errno::EXIST) => {}
                    Err(error) => return Err(error.into()),
                }
            }
            let fd = open_dir(&self.fd, Path::new(name))?;
            let stat = rustix::fs::fstat(&fd)?;
            check_private(&stat)?;
            Ok(Self {
                fd,
                path: self.path.join(name),
                identity: identity(&stat),
            })
        }

        /// Prove the canonical path still names this exact directory. A
        /// renamed or replaced component anywhere on the path fails here.
        pub fn verify(&self) -> io::Result<()> {
            let mut current = open_dir(CWD, Path::new("/"))?;
            for part in split(&self.path) {
                current = open_dir(&current, Path::new(&part))?;
            }
            if identity(&rustix::fs::fstat(&current)?) != self.identity {
                return Err(invalid("trusted directory was replaced"));
            }
            Ok(())
        }

        /// Open an existing regular file for reading without following a link.
        pub fn open_read(&self, name: &str) -> io::Result<File> {
            check_name(name)?;
            let fd = rustix::fs::openat(
                &self.fd,
                name,
                OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
                Mode::empty(),
            )
            .map_err(|error| match error {
                Errno::LOOP => invalid("refusing to read through a symlink"),
                other => other.into(),
            })?;
            if mode(&rustix::fs::fstat(&fd)?) & S_IFMT != S_IFREG {
                return Err(invalid("expected a regular file"));
            }
            Ok(File::from(fd))
        }

        /// Create a fresh private file under a unique temporary name.
        pub fn create_temp(&self) -> io::Result<TempFile<'_>> {
            let name = temp_name();
            let fd = rustix::fs::openat(
                &self.fd,
                name.as_str(),
                OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::from_raw_mode(0o600),
            )?;
            Ok(TempFile {
                dir: self,
                name,
                file: File::from(fd),
            })
        }

        pub(super) fn link_noclobber(&self, from: &str, to: &str) -> io::Result<bool> {
            match rustix::fs::linkat(&self.fd, from, &self.fd, to, AtFlags::empty()) {
                Ok(()) => Ok(true),
                Err(Errno::EXIST) => Ok(false),
                Err(error) => Err(error.into()),
            }
        }

        /// Unlink a file. A missing file is not an error.
        pub fn remove(&self, name: &str) -> io::Result<()> {
            check_name(name)?;
            match rustix::fs::unlinkat(&self.fd, name, AtFlags::empty()) {
                Ok(()) | Err(Errno::NOENT) => Ok(()),
                Err(error) => Err(error.into()),
            }
        }

        /// Make directory entries durable.
        pub fn sync(&self) -> io::Result<()> {
            Ok(rustix::fs::fsync(&self.fd)?)
        }

        /// The path SQLite should open for `name`, after proving the database
        /// and every sidecar is absent or a private single-link regular file.
        /// Open it with `SQLITE_OPEN_NOFOLLOW`, then call [`Self::verify`].
        pub fn sqlite_path(&self, name: &str) -> io::Result<PathBuf> {
            check_name(name)?;
            for suffix in SQLITE_SIDECARS {
                let file = format!("{name}{suffix}");
                match rustix::fs::statat(&self.fd, file.as_str(), AtFlags::SYMLINK_NOFOLLOW) {
                    Err(Errno::NOENT) => {}
                    Err(error) => return Err(error.into()),
                    Ok(stat) => {
                        if mode(&stat) & S_IFMT != S_IFREG
                            || links(&stat) != 1
                            || stat.st_uid != euid()
                        {
                            return Err(invalid("refusing an untrusted database file"));
                        }
                    }
                }
            }
            self.verify()?;
            Ok(self.path.join(name))
        }
    }

    /// Normal components as names, `..` kept for physical resolution, `.` dropped.
    fn split(path: &Path) -> VecDeque<OsString> {
        path.components()
            .filter_map(|part| match part {
                Component::Normal(name) => Some(name.to_owned()),
                Component::ParentDir => Some(OsString::from("..")),
                _ => None,
            })
            .collect()
    }
}

#[cfg(not(unix))]
pub use portable::SecureDir;

/// Path-based fallback for platforms without `openat`. Every access rechecks
/// that no component is a link, which narrows the replacement window without
/// closing it.
#[cfg(not(unix))]
mod portable {
    use super::{check_name, invalid, temp_name, TempFile, SQLITE_SIDECARS};
    use std::fs::{self, File, OpenOptions};
    use std::io;
    use std::path::{Path, PathBuf};

    pub struct SecureDir {
        path: PathBuf,
    }

    fn no_links(path: &Path) -> io::Result<()> {
        for ancestor in path.ancestors() {
            if let Ok(metadata) = fs::symlink_metadata(ancestor) {
                if metadata.file_type().is_symlink() {
                    return Err(invalid("refusing a path through a link"));
                }
            }
        }
        Ok(())
    }

    impl SecureDir {
        pub fn open_root(path: &Path, create: bool) -> io::Result<Self> {
            if !path.is_absolute() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "trusted root must be absolute",
                ));
            }
            no_links(path)?;
            if create {
                fs::create_dir_all(path)?;
            }
            let dir = Self {
                path: path.to_owned(),
            };
            dir.verify()?;
            Ok(dir)
        }

        pub fn path(&self) -> &Path {
            &self.path
        }

        pub fn child(&self, name: &str, create: bool) -> io::Result<Self> {
            check_name(name)?;
            let path = self.path.join(name);
            if create {
                match fs::create_dir(&path) {
                    Ok(()) => {}
                    Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                    Err(error) => return Err(error),
                }
            }
            let dir = Self { path };
            dir.verify()?;
            Ok(dir)
        }

        pub fn verify(&self) -> io::Result<()> {
            no_links(&self.path)?;
            if !fs::symlink_metadata(&self.path)?.is_dir() {
                return Err(invalid("trusted directory was replaced"));
            }
            Ok(())
        }

        pub fn open_read(&self, name: &str) -> io::Result<File> {
            check_name(name)?;
            self.verify()?;
            let path = self.path.join(name);
            if !fs::symlink_metadata(&path)?.file_type().is_file() {
                return Err(invalid("expected a regular file"));
            }
            File::open(path)
        }

        pub fn create_temp(&self) -> io::Result<TempFile<'_>> {
            self.verify()?;
            let name = temp_name();
            let file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(self.path.join(&name))?;
            Ok(TempFile {
                dir: self,
                name,
                file,
            })
        }

        pub(super) fn link_noclobber(&self, from: &str, to: &str) -> io::Result<bool> {
            self.verify()?;
            match fs::hard_link(self.path.join(from), self.path.join(to)) {
                Ok(()) => Ok(true),
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => Ok(false),
                Err(error) => Err(error),
            }
        }

        pub fn remove(&self, name: &str) -> io::Result<()> {
            check_name(name)?;
            self.verify()?;
            match fs::remove_file(self.path.join(name)) {
                Ok(()) => Ok(()),
                Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
                Err(error) => Err(error),
            }
        }

        pub fn sync(&self) -> io::Result<()> {
            Ok(())
        }

        pub fn sqlite_path(&self, name: &str) -> io::Result<PathBuf> {
            check_name(name)?;
            self.verify()?;
            for suffix in SQLITE_SIDECARS {
                match fs::symlink_metadata(self.path.join(format!("{name}{suffix}"))) {
                    Ok(metadata) if !metadata.file_type().is_file() => {
                        return Err(invalid("refusing an untrusted database file"))
                    }
                    _ => {}
                }
            }
            Ok(self.path.join(name))
        }
    }
}

/// Flags for a database opened through [`SecureDir::sqlite_path`]: never
/// through a symlink, whether read-write (creating) or read-only.
pub fn sqlite_flags(read_only: bool) -> rusqlite::OpenFlags {
    let base = if read_only {
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX
    } else {
        rusqlite::OpenFlags::default()
    };
    base | rusqlite::OpenFlags::SQLITE_OPEN_NOFOLLOW
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::fs;
    use std::os::unix::fs::{symlink, PermissionsExt};
    use std::path::Path;

    fn chmod(path: &Path, mode: u32) {
        fs::set_permissions(path, fs::Permissions::from_mode(mode)).unwrap();
    }

    fn empty(path: &Path) -> bool {
        fs::read_dir(path).unwrap().next().is_none()
    }

    #[test]
    fn a_symlink_at_any_root_component_is_refused_and_nothing_escapes() {
        for position in 0..3 {
            let temp = tempfile::tempdir().unwrap();
            let outside = temp.path().join("outside");
            fs::create_dir(&outside).unwrap();
            let parts = ["a", "b", "root"];
            let mut path = temp.path().to_path_buf();
            for (index, part) in parts.iter().enumerate() {
                path.push(part);
                if index == position {
                    symlink(&outside, &path).unwrap();
                    break;
                }
                fs::create_dir(&path).unwrap();
            }
            let root = temp.path().join("a/b/root");
            assert!(
                SecureDir::open_root(&root, true).is_err(),
                "position {position}"
            );
            assert!(empty(&outside), "position {position} wrote through a link");
        }
    }

    #[test]
    fn children_objects_and_databases_refuse_links_and_foreign_files() {
        let temp = tempfile::tempdir().unwrap();
        let outside = temp.path().join("outside");
        fs::create_dir(&outside).unwrap();
        fs::write(outside.join("target"), b"secret").unwrap();
        let dir = SecureDir::open_root(&temp.path().join("root"), true).unwrap();
        symlink(&outside, dir.path().join("objects")).unwrap();
        assert!(dir.child("objects", true).is_err());
        assert!(dir.child("objects", false).is_err());
        symlink(outside.join("target"), dir.path().join("object")).unwrap();
        let refused = dir.open_read("object").unwrap_err();
        assert_eq!(refused.kind(), io::ErrorKind::InvalidData);
        fs::create_dir(dir.path().join("directory")).unwrap();
        assert_eq!(
            dir.open_read("directory").unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
        assert!(dir.remove("directory").is_err());
        assert!(dir.remove("..").is_err());
        dir.remove("absent").unwrap();
        for suffix in SQLITE_SIDECARS {
            let name = format!("db{suffix}");
            symlink(outside.join("target"), dir.path().join(&name)).unwrap();
            assert!(dir.sqlite_path("db").is_err(), "{name}");
            fs::remove_file(dir.path().join(&name)).unwrap();
        }
        fs::hard_link(outside.join("target"), dir.path().join("db")).unwrap();
        assert!(dir.sqlite_path("db").is_err());
        fs::remove_file(dir.path().join("db")).unwrap();
        assert!(dir.sqlite_path("db").is_ok());
        assert_eq!(fs::read(outside.join("target")).unwrap(), b"secret");
        chmod(dir.path(), 0o600);
        let denied = dir.sqlite_path("db");
        chmod(dir.path(), 0o700);
        assert!(denied.is_err());
    }

    #[test]
    fn replacement_after_open_is_detected_and_writes_stay_beneath_the_held_root() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("root");
        let dir = SecureDir::open_root(&path, true).unwrap();
        let objects = dir.child("objects", true).unwrap();
        dir.verify().unwrap();
        objects.verify().unwrap();
        let outside = temp.path().join("outside");
        fs::create_dir(&outside).unwrap();
        fs::rename(&path, temp.path().join("moved")).unwrap();
        symlink(&outside, &path).unwrap();
        assert!(dir.verify().is_err());
        assert!(objects.verify().is_err());
        // Held descriptors still name the original directory, not the link.
        let mut temporary = objects.create_temp().unwrap();
        io::Write::write_all(temporary.file(), b"body").unwrap();
        assert!(temporary.publish_noclobber("name").unwrap());
        assert!(!temporary.publish_noclobber("name").unwrap());
        assert!(temporary.publish_noclobber("../escape").is_err());
        drop(temporary);
        assert!(empty(&outside));
        assert_eq!(
            fs::read(temp.path().join("moved/objects/name")).unwrap(),
            b"body"
        );
        assert_eq!(
            fs::read_dir(temp.path().join("moved/objects"))
                .unwrap()
                .count(),
            1
        );
        fs::remove_file(&path).unwrap();
        fs::create_dir(&path).unwrap();
        assert!(dir.verify().is_err());
    }

    #[test]
    fn a_vanished_temporary_cannot_be_published() {
        let temp = tempfile::tempdir().unwrap();
        let dir = SecureDir::open_root(&temp.path().join("root"), true).unwrap();
        let temporary = dir.create_temp().unwrap();
        for entry in fs::read_dir(dir.path()).unwrap() {
            fs::remove_file(entry.unwrap().path()).unwrap();
        }
        assert!(temporary.publish_noclobber("name").is_err());
    }

    #[test]
    fn root_ownership_and_permissions_are_enforced() {
        let temp = tempfile::tempdir().unwrap();
        assert!(SecureDir::open_root(Path::new("relative"), true).is_err());
        assert!(SecureDir::open_root(Path::new("/"), false).is_err());
        assert!(SecureDir::open_root(&temp.path().join("absent"), false).is_err());
        let file = temp.path().join("file");
        fs::write(&file, b"").unwrap();
        assert!(SecureDir::open_root(&file, true).is_err());

        // A readable but too-broad root is narrowed on a writable open.
        let broad = temp.path().join("broad");
        fs::create_dir(&broad).unwrap();
        chmod(&broad, 0o755);
        SecureDir::open_root(&broad, true).unwrap();
        assert_eq!(
            fs::metadata(&broad).unwrap().permissions().mode() & 0o777,
            0o700
        );
        // A writable-by-others root, or one beneath a writable-by-others
        // ancestor without the sticky bit, is refused outright.
        chmod(&broad, 0o777);
        assert!(SecureDir::open_root(&broad, false).is_err());
        let shared = temp.path().join("shared");
        fs::create_dir(&shared).unwrap();
        chmod(&shared, 0o777);
        assert!(SecureDir::open_root(&shared.join("root"), true).is_err());
        chmod(&shared, 0o1777);
        assert!(SecureDir::open_root(&shared.join("root"), true).is_ok());
        chmod(&broad, 0o700);

        // Physical `..` resolution returns to the held parent.
        let dotted = temp.path().join("x/../x/root");
        fs::create_dir(temp.path().join("x")).unwrap();
        let dir = SecureDir::open_root(&dotted, true).unwrap();
        assert!(dir.path().ends_with("x/root"));
        assert!(SecureDir::open_root(
            Path::new("/../..")
                .join(dir.path().strip_prefix("/").unwrap())
                .as_path(),
            false
        )
        .is_ok());

        // Creation fails cleanly beneath a directory we cannot write.
        let locked = temp.path().join("locked");
        fs::create_dir(&locked).unwrap();
        chmod(&locked, 0o500);
        let nested = SecureDir::open_root(&locked.join("root"), true);
        let child = SecureDir::open_root(&locked, false)
            .unwrap()
            .child("new", true);
        chmod(&locked, 0o700);
        assert!(nested.is_err());
        assert!(child.is_err());
        assert!(dir.child("..", true).is_err());
    }

    #[test]
    fn only_root_owned_system_links_are_followed() {
        // Whichever of these is a root-owned system link on this host: it is
        // followed, and the result is still refused as a private root because
        // root, not us, owns what it names.
        let euid = rustix::process::geteuid().as_raw();
        let mut followed = 0;
        for candidate in ["/tmp", "/var", "/bin", "/var/run", "/etc"] {
            let path = Path::new(candidate);
            if fs::symlink_metadata(path).is_ok_and(|m| m.file_type().is_symlink()) {
                followed += 1;
                let result = SecureDir::open_root(path, false);
                assert!(result.is_err() || euid == 0, "{candidate}");
            }
        }
        assert!(followed > 0, "no system symlink to exercise on this host");
    }
}
