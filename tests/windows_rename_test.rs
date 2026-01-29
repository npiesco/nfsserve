//! Integration test for Windows NFS directory rename
//!
//! This test replicates the failure: Windows NFS client refuses to send RENAME
//! for directories when connected to our server, but works with WinNFSd.
//!
//! Run on Windows with: cargo test --test windows_rename_test --features demo

use std::collections::HashMap;
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::oneshot;
use nfsserve::{
    nfs::{fattr3, fileid3, filename3, ftype3, nfspath3, nfsstat3, nfstime3, sattr3, specdata3},
    tcp::{NFSTcpListener, NFSTcp},
    vfs::{DirEntry, NFSFileSystem, ReadDirResult, VFSCapabilities},
};

/// Simple in-memory filesystem for testing
#[derive(Debug, Clone)]
pub struct TestFS {
    dirs: Arc<Mutex<HashMap<(fileid3, String), fileid3>>>,
    next_id: Arc<Mutex<fileid3>>,
}

impl Default for TestFS {
    fn default() -> Self {
        TestFS {
            dirs: Arc::new(Mutex::new(HashMap::new())),
            next_id: Arc::new(Mutex::new(1000)),
        }
    }
}

fn dir_attr(id: fileid3) -> fattr3 {
    fattr3 {
        ftype: ftype3::NF3DIR,
        mode: 0o777,
        nlink: 1,  // WinNFSd uses 1, not 2
        uid: 0,
        gid: 0,
        size: 4096,
        used: 4096,
        rdev: specdata3::default(),
        fsid: 7,  // WinNFSd uses 7, not 0
        fileid: id,
        atime: nfstime3::default(),
        mtime: nfstime3::default(),
        ctime: nfstime3::default(),
    }
}

#[async_trait]
impl NFSFileSystem for TestFS {
    fn root_dir(&self) -> fileid3 { 1 }
    fn capabilities(&self) -> VFSCapabilities { VFSCapabilities::ReadWrite }

    async fn lookup(&self, dirid: fileid3, filename: &filename3) -> Result<fileid3, nfsstat3> {
        let name = String::from_utf8_lossy(filename.as_ref()).to_string();
        if name == "." { return Ok(dirid); }
        if name == ".." { return Ok(1); }
        let dirs = self.dirs.lock().unwrap();
        dirs.get(&(dirid, name)).copied().ok_or(nfsstat3::NFS3ERR_NOENT)
    }

    async fn getattr(&self, id: fileid3) -> Result<fattr3, nfsstat3> {
        Ok(dir_attr(id))
    }

    async fn setattr(&self, id: fileid3, _setattr: sattr3) -> Result<fattr3, nfsstat3> {
        Ok(dir_attr(id))
    }

    async fn read(&self, _id: fileid3, _offset: u64, _count: u32) -> Result<(Vec<u8>, bool), nfsstat3> {
        Err(nfsstat3::NFS3ERR_ISDIR)
    }

    async fn write(&self, _id: fileid3, _offset: u64, _data: &[u8]) -> Result<fattr3, nfsstat3> {
        Err(nfsstat3::NFS3ERR_ISDIR)
    }

    async fn create(&self, _dirid: fileid3, _filename: &filename3, _attr: sattr3) -> Result<(fileid3, fattr3), nfsstat3> {
        Err(nfsstat3::NFS3ERR_NOTSUPP)
    }

    async fn create_exclusive(&self, _dirid: fileid3, _filename: &filename3) -> Result<fileid3, nfsstat3> {
        Err(nfsstat3::NFS3ERR_NOTSUPP)
    }

    async fn mkdir(&self, dirid: fileid3, dirname: &filename3) -> Result<(fileid3, fattr3), nfsstat3> {
        let name = String::from_utf8_lossy(dirname.as_ref()).to_string();
        eprintln!("TEST: MKDIR parent={}, name={}", dirid, name);

        let new_id = {
            let mut next = self.next_id.lock().unwrap();
            let id = *next;
            *next += 1;
            id
        };

        self.dirs.lock().unwrap().insert((dirid, name), new_id);
        Ok((new_id, dir_attr(new_id)))
    }

    async fn remove(&self, _dirid: fileid3, _filename: &filename3) -> Result<(), nfsstat3> {
        Err(nfsstat3::NFS3ERR_NOTSUPP)
    }

    async fn rename(
        &self,
        from_dirid: fileid3,
        from_filename: &filename3,
        to_dirid: fileid3,
        to_filename: &filename3,
    ) -> Result<(), nfsstat3> {
        let from_name = String::from_utf8_lossy(from_filename.as_ref()).to_string();
        let to_name = String::from_utf8_lossy(to_filename.as_ref()).to_string();
        eprintln!("TEST: RENAME from=({}, {}) to=({}, {})", from_dirid, from_name, to_dirid, to_name);

        let mut dirs = self.dirs.lock().unwrap();
        if let Some(id) = dirs.remove(&(from_dirid, from_name)) {
            dirs.insert((to_dirid, to_name), id);
            Ok(())
        } else {
            Err(nfsstat3::NFS3ERR_NOENT)
        }
    }

    async fn readdir(&self, dirid: fileid3, _start_after: fileid3, _max_entries: usize) -> Result<ReadDirResult, nfsstat3> {
        let dirs = self.dirs.lock().unwrap();
        let entries: Vec<DirEntry> = dirs.iter()
            .filter(|((parent, _), _)| *parent == dirid)
            .map(|((_, name), &id)| DirEntry {
                fileid: id,
                name: name.as_bytes().into(),
                attr: dir_attr(id),
            })
            .collect();
        Ok(ReadDirResult { entries, end: true })
    }

    async fn symlink(&self, _dirid: fileid3, _linkname: &filename3, _symlink: &nfspath3, _attr: &sattr3) -> Result<(fileid3, fattr3), nfsstat3> {
        Err(nfsstat3::NFS3ERR_NOTSUPP)
    }

    async fn readlink(&self, _id: fileid3) -> Result<nfspath3, nfsstat3> {
        Err(nfsstat3::NFS3ERR_NOTSUPP)
    }
}

#[tokio::test]
async fn test_windows_directory_rename() {
    // Skip if not on Windows
    if cfg!(not(target_os = "windows")) {
        eprintln!("Skipping test - not on Windows");
        return;
    }

    let fs = TestFS::default();

    // Channels for server ready signals
    let (portmap_ready_tx, portmap_ready_rx) = oneshot::channel::<Result<(), String>>();
    let (nfs_ready_tx, nfs_ready_rx) = oneshot::channel::<Result<(), String>>();

    // Start portmapper on 111
    let fs_clone = fs.clone();
    let portmap_handle = tokio::spawn(async move {
        match NFSTcpListener::bind("0.0.0.0:111", fs_clone).await {
            Ok(mut listener) => {
                listener.with_export_name("share");
                // Tell portmapper to report NFS on port 2049
                listener.with_nfs_port(2049);
                eprintln!("Portmapper bound on 111");
                let _ = portmap_ready_tx.send(Ok(()));
                let _ = listener.handle_forever().await;
            }
            Err(e) => {
                let _ = portmap_ready_tx.send(Err(format!("Failed to bind 111: {:?}", e)));
            }
        }
    });

    // Start NFS on 2049
    let fs_clone = fs.clone();
    let nfs_handle = tokio::spawn(async move {
        match NFSTcpListener::bind("0.0.0.0:2049", fs_clone).await {
            Ok(mut listener) => {
                listener.with_export_name("share");
                eprintln!("NFS bound on 2049");
                let _ = nfs_ready_tx.send(Ok(()));
                let _ = listener.handle_forever().await;
            }
            Err(e) => {
                let _ = nfs_ready_tx.send(Err(format!("Failed to bind 2049: {:?}", e)));
            }
        }
    });

    // Wait for both servers to be ready
    let portmap_result = tokio::time::timeout(Duration::from_secs(5), portmap_ready_rx).await;
    let nfs_result = tokio::time::timeout(Duration::from_secs(5), nfs_ready_rx).await;

    match portmap_result {
        Ok(Ok(Ok(()))) => eprintln!("Portmapper ready"),
        Ok(Ok(Err(e))) => panic!("Portmapper failed: {}", e),
        Ok(Err(_)) => panic!("Portmapper channel closed"),
        Err(_) => panic!("Portmapper timeout"),
    }

    match nfs_result {
        Ok(Ok(Ok(()))) => eprintln!("NFS ready"),
        Ok(Ok(Err(e))) => panic!("NFS failed: {}", e),
        Ok(Err(_)) => panic!("NFS channel closed"),
        Err(_) => panic!("NFS timeout"),
    }

    // Restart NFS client services (blocking is OK here - external process)
    let _ = Command::new("sc.exe").args(["stop", "NfsClnt"]).output();
    std::thread::sleep(Duration::from_secs(1));
    let _ = Command::new("sc.exe").args(["stop", "NfsRdr"]).output();
    std::thread::sleep(Duration::from_secs(1));
    let _ = Command::new("sc.exe").args(["start", "NfsRdr"]).output();
    std::thread::sleep(Duration::from_secs(2));
    let _ = Command::new("sc.exe").args(["start", "NfsClnt"]).output();
    std::thread::sleep(Duration::from_secs(3));

    // Mount the share
    eprintln!("Attempting mount...");
    let mount_output = Command::new("mount.exe")
        .args(["-o", "nolock", "\\\\127.0.0.1\\share", "Z:"])
        .output()
        .expect("Failed to run mount");

    if !mount_output.status.success() {
        let stdout = String::from_utf8_lossy(&mount_output.stdout);
        let stderr = String::from_utf8_lossy(&mount_output.stderr);
        portmap_handle.abort();
        nfs_handle.abort();
        panic!("Mount failed: {} {}", stdout, stderr);
    }
    eprintln!("Mount successful");

    // Create a directory
    let mkdir_output = Command::new("cmd.exe")
        .args(["/C", "mkdir", "Z:\\test_dir"])
        .output()
        .expect("Failed to run mkdir");

    if !mkdir_output.status.success() {
        let stderr = String::from_utf8_lossy(&mkdir_output.stderr);
        let _ = Command::new("umount.exe").args(["Z:"]).output();
        portmap_handle.abort();
        nfs_handle.abort();
        panic!("mkdir failed: {}", stderr);
    }
    eprintln!("mkdir successful");

    // RENAME THE DIRECTORY - This is the test that should fail (RED)
    let rename_output = Command::new("cmd.exe")
        .args(["/C", "move", "Z:\\test_dir", "Z:\\renamed_dir"])
        .output()
        .expect("Failed to run move");

    // Cleanup
    let _ = Command::new("umount.exe").args(["Z:"]).output();
    portmap_handle.abort();
    nfs_handle.abort();

    // Check rename result
    if !rename_output.status.success() {
        let stdout = String::from_utf8_lossy(&rename_output.stdout);
        let stderr = String::from_utf8_lossy(&rename_output.stderr);
        panic!("RENAME FAILED (expected - this is the bug): {} {}", stdout, stderr);
    }

    eprintln!("Directory rename successful!");
}
