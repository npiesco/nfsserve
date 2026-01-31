//! Integration test for Windows NFS COMMIT operation
//!
//! Tests that flush operations work correctly through the NFS mount.
//! Windows NFS client calls COMMIT after write when the app flushes.
//!
//! Run on Windows with: cargo test --test windows_commit_test

use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::Write;
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::{oneshot, watch};
use nfsserve::{
    nfs::{fattr3, fileid3, filename3, ftype3, nfspath3, nfsstat3, nfstime3, sattr3, specdata3},
    tcp::{NFSTcp, NFSTcpListener},
    vfs::{DirEntry, NFSFileSystem, ReadDirResult, VFSCapabilities},
};

/// Simple in-memory filesystem for testing with file support
#[derive(Clone)]
pub struct TestFS {
    files: Arc<Mutex<HashMap<(fileid3, String), (fileid3, Vec<u8>)>>>,
    next_id: Arc<Mutex<fileid3>>,
    /// Signal when first NFS operation is received (proves client connected to us)
    connected_tx: Arc<Mutex<Option<watch::Sender<bool>>>>,
}

impl Default for TestFS {
    fn default() -> Self {
        // Pre-create data.csv file
        let mut files = HashMap::new();
        files.insert((1, "data.csv".to_string()), (100, b"id,name\n1,Alice\n".to_vec()));

        TestFS {
            files: Arc::new(Mutex::new(files)),
            next_id: Arc::new(Mutex::new(1000)),
        }
    }
}

fn file_attr(id: fileid3, size: u64) -> fattr3 {
    fattr3 {
        ftype: ftype3::NF3REG,
        mode: 0o666, // World read-write for Windows anonymous access
        nlink: 1,
        uid: 0,
        gid: 0,
        size,
        used: size,
        rdev: specdata3::default(),
        fsid: 7,
        fileid: id,
        atime: nfstime3::default(),
        mtime: nfstime3::default(),
        ctime: nfstime3::default(),
    }
}

fn dir_attr(id: fileid3) -> fattr3 {
    fattr3 {
        ftype: ftype3::NF3DIR,
        mode: 0o777,
        nlink: 2,
        uid: 0,
        gid: 0,
        size: 4096,
        used: 4096,
        rdev: specdata3::default(),
        fsid: 7,
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
        eprintln!(">>> LOOKUP: dirid={}, name={}", dirid, name);
        if name == "." { return Ok(dirid); }
        if name == ".." { return Ok(1); }
        let files = self.files.lock().unwrap();
        let result = files.get(&(dirid, name.clone())).map(|(id, _)| *id).ok_or(nfsstat3::NFS3ERR_NOENT);
        eprintln!(">>> LOOKUP result: {:?}", result);
        result
    }

    async fn getattr(&self, id: fileid3) -> Result<fattr3, nfsstat3> {
        if id == 1 {
            return Ok(dir_attr(id));
        }
        let files = self.files.lock().unwrap();
        for ((_, _), (fid, content)) in files.iter() {
            if *fid == id {
                return Ok(file_attr(id, content.len() as u64));
            }
        }
        Err(nfsstat3::NFS3ERR_NOENT)
    }

    async fn setattr(&self, id: fileid3, _setattr: sattr3) -> Result<fattr3, nfsstat3> {
        self.getattr(id).await
    }

    async fn read(&self, id: fileid3, offset: u64, count: u32) -> Result<(Vec<u8>, bool), nfsstat3> {
        let files = self.files.lock().unwrap();
        for ((_, _), (fid, content)) in files.iter() {
            if *fid == id {
                let start = offset as usize;
                let end = (start + count as usize).min(content.len());
                let data = if start < content.len() {
                    content[start..end].to_vec()
                } else {
                    Vec::new()
                };
                let eof = end >= content.len();
                return Ok((data, eof));
            }
        }
        Err(nfsstat3::NFS3ERR_NOENT)
    }

    async fn write(&self, id: fileid3, offset: u64, data: &[u8]) -> Result<fattr3, nfsstat3> {
        eprintln!("TEST: WRITE id={}, offset={}, len={}", id, offset, data.len());
        let mut files = self.files.lock().unwrap();
        for ((_, _), (fid, content)) in files.iter_mut() {
            if *fid == id {
                // Append or overwrite
                let start = offset as usize;
                if start >= content.len() {
                    content.extend_from_slice(data);
                } else {
                    let end = start + data.len();
                    if end > content.len() {
                        content.resize(end, 0);
                    }
                    content[start..end].copy_from_slice(data);
                }
                return Ok(file_attr(id, content.len() as u64));
            }
        }
        Err(nfsstat3::NFS3ERR_NOENT)
    }

    async fn create(&self, dirid: fileid3, filename: &filename3, _attr: sattr3) -> Result<(fileid3, fattr3), nfsstat3> {
        let name = String::from_utf8_lossy(filename.as_ref()).to_string();
        eprintln!("TEST: CREATE parent={}, name={}", dirid, name);

        let new_id = {
            let mut next = self.next_id.lock().unwrap();
            let id = *next;
            *next += 1;
            id
        };

        self.files.lock().unwrap().insert((dirid, name), (new_id, Vec::new()));
        Ok((new_id, file_attr(new_id, 0)))
    }

    async fn create_exclusive(&self, _dirid: fileid3, _filename: &filename3) -> Result<fileid3, nfsstat3> {
        Err(nfsstat3::NFS3ERR_NOTSUPP)
    }

    async fn mkdir(&self, _dirid: fileid3, _dirname: &filename3) -> Result<(fileid3, fattr3), nfsstat3> {
        Err(nfsstat3::NFS3ERR_NOTSUPP)
    }

    async fn remove(&self, _dirid: fileid3, _filename: &filename3) -> Result<(), nfsstat3> {
        Err(nfsstat3::NFS3ERR_NOTSUPP)
    }

    async fn rename(&self, _from_dirid: fileid3, _from_filename: &filename3, _to_dirid: fileid3, _to_filename: &filename3) -> Result<(), nfsstat3> {
        Err(nfsstat3::NFS3ERR_NOTSUPP)
    }

    async fn readdir(&self, dirid: fileid3, _start_after: fileid3, _max_entries: usize) -> Result<ReadDirResult, nfsstat3> {
        eprintln!(">>> READDIR: dirid={}", dirid);
        let files = self.files.lock().unwrap();
        eprintln!(">>> READDIR: files = {:?}", files.keys().collect::<Vec<_>>());
        let entries: Vec<DirEntry> = files.iter()
            .filter(|((parent, _), _)| *parent == dirid)
            .map(|((_, name), (id, content))| {
                eprintln!(">>> READDIR: adding entry name={}, id={}", name, id);
                DirEntry {
                    fileid: *id,
                    name: name.as_bytes().into(),
                    attr: file_attr(*id, content.len() as u64),
                }
            })
            .collect();
        eprintln!(">>> READDIR: returning {} entries", entries.len());
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
async fn test_windows_commit_on_flush() {
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

    // Wait for servers to be fully ready
    std::thread::sleep(Duration::from_secs(2));
    eprintln!("Servers fully ready");

    // Restart NFS client services
    let _ = Command::new("sc.exe").args(["stop", "NfsClnt"]).output();
    std::thread::sleep(Duration::from_secs(1));
    let _ = Command::new("sc.exe").args(["stop", "NfsRdr"]).output();
    std::thread::sleep(Duration::from_secs(1));
    let _ = Command::new("sc.exe").args(["start", "NfsRdr"]).output();
    std::thread::sleep(Duration::from_secs(2));
    let _ = Command::new("sc.exe").args(["start", "NfsClnt"]).output();
    std::thread::sleep(Duration::from_secs(3));

    // Find a free drive letter
    let drive = ('D'..='Z').rev()
        .filter(|&c| c != 'Y')
        .find(|c| !std::path::Path::new(&format!("{}:\\", c)).exists())
        .expect("No free drive letter");
    let drive_str = format!("{}:", drive);
    eprintln!("Using drive: {}", drive_str);

    // Clean up any stale mount
    let _ = Command::new("umount.exe").args(["-f", &drive_str]).output();
    let _ = Command::new("net.exe").args(["use", &drive_str, "/delete", "/y"]).output();
    std::thread::sleep(Duration::from_secs(1));

    // Mount the share
    eprintln!("Attempting mount...");
    let mount_output = Command::new("mount")
        .args(["-o", "anon,nolock,mtype=soft,fileaccess=6,rsize=128,wsize=128,timeout=60,retry=2", &format!("\\\\localhost\\share"), &drive_str])
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

    // Wait for mount to stabilize
    std::thread::sleep(Duration::from_secs(2));

    // First list directory to see what's there
    let dir_output = Command::new("cmd.exe")
        .args(["/C", "dir", &drive_str])
        .output()
        .expect("Failed to run dir");
    eprintln!("Directory listing:\n{}", String::from_utf8_lossy(&dir_output.stdout));

    // Open file for append, write, and flush - THIS TESTS COMMIT
    let data_csv = format!("{}\\data.csv", drive_str);
    eprintln!("Opening file for append: {}", data_csv);
    let result = OpenOptions::new()
        .append(true)
        .open(&data_csv);

    let flush_result = match result {
        Ok(mut file) => {
            eprintln!("File opened, writing...");
            match file.write_all(b"2,Bob\n") {
                Ok(_) => {
                    eprintln!("Write succeeded, flushing...");
                    file.flush()
                }
                Err(e) => Err(e),
            }
        }
        Err(e) => Err(e),
    };

    // Cleanup
    let _ = Command::new("umount.exe").args([&drive_str]).output();
    portmap_handle.abort();
    nfs_handle.abort();

    // Check result
    match flush_result {
        Ok(()) => eprintln!("SUCCESS: Write and flush completed!"),
        Err(e) => panic!("COMMIT FAILED: {:?}", e),
    }
}
