use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;

use nfsserve::{
    nfs::{fattr3, fileid3, filename3, ftype3, nfspath3, nfsstat3, nfstime3, sattr3, specdata3},
    tcp::*,
    vfs::{DirEntry, NFSFileSystem, ReadDirResult, VFSCapabilities},
};

#[derive(Debug)]
pub struct WinTestFS {
    // (parent_id, name) -> id
    dirs: Mutex<HashMap<(fileid3, String), fileid3>>,
    next_id: Mutex<fileid3>,
}

impl Default for WinTestFS {
    fn default() -> Self {
        WinTestFS {
            dirs: Mutex::new(HashMap::new()),
            next_id: Mutex::new(1000),
        }
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
        fsid: 0,
        fileid: id,
        atime: nfstime3::default(),
        mtime: nfstime3::default(),
        ctime: nfstime3::default(),
    }
}

#[async_trait]
impl NFSFileSystem for WinTestFS {
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
        eprintln!("MKDIR: parent={}, name={}", dirid, name);

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
        eprintln!("RENAME: from=({}, {}) to=({}, {})", from_dirid, from_name, to_dirid, to_name);

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

fn main() {
    eprintln!("Starting NFS server on port 2049 and portmapper on 111...");

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let fs = WinTestFS::default();
        let fs2 = WinTestFS::default();

        // Spawn portmapper on 111
        tokio::spawn(async move {
            match NFSTcpListener::bind("0.0.0.0:111", fs2).await {
                Ok(mut listener) => {
                    // Tell portmapper to report NFS on port 2049
                    listener.with_nfs_port(2049);
                    eprintln!("Portmapper listening on 111");
                    let _ = listener.handle_forever().await;
                }
                Err(e) => eprintln!("Failed to bind portmapper on 111: {:?}", e),
            }
        });

        // Main NFS listener on 2049
        let listener = NFSTcpListener::bind("0.0.0.0:2049", fs)
            .await
            .expect("Failed to bind to port 2049");

        eprintln!("Server ready. Mount with: mount -o nolock \\\\127.0.0.1\\ X:");

        listener.handle_forever().await.unwrap();
    });
}
