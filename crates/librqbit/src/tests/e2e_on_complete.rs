use std::{
    net::Ipv4Addr,
    path::{Path, PathBuf},
    time::Duration,
};

use tokio::time::{sleep, timeout};

use crate::{
    AddTorrent, AddTorrentOptions, AddTorrentResponse, CreateTorrentOptions, ListenerMode,
    OnCompleteAction, Session, SessionOptions, create_torrent,
    listen::ListenerOptions,
    spawn_utils::BlockingSpawner,
    tests::test_util::{create_default_random_dir_with_torrents, setup_test_logging},
};

const NUM_FILES: usize = 2;
const FILE_LEN: usize = 200_000;

fn collect_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(rd) = std::fs::read_dir(dir) else {
        return;
    };
    for e in rd {
        let p = e.unwrap().path();
        if p.is_dir() {
            collect_files(&p, out);
        } else {
            out.push(p);
        }
    }
}

// Downloads a small torrent from a local seeder into a client configured with `action`,
// waits until the client removes the finished torrent, and returns the files left on disk.
async fn download_and_collect_leftovers(action: OnCompleteAction, port: u16) -> Vec<PathBuf> {
    setup_test_logging();

    let src = create_default_random_dir_with_torrents(NUM_FILES, FILE_LEN, Some("rqbit_on_compl"));
    let torrent_file = create_torrent(
        src.path(),
        CreateTorrentOptions {
            piece_length: Some(32768),
            ..Default::default()
        },
        &BlockingSpawner::new(1),
    )
    .await
    .unwrap();
    let torrent_bytes = torrent_file.as_bytes().unwrap();

    // The seeder.
    let server = Session::new_with_opts(
        std::env::temp_dir().join("does_not_exist"),
        SessionOptions {
            dht: None,
            listen: Some(ListenerOptions {
                mode: ListenerMode::TcpOnly,
                listen_addr: (Ipv4Addr::LOCALHOST, port).into(),
                ..Default::default()
            }),
            disable_local_service_discovery: true,
            ipv4_only: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let seeder = server
        .add_torrent(
            AddTorrent::TorrentFileBytes(torrent_bytes.clone()),
            Some(AddTorrentOptions {
                overwrite: true,
                output_folder: Some(src.path().to_str().unwrap().to_owned()),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .into_handle()
        .unwrap();
    timeout(Duration::from_secs(30), seeder.wait_until_completed())
        .await
        .expect("seeder timed out")
        .unwrap();
    let server_addr = server.listen_addr().unwrap();

    // The client that removes torrents on completion.
    let out = tempfile::TempDir::with_prefix("rqbit_on_compl_client").unwrap();
    let client = Session::new_with_opts(
        out.path().to_owned(),
        SessionOptions {
            dht: None,
            disable_local_service_discovery: true,
            ipv4_only: true,
            on_complete: action,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let id = match client
        .add_torrent(
            AddTorrent::TorrentFileBytes(torrent_bytes),
            Some(AddTorrentOptions {
                initial_peers: Some(vec![server_addr]),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
    {
        AddTorrentResponse::Added(id, _) => id,
        _ => panic!("expected the torrent to be added"),
    };

    timeout(Duration::from_secs(60), async {
        while client.get(id.into()).is_some() {
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the finished torrent was not removed from the session");

    let mut files = Vec::new();
    collect_files(out.path(), &mut files);
    for f in &files {
        assert_eq!(std::fs::metadata(f).unwrap().len(), FILE_LEN as u64);
    }
    files
}

#[tokio::test(flavor = "multi_thread")]
async fn test_on_complete_forget_keeps_files() {
    let files = download_and_collect_leftovers(OnCompleteAction::Forget, 15301).await;
    assert_eq!(files.len(), NUM_FILES, "files must stay on disk: {files:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_on_complete_delete_files_removes_files() {
    let files = download_and_collect_leftovers(OnCompleteAction::DeleteFiles, 15302).await;
    assert!(files.is_empty(), "files must be deleted: {files:?}");
}
