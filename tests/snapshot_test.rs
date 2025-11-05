use spatio::{DBBuilder, SetOptions};
use std::time::Duration;
use tempfile::NamedTempFile;

#[test]
fn test_snapshot_persistence() {
    let temp = NamedTempFile::new().unwrap();
    let path = temp.path();

    {
        let mut db = DBBuilder::new().snapshot_path(path).build().unwrap();
        db.insert("key1", b"value1", None).unwrap();
        db.insert("key2", b"value2", None).unwrap();
        db.snapshot().unwrap();
    }

    {
        let db = DBBuilder::new().snapshot_path(path).build().unwrap();
        assert_eq!(db.get("key1").unwrap().unwrap().as_ref(), b"value1");
        assert_eq!(db.get("key2").unwrap().unwrap().as_ref(), b"value2");
    }
}

#[test]
fn test_snapshot_auto_save() {
    let temp = NamedTempFile::new().unwrap();
    let path = temp.path();

    {
        let config = spatio::Config::default().with_snapshot_auto_ops(5);
        let mut db = DBBuilder::new()
            .snapshot_path(path)
            .config(config)
            .build()
            .unwrap();

        for i in 0..10 {
            db.insert(format!("key{}", i), format!("value{}", i).as_bytes(), None)
                .unwrap();
        }
    }

    {
        let db = DBBuilder::new().snapshot_path(path).build().unwrap();
        for i in 0..10 {
            let value = db
                .get(format!("key{}", i))
                .unwrap()
                .unwrap_or_else(|| panic!("key{} not found", i));
            assert_eq!(value.as_ref(), format!("value{}", i).as_bytes());
        }
    }
}

#[test]
fn test_snapshot_with_ttl() {
    let temp = NamedTempFile::new().unwrap();
    let path = temp.path();

    {
        let mut db = DBBuilder::new().snapshot_path(path).build().unwrap();
        let opts = SetOptions::with_ttl(Duration::from_secs(3600));
        db.insert("key1", b"value1", Some(opts)).unwrap();
        db.snapshot().unwrap();
    }

    {
        let db = DBBuilder::new().snapshot_path(path).build().unwrap();
        assert_eq!(db.get("key1").unwrap().unwrap().as_ref(), b"value1");
    }
}

#[test]
fn test_snapshot_drop_saves() {
    let temp = NamedTempFile::new().unwrap();
    let path = temp.path();

    {
        let mut db = DBBuilder::new().snapshot_path(path).build().unwrap();
        db.insert("drop_key", b"drop_value", None).unwrap();
    }

    {
        let db = DBBuilder::new().snapshot_path(path).build().unwrap();
        assert_eq!(db.get("drop_key").unwrap().unwrap().as_ref(), b"drop_value");
    }
}

#[test]
fn test_snapshot_overwrite() {
    let temp = NamedTempFile::new().unwrap();
    let path = temp.path();

    {
        let mut db = DBBuilder::new().snapshot_path(path).build().unwrap();
        db.insert("key", b"old_value", None).unwrap();
        db.snapshot().unwrap();
    }

    {
        let mut db = DBBuilder::new().snapshot_path(path).build().unwrap();
        assert_eq!(db.get("key").unwrap().unwrap().as_ref(), b"old_value");
        db.insert("key", b"new_value", None).unwrap();
        db.snapshot().unwrap();
    }

    {
        let db = DBBuilder::new().snapshot_path(path).build().unwrap();
        assert_eq!(db.get("key").unwrap().unwrap().as_ref(), b"new_value");
    }
}

#[test]
fn test_snapshot_empty_database() {
    let temp = NamedTempFile::new().unwrap();
    let path = temp.path();

    {
        let mut db = DBBuilder::new().snapshot_path(path).build().unwrap();
        db.snapshot().unwrap();
    }

    {
        let db = DBBuilder::new().snapshot_path(path).build().unwrap();
        assert!(db.get("nonexistent").unwrap().is_none());
    }
}

#[test]
fn test_in_memory_no_snapshot() {
    let mut db = DBBuilder::new().in_memory().build().unwrap();
    db.insert("key", b"value", None).unwrap();
    assert_eq!(db.get("key").unwrap().unwrap().as_ref(), b"value");
}
