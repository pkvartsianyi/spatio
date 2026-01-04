use serde::{Deserialize, Serialize};
use spatio::config::DbStats;
use spatio::{Point3d, SetOptions};

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[repr(u8)]
pub enum CommandType {
    Upsert = 0x01,
    Get = 0x02,
    QueryRadius = 0x03,
    Knn = 0x04,
    Stats = 0x05,
    Close = 0x06,
    Delete = 0x07,
    QueryBbox = 0x08,
    QueryCylinder = 0x09,
    QueryTrajectory = 0x0A,
    InsertTrajectory = 0x0B,
    QueryBbox3d = 0x0C,
    QueryNear = 0x0D,
}

impl TryFrom<u8> for CommandType {
    type Error = anyhow::Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x01 => Ok(CommandType::Upsert),
            0x02 => Ok(CommandType::Get),
            0x03 => Ok(CommandType::QueryRadius),
            0x04 => Ok(CommandType::Knn),
            0x05 => Ok(CommandType::Stats),
            0x06 => Ok(CommandType::Close),
            0x07 => Ok(CommandType::Delete),
            0x08 => Ok(CommandType::QueryBbox),
            0x09 => Ok(CommandType::QueryCylinder),
            0x0A => Ok(CommandType::QueryTrajectory),
            0x0B => Ok(CommandType::InsertTrajectory),
            0x0C => Ok(CommandType::QueryBbox3d),
            0x0D => Ok(CommandType::QueryNear),
            _ => Err(anyhow::anyhow!("Unknown command type: 0x{:02X}", value)),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Command {
    Upsert {
        namespace: String,
        id: String,
        point: Point3d,
        metadata: Vec<u8>,
        opts: Option<SetOptions>,
    },
    Get {
        namespace: String,
        id: String,
    },
    QueryRadius {
        namespace: String,
        center: Point3d,
        radius: f64,
        limit: usize,
    },
    Knn {
        namespace: String,
        center: Point3d,
        k: usize,
    },
    Stats,
    Close,
    Delete {
        namespace: String,
        id: String,
    },
    QueryBbox {
        namespace: String,
        min_x: f64,
        min_y: f64,
        max_x: f64,
        max_y: f64,
        limit: usize,
    },
    QueryCylinder {
        namespace: String,
        center_x: f64,
        center_y: f64,
        min_z: f64,
        max_z: f64,
        radius: f64,
        limit: usize,
    },
    QueryTrajectory {
        namespace: String,
        id: String,
        start_time: std::time::SystemTime,
        end_time: std::time::SystemTime,
        limit: usize,
    },
    InsertTrajectory {
        namespace: String,
        id: String,
        trajectory: Vec<spatio_types::point::TemporalPoint>,
    },
    QueryBbox3d {
        namespace: String,
        min_x: f64,
        min_y: f64,
        min_z: f64,
        max_x: f64,
        max_y: f64,
        max_z: f64,
        limit: usize,
    },
    QueryNear {
        namespace: String,
        id: String,
        radius: f64,
        limit: usize,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[repr(u8)]
pub enum ResponseStatus {
    Ok = 0x00,
    Error = 0x01,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LocationUpdate {
    pub timestamp: std::time::SystemTime,
    pub position: Point3d,
    pub metadata: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ResponsePayload {
    Ok,
    Stats(DbStats),
    Object {
        id: String,
        point: Point3d,
        metadata: Vec<u8>,
    },
    Objects(Vec<(String, Point3d, Vec<u8>, f64)>),
    ObjectList(Vec<(String, Point3d, Vec<u8>)>),
    Trajectory(Vec<LocationUpdate>),
    Error(String),
}
