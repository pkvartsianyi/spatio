use anyhow::Result;
use bytes::{Buf, BufMut, BytesMut};
use serde::{Deserialize, Serialize};
use spatio_types::config::SetOptions;
use spatio_types::point::{Point3d, TemporalPoint};
use spatio_types::stats::DbStats;
use std::time::SystemTime;
use tokio_util::codec::{Decoder, Encoder};

pub const MAX_FRAME_SIZE: usize = 10 * 1024 * 1024; // 10MB

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
        start_time: SystemTime,
        end_time: SystemTime,
        limit: usize,
    },
    InsertTrajectory {
        namespace: String,
        id: String,
        trajectory: Vec<TemporalPoint>,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocationUpdate {
    pub timestamp: SystemTime,
    pub position: Point3d,
    pub metadata: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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

pub struct RpcServerCodec;

impl Decoder for RpcServerCodec {
    type Item = Command;
    type Error = anyhow::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 5 {
            return Ok(None);
        }

        let mut buf = std::io::Cursor::new(&src[..]);
        let _tag = buf.get_u8();
        let len = buf.get_u32() as usize;

        if len > MAX_FRAME_SIZE {
            return Err(anyhow::anyhow!(
                "Frame size {} exceeds maximum {}",
                len,
                MAX_FRAME_SIZE
            ));
        }

        if src.len() < 5 + len {
            return Ok(None);
        }

        src.advance(5);
        let payload = src.split_to(len);
        let cmd: Command = bincode::deserialize(&payload)?;

        Ok(Some(cmd))
    }
}

impl Encoder<(ResponseStatus, ResponsePayload)> for RpcServerCodec {
    type Error = anyhow::Error;

    fn encode(
        &mut self,
        item: (ResponseStatus, ResponsePayload),
        dst: &mut BytesMut,
    ) -> Result<(), Self::Error> {
        let (status, payload) = item;
        let serialized_payload = bincode::serialize(&payload)?;
        let len = serialized_payload.len() as u32;

        dst.reserve(5 + serialized_payload.len());
        dst.put_u8(status as u8);
        dst.put_u32(len);
        dst.put_slice(&serialized_payload);

        Ok(())
    }
}

pub struct RpcClientCodec;

impl Decoder for RpcClientCodec {
    type Item = (ResponseStatus, ResponsePayload);
    type Error = anyhow::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 5 {
            return Ok(None);
        }

        let mut buf = std::io::Cursor::new(&src[..]);
        let status_raw = buf.get_u8();
        let status = if status_raw == 0 {
            ResponseStatus::Ok
        } else {
            ResponseStatus::Error
        };
        let len = buf.get_u32() as usize;

        if len > MAX_FRAME_SIZE {
            return Err(anyhow::anyhow!(
                "Frame size {} exceeds maximum {}",
                len,
                MAX_FRAME_SIZE
            ));
        }

        if src.len() < 5 + len {
            return Ok(None);
        }

        src.advance(5);
        let payload = src.split_to(len);
        let response_payload: ResponsePayload = bincode::deserialize(&payload)?;

        Ok(Some((status, response_payload)))
    }
}

impl Encoder<Command> for RpcClientCodec {
    type Error = anyhow::Error;

    fn encode(&mut self, item: Command, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let serialized_payload = bincode::serialize(&item)?;
        let len = serialized_payload.len() as u32;

        dst.reserve(5 + serialized_payload.len());
        dst.put_u8(0x00); // Tag for Command
        dst.put_u32(len);
        dst.put_slice(&serialized_payload);

        Ok(())
    }
}
