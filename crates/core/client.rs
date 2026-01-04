use anyhow::Result;
use futures::SinkExt;
use spatio_protocol::{Command, ResponsePayload, ResponseStatus, SBPClientCodec};
use spatio_types::config::SetOptions;
use spatio_types::point::{Point3d, TemporalPoint};
use spatio_types::stats::DbStats;
use std::time::{Duration, SystemTime};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_stream::StreamExt;
use tokio_util::codec::Framed;

pub struct SpatioClient {
    host: String,
    port: u16,
    inner: Mutex<Option<Framed<TcpStream, SBPClientCodec>>>,
    timeout: Duration,
}

impl SpatioClient {
    pub fn new(host: String, port: u16) -> Self {
        Self {
            host,
            port,
            inner: Mutex::new(None),
            timeout: Duration::from_secs(10),
        }
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    async fn get_connection(&self) -> Result<Framed<TcpStream, SBPClientCodec>> {
        let mut inner = self.inner.lock().await;

        if let Some(framed) = inner.take() {
            return Ok(framed);
        }

        let addr = format!("{}:{}", self.host, self.port);
        let stream = tokio::time::timeout(self.timeout, TcpStream::connect(&addr)).await??;
        Ok(Framed::new(stream, SBPClientCodec))
    }

    async fn call(&self, cmd: Command) -> Result<ResponsePayload> {
        let mut framed = self.get_connection().await?;

        let res: Result<ResponsePayload> = async {
            framed.send(cmd).await?;
            let (status, payload) = framed
                .next()
                .await
                .ok_or_else(|| anyhow::anyhow!("Connection closed"))??;

            match status {
                ResponseStatus::Ok => Ok(payload),
                ResponseStatus::Error => {
                    if let ResponsePayload::Error(e) = payload {
                        Err(anyhow::anyhow!(e))
                    } else {
                        Err(anyhow::anyhow!("Unknown error"))
                    }
                }
            }
        }
        .await;

        if let Ok(ref payload) = res {
            *self.inner.lock().await = Some(framed);
            Ok(payload.clone())
        } else {
            res
        }
    }

    pub async fn upsert(
        &self,
        namespace: &str,
        object_id: &str,
        point: Point3d,
        metadata: serde_json::Value,
        opts: Option<SetOptions>,
    ) -> Result<()> {
        let metadata_bytes = serde_json::to_vec(&metadata)?;
        let cmd = Command::Upsert {
            namespace: namespace.to_string(),
            id: object_id.to_string(),
            point,
            metadata: metadata_bytes,
            opts,
        };

        match self.call(cmd).await? {
            ResponsePayload::Ok => Ok(()),
            _ => Err(anyhow::anyhow!("Unexpected response")),
        }
    }

    pub async fn get(
        &self,
        namespace: &str,
        object_id: &str,
    ) -> Result<Option<(Point3d, serde_json::Value)>> {
        let cmd = Command::Get {
            namespace: namespace.to_string(),
            id: object_id.to_string(),
        };

        match self.call(cmd).await? {
            ResponsePayload::Object {
                point, metadata, ..
            } => {
                let metadata_json = serde_json::from_slice(&metadata)?;
                Ok(Some((point, metadata_json)))
            }
            ResponsePayload::Error(e) if e == "Not found" => Ok(None),
            _ => Err(anyhow::anyhow!("Unexpected response")),
        }
    }

    pub async fn query_radius(
        &self,
        namespace: &str,
        center: &Point3d,
        radius: f64,
        limit: usize,
    ) -> Result<Vec<(String, Point3d, serde_json::Value, f64)>> {
        let cmd = Command::QueryRadius {
            namespace: namespace.to_string(),
            center: center.clone(),
            radius,
            limit,
        };

        match self.call(cmd).await? {
            ResponsePayload::Objects(results) => {
                let mut formatted = Vec::with_capacity(results.len());
                for (id, point, metadata, dist) in results {
                    formatted.push((id, point, serde_json::from_slice(&metadata)?, dist));
                }
                Ok(formatted)
            }
            _ => Err(anyhow::anyhow!("Unexpected response")),
        }
    }

    pub async fn knn(
        &self,
        namespace: &str,
        center: &Point3d,
        k: usize,
    ) -> Result<Vec<(String, Point3d, serde_json::Value, f64)>> {
        let cmd = Command::Knn {
            namespace: namespace.to_string(),
            center: center.clone(),
            k,
        };

        match self.call(cmd).await? {
            ResponsePayload::Objects(results) => {
                let mut formatted = Vec::with_capacity(results.len());
                for (id, point, metadata, dist) in results {
                    formatted.push((id, point, serde_json::from_slice(&metadata)?, dist));
                }
                Ok(formatted)
            }
            _ => Err(anyhow::anyhow!("Unexpected response")),
        }
    }

    pub async fn stats(&self) -> Result<DbStats> {
        let cmd = Command::Stats;

        match self.call(cmd).await? {
            ResponsePayload::Stats(stats) => Ok(stats),
            _ => Err(anyhow::anyhow!("Unexpected response")),
        }
    }

    pub async fn delete(&self, namespace: &str, object_id: &str) -> Result<()> {
        let cmd = Command::Delete {
            namespace: namespace.to_string(),
            id: object_id.to_string(),
        };

        match self.call(cmd).await? {
            ResponsePayload::Ok => Ok(()),
            _ => Err(anyhow::anyhow!("Unexpected response")),
        }
    }

    pub async fn query_bbox(
        &self,
        namespace: &str,
        min_x: f64,
        min_y: f64,
        max_x: f64,
        max_y: f64,
        limit: usize,
    ) -> Result<Vec<(String, Point3d, serde_json::Value)>> {
        let cmd = Command::QueryBbox {
            namespace: namespace.to_string(),
            min_x,
            min_y,
            max_x,
            max_y,
            limit,
        };

        match self.call(cmd).await? {
            ResponsePayload::ObjectList(results) => {
                let mut formatted = Vec::with_capacity(results.len());
                for (id, point, metadata) in results {
                    formatted.push((id, point, serde_json::from_slice(&metadata)?));
                }
                Ok(formatted)
            }
            _ => Err(anyhow::anyhow!("Unexpected response")),
        }
    }

    pub async fn insert_trajectory(
        &self,
        namespace: &str,
        object_id: &str,
        trajectory: Vec<TemporalPoint>,
    ) -> Result<()> {
        let cmd = Command::InsertTrajectory {
            namespace: namespace.to_string(),
            id: object_id.to_string(),
            trajectory,
        };

        match self.call(cmd).await? {
            ResponsePayload::Ok => Ok(()),
            _ => Err(anyhow::anyhow!("Unexpected response")),
        }
    }

    pub async fn query_trajectory(
        &self,
        namespace: &str,
        object_id: &str,
        start_time: SystemTime,
        end_time: SystemTime,
        limit: usize,
    ) -> Result<Vec<(Point3d, serde_json::Value, SystemTime)>> {
        let cmd = Command::QueryTrajectory {
            namespace: namespace.to_string(),
            id: object_id.to_string(),
            start_time,
            end_time,
            limit,
        };

        match self.call(cmd).await? {
            ResponsePayload::Trajectory(results) => {
                let mut formatted = Vec::with_capacity(results.len());
                for upd in results {
                    formatted.push((
                        upd.position,
                        serde_json::from_slice(&upd.metadata)?,
                        upd.timestamp,
                    ));
                }
                Ok(formatted)
            }
            _ => Err(anyhow::anyhow!("Unexpected response")),
        }
    }
}
