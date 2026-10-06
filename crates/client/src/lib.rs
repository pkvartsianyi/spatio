//! Spatio Client
//!
//! Native Rust RPC client for Spatio database.
//!
//! # Example
//!
//! ```ignore
//! use spatio_client::SpatioClient;
//!
//! let client = SpatioClient::connect("127.0.0.1:3000".parse()?).await?;
//! client.upsert("ns", "id", point, metadata).await?;
//! ```

#![allow(clippy::too_many_arguments)]

use spatio_server::SpatioServiceClient;
use spatio_types::geo::{DistanceMetric, Point, Polygon};
use spatio_types::point::Point3d;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tarpc::client::{self, RpcError};
use tarpc::context;
use tarpc::tokio_serde::formats::Json;
use thiserror::Error;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

pub use spatio_server::{CurrentLocation, LocationUpdate, Stats};

#[derive(Error, Debug)]
pub enum ClientError {
    #[error("Connection error: {0}")]
    Connection(#[from] std::io::Error),
    #[error("RPC error: {0}")]
    Rpc(#[from] RpcError),
    #[error("Server error: {0}")]
    Server(String),
}

pub type Result<T> = std::result::Result<T, ClientError>;

/// RPC client. If the connection drops, the failing call returns its error and
/// the client reconnects so the next call can succeed.
#[derive(Clone)]
pub struct SpatioClient {
    addr: SocketAddr,
    client: Arc<Mutex<SpatioServiceClient>>,
}

async fn dial(addr: SocketAddr) -> Result<SpatioServiceClient> {
    let socket = tokio::net::TcpStream::connect(addr).await?;
    let codec = LengthDelimitedCodec::builder()
        .max_frame_length(spatio_server::MAX_FRAME_BYTES)
        .new_codec();
    let transport = tarpc::serde_transport::new(Framed::new(socket, codec), Json::default());
    Ok(SpatioServiceClient::new(client::Config::default(), transport).spawn())
}

impl SpatioClient {
    pub async fn connect(addr: SocketAddr) -> Result<Self> {
        let client = dial(addr).await?;
        Ok(Self {
            addr,
            client: Arc::new(Mutex::new(client)),
        })
    }

    async fn call<T, F, Fut>(&self, f: F) -> Result<T>
    where
        F: FnOnce(SpatioServiceClient, context::Context) -> Fut,
        Fut: Future<Output = std::result::Result<std::result::Result<T, String>, RpcError>>,
    {
        let client = self
            .client
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let mut ctx = context::current();
        ctx.deadline = std::time::SystemTime::now() + Duration::from_secs(30);
        match f(client, ctx).await {
            Ok(reply) => reply.map_err(ClientError::Server),
            Err(e) => {
                if matches!(
                    e,
                    RpcError::Shutdown | RpcError::Send(_) | RpcError::Receive(_)
                ) && let Ok(fresh) = dial(self.addr).await
                {
                    *self.client.lock().unwrap_or_else(|e| e.into_inner()) = fresh;
                }
                Err(e.into())
            }
        }
    }

    pub async fn upsert(
        &self,
        namespace: &str,
        id: &str,
        point: Point3d,
        metadata: serde_json::Value,
    ) -> Result<()> {
        let (ns, id) = (namespace.to_string(), id.to_string());
        self.call(|c, ctx| async move { c.upsert(ctx, ns, id, point, metadata).await })
            .await
    }

    pub async fn get(&self, namespace: &str, id: &str) -> Result<Option<CurrentLocation>> {
        let (ns, id) = (namespace.to_string(), id.to_string());
        self.call(|c, ctx| async move { c.get(ctx, ns, id).await })
            .await
    }

    pub async fn delete(&self, namespace: &str, id: &str) -> Result<()> {
        let (ns, id) = (namespace.to_string(), id.to_string());
        self.call(|c, ctx| async move { c.delete(ctx, ns, id).await })
            .await
    }

    pub async fn query_radius(
        &self,
        namespace: &str,
        center: Point3d,
        radius: f64,
        limit: usize,
    ) -> Result<Vec<(CurrentLocation, f64)>> {
        let ns = namespace.to_string();
        self.call(|c, ctx| async move { c.query_radius(ctx, ns, center, radius, limit).await })
            .await
    }

    pub async fn knn(
        &self,
        namespace: &str,
        center: Point3d,
        k: usize,
    ) -> Result<Vec<(CurrentLocation, f64)>> {
        let ns = namespace.to_string();
        self.call(|c, ctx| async move { c.knn(ctx, ns, center, k).await })
            .await
    }

    pub async fn stats(&self) -> Result<Stats> {
        self.call(|c, ctx| async move { c.stats(ctx).await }).await
    }

    pub async fn query_bbox(
        &self,
        namespace: &str,
        min_x: f64,
        min_y: f64,
        max_x: f64,
        max_y: f64,
        limit: usize,
    ) -> Result<Vec<CurrentLocation>> {
        let ns = namespace.to_string();
        self.call(|c, ctx| async move {
            c.query_bbox(ctx, ns, min_x, min_y, max_x, max_y, limit)
                .await
        })
        .await
    }

    pub async fn query_cylinder(
        &self,
        namespace: &str,
        center: Point,
        min_z: f64,
        max_z: f64,
        radius: f64,
        limit: usize,
    ) -> Result<Vec<(CurrentLocation, f64)>> {
        let ns = namespace.to_string();
        self.call(|c, ctx| async move {
            c.query_cylinder(ctx, ns, center, min_z, max_z, radius, limit)
                .await
        })
        .await
    }

    pub async fn query_trajectory(
        &self,
        namespace: &str,
        id: &str,
        start_time: Option<f64>,
        end_time: Option<f64>,
        limit: usize,
    ) -> Result<Vec<LocationUpdate>> {
        let (ns, id) = (namespace.to_string(), id.to_string());
        self.call(|c, ctx| async move {
            c.query_trajectory(ctx, ns, id, start_time, end_time, limit)
                .await
        })
        .await
    }

    /// Append `(unix_seconds, point)` samples to an object's history.
    pub async fn insert_trajectory(
        &self,
        namespace: &str,
        id: &str,
        trajectory: Vec<(f64, Point)>,
    ) -> Result<()> {
        let (ns, id) = (namespace.to_string(), id.to_string());
        self.call(|c, ctx| async move { c.insert_trajectory(ctx, ns, id, trajectory).await })
            .await
    }

    pub async fn query_bbox_3d(
        &self,
        namespace: &str,
        min_x: f64,
        min_y: f64,
        min_z: f64,
        max_x: f64,
        max_y: f64,
        max_z: f64,
        limit: usize,
    ) -> Result<Vec<CurrentLocation>> {
        let ns = namespace.to_string();
        self.call(|c, ctx| async move {
            c.query_bbox_3d(ctx, ns, min_x, min_y, min_z, max_x, max_y, max_z, limit)
                .await
        })
        .await
    }

    pub async fn query_near(
        &self,
        namespace: &str,
        id: &str,
        radius: f64,
        limit: usize,
    ) -> Result<Vec<(CurrentLocation, f64)>> {
        let (ns, id) = (namespace.to_string(), id.to_string());
        self.call(|c, ctx| async move { c.query_near(ctx, ns, id, radius, limit).await })
            .await
    }

    pub async fn contains(
        &self,
        namespace: &str,
        polygon: Polygon,
        limit: usize,
    ) -> Result<Vec<CurrentLocation>> {
        let ns = namespace.to_string();
        self.call(|c, ctx| async move { c.contains(ctx, ns, polygon, limit).await })
            .await
    }

    pub async fn distance(
        &self,
        namespace: &str,
        id1: &str,
        id2: &str,
        metric: Option<DistanceMetric>,
    ) -> Result<Option<f64>> {
        let (ns, id1, id2) = (namespace.to_string(), id1.to_string(), id2.to_string());
        self.call(|c, ctx| async move { c.distance(ctx, ns, id1, id2, metric).await })
            .await
    }

    pub async fn distance_to(
        &self,
        namespace: &str,
        id: &str,
        point: Point,
        metric: Option<DistanceMetric>,
    ) -> Result<Option<f64>> {
        let (ns, id) = (namespace.to_string(), id.to_string());
        self.call(|c, ctx| async move { c.distance_to(ctx, ns, id, point, metric).await })
            .await
    }

    pub async fn convex_hull(&self, namespace: &str) -> Result<Option<Polygon>> {
        let ns = namespace.to_string();
        self.call(|c, ctx| async move { c.convex_hull(ctx, ns).await })
            .await
    }

    pub async fn bounding_box(
        &self,
        namespace: &str,
    ) -> Result<Option<spatio_types::bbox::BoundingBox2D>> {
        let ns = namespace.to_string();
        self.call(|c, ctx| async move { c.bounding_box(ctx, ns).await })
            .await
    }
}
