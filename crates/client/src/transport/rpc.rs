//! tarpc transport for Spatio client
//!
//! This is the default high-performance RPC client.

use spatio_server::SpatioServiceClient;
use spatio_types::geo::{DistanceMetric, Point, Polygon};
use spatio_types::point::Point3d;
use std::net::SocketAddr;
use std::time::Duration;
use tarpc::client;
use tarpc::context;
use tarpc::tokio_serde::formats::Json;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ClientError {
    #[error("Connection error: {0}")]
    Connection(#[from] std::io::Error),
    #[error("RPC error: {0}")]
    Rpc(#[from] tarpc::client::RpcError),
    #[error("Server error: {0}")]
    Server(String),
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
}

pub type Result<T> = std::result::Result<T, ClientError>;

#[derive(Clone)]
pub struct SpatioClient {
    client: SpatioServiceClient,
}

impl SpatioClient {
    pub async fn connect(addr: SocketAddr) -> Result<Self> {
        let transport = tarpc::serde_transport::tcp::connect(addr, Json::default).await?;
        let client = SpatioServiceClient::new(client::Config::default(), transport).spawn();
        Ok(Self { client })
    }

    pub async fn upsert(
        &self,
        namespace: &str,
        id: &str,
        point: Point3d,
        metadata: serde_json::Value,
        ttl: Option<Duration>,
    ) -> Result<()> {
        let update_ctx = context::current();
        let opts = ttl.map(|t| spatio_server::UpsertOptions { ttl: t });

        self.client
            .upsert(
                update_ctx,
                namespace.to_string(),
                id.to_string(),
                point,
                metadata,
                opts,
            )
            .await?
            .map_err(ClientError::Server)
    }

    pub async fn get(
        &self,
        namespace: &str,
        id: &str,
    ) -> Result<Option<spatio_server::CurrentLocation>> {
        let ctx = context::current();
        self.client
            .get(ctx, namespace.to_string(), id.to_string())
            .await?
            .map_err(ClientError::Server)
    }

    pub async fn delete(&self, namespace: &str, id: &str) -> Result<()> {
        let ctx = context::current();
        self.client
            .delete(ctx, namespace.to_string(), id.to_string())
            .await?
            .map_err(ClientError::Server)
    }

    pub async fn query_radius(
        &self,
        namespace: &str,
        center: Point3d,
        radius: f64,
        limit: usize,
    ) -> Result<Vec<(spatio_server::CurrentLocation, f64)>> {
        let ctx = context::current();
        self.client
            .query_radius(ctx, namespace.to_string(), center, radius, limit)
            .await?
            .map_err(ClientError::Server)
    }

    pub async fn knn(
        &self,
        namespace: &str,
        center: Point3d,
        k: usize,
    ) -> Result<Vec<(spatio_server::CurrentLocation, f64)>> {
        let ctx = context::current();
        self.client
            .knn(ctx, namespace.to_string(), center, k)
            .await?
            .map_err(ClientError::Server)
    }

    pub async fn query_bbox(
        &self,
        namespace: &str,
        min_x: f64,
        min_y: f64,
        max_x: f64,
        max_y: f64,
        limit: usize,
    ) -> Result<Vec<spatio_server::CurrentLocation>> {
        let ctx = context::current();
        self.client
            .query_bbox(
                ctx,
                namespace.to_string(),
                min_x,
                min_y,
                max_x,
                max_y,
                limit,
            )
            .await?
            .map_err(ClientError::Server)
    }

    pub async fn query_cylinder(
        &self,
        namespace: &str,
        center: Point,
        min_z: f64,
        max_z: f64,
        radius: f64,
        limit: usize,
    ) -> Result<Vec<(spatio_server::CurrentLocation, f64)>> {
        let ctx = context::current();
        self.client
            .query_cylinder(
                ctx,
                namespace.to_string(),
                center,
                min_z,
                max_z,
                radius,
                limit,
            )
            .await?
            .map_err(ClientError::Server)
    }

    pub async fn query_trajectory(
        &self,
        namespace: &str,
        id: &str,
        start_time: Option<f64>,
        end_time: Option<f64>,
        limit: usize,
    ) -> Result<Vec<spatio_server::LocationUpdate>> {
        let ctx = context::current();
        self.client
            .query_trajectory(
                ctx,
                namespace.to_string(),
                id.to_string(),
                start_time,
                end_time,
                limit,
            )
            .await?
            .map_err(ClientError::Server)
    }

    pub async fn insert_trajectory(
        &self,
        namespace: &str,
        id: &str,
        trajectory: Vec<(f64, Point3d, serde_json::Value)>,
    ) -> Result<()> {
        let ctx = context::current();
        self.client
            .insert_trajectory(ctx, namespace.to_string(), id.to_string(), trajectory)
            .await?
            .map_err(ClientError::Server)
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
    ) -> Result<Vec<spatio_server::CurrentLocation>> {
        let ctx = context::current();
        self.client
            .query_bbox_3d(
                ctx,
                namespace.to_string(),
                min_x,
                min_y,
                min_z,
                max_x,
                max_y,
                max_z,
                limit,
            )
            .await?
            .map_err(ClientError::Server)
    }

    pub async fn query_near(
        &self,
        namespace: &str,
        id: &str,
        radius: f64,
        limit: usize,
    ) -> Result<Vec<(spatio_server::CurrentLocation, f64)>> {
        let ctx = context::current();
        self.client
            .query_near(ctx, namespace.to_string(), id.to_string(), radius, limit)
            .await?
            .map_err(ClientError::Server)
    }

    pub async fn contains(
        &self,
        namespace: &str,
        polygon: Polygon,
        limit: usize,
    ) -> Result<Vec<spatio_server::CurrentLocation>> {
        let ctx = context::current();
        self.client
            .contains(ctx, namespace.to_string(), polygon, limit)
            .await?
            .map_err(ClientError::Server)
    }

    pub async fn distance(
        &self,
        namespace: &str,
        id1: &str,
        id2: &str,
        metric: Option<DistanceMetric>,
    ) -> Result<Option<f64>> {
        let ctx = context::current();
        self.client
            .distance(
                ctx,
                namespace.to_string(),
                id1.to_string(),
                id2.to_string(),
                metric,
            )
            .await?
            .map_err(ClientError::Server)
    }

    pub async fn distance_to(
        &self,
        namespace: &str,
        id: &str,
        point: Point,
        metric: Option<DistanceMetric>,
    ) -> Result<Option<f64>> {
        let ctx = context::current();
        self.client
            .distance_to(ctx, namespace.to_string(), id.to_string(), point, metric)
            .await?
            .map_err(ClientError::Server)
    }

    pub async fn convex_hull(&self, namespace: &str) -> Result<Option<Polygon>> {
        let ctx = context::current();
        self.client
            .convex_hull(ctx, namespace.to_string())
            .await?
            .map_err(ClientError::Server)
    }

    pub async fn bounding_box(
        &self,
        namespace: &str,
    ) -> Result<Option<spatio_types::bbox::BoundingBox2D>> {
        let ctx = context::current();
        self.client
            .bounding_box(ctx, namespace.to_string())
            .await?
            .map_err(ClientError::Server)
    }

    pub async fn stats(&self) -> Result<spatio_server::Stats> {
        let ctx = context::current();
        Ok(self.client.stats(ctx).await?)
    }
}
