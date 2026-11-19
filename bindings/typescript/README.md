# Spatio for Node.js

Embedded spatio-temporal database for Node.js and TypeScript, powered by Rust.

Spatio tracks moving objects. It runs inside your process, like SQLite, and gives
you real-time spatial queries plus historical trajectories. Types are included.

```bash
npm install spatio-db
```

Prebuilt binaries are available for Linux (x64, arm64, glibc), macOS (arm64) and
Windows (x64). Requires Node.js 20 or newer.

## Quick start

```ts
import { Point, Spatio } from 'spatio-db'

const db = Spatio.memory()

const pos = new Point(-74.006, 40.712, 10)
db.upsert('drones', 'drone_1', pos, { status: 'active' })

for (const { objectId, distance } of db.queryRadius('drones', pos, 500, 10)) {
  console.log(`${objectId} at ${distance.toFixed(1)}m`)
}

db.close()
```

## API

Calls are synchronous, like `better-sqlite3`: they run on the calling thread and
return the result directly. `limit` defaults to 100.

```ts
// Opening
Spatio.memory(config?)
Spatio.open(path, config?)
db.close()

// Objects
db.upsert(namespace, objectId, point, metadata?, { timestamp }?)
db.get(namespace, objectId)     // { point, metadata, timestamp } or null
db.delete(namespace, objectId)

// Proximity: [{ objectId, point, metadata, distance }]
db.queryRadius(namespace, center, radius, limit?)
db.queryNear(namespace, objectId, radius, limit?)
db.knn(namespace, center, k)
db.knnNearObject(namespace, objectId, k)
db.queryWithinCylinder(namespace, center, minZ, maxZ, radius, limit?)
db.queryCylinderNearObject(namespace, objectId, minZ, maxZ, radius, limit?)

// Areas: [{ objectId, point, metadata }]
db.queryBbox(namespace, minX, minY, maxX, maxY, limit?)
db.queryWithinBbox3D(namespace, minX, minY, minZ, maxX, maxY, maxZ, limit?)
db.queryBboxNearObject(namespace, objectId, width, height, limit?)
db.queryBbox3DNearObject(namespace, objectId, width, height, depth, limit?)
db.queryPolygon(namespace, polygon, limit?)

// Trajectories
db.insertTrajectory(namespace, objectId, [{ point, timestamp }])
db.queryTrajectory(namespace, objectId, startTime, endTime, limit?)

// Shapes (stored areas, in their own keyspace)
db.setShape(namespace, shapeId, shape)
db.getShape(namespace, shapeId)        // Shape or null
db.deleteShape(namespace, shapeId)     // true if it existed
db.listShapes(namespace)               // [{ shapeId, shape }] sorted by ID
db.shapesContaining(namespace, point, limit?)          // [shapeId] sorted
db.shapesWithin(namespace, point, meters, limit?)      // [{ shapeId, distance }] nearest first

// Analytics
db.distanceBetween(namespace, id1, id2, 'haversine')
db.distanceTo(namespace, id, point, 'geodesic')
db.convexHull(namespace)
db.boundingBox(namespace)       // { minX, minY, maxX, maxY } or null
db.stats()
```

## Shapes and lines

A `Shape` is a polygon, a circle or a corridor (everything within a distance of
a line). Distances are in meters; circles, corridors and lines follow great
circles, while polygon edges are straight in lon/lat, as in GeoJSON.

```ts
import { LineString, Point, Polygon, Shape, Spatio } from 'spatio-db'

const depot = Shape.circle(new Point(-74.006, 40.712), 500)
const route = Shape.corridor(new LineString([[-74.0, 40.7], [-73.9, 40.8]]), 50)
const zone = Shape.polygon(new Polygon([[-74.1, 40.6], [-73.9, 40.6], [-73.9, 40.8], [-74.1, 40.6]]))

depot.kind                       // 'circle'; also center, radiusM, line, halfWidthM, polygon
depot.contains(point)            // boundary counts
depot.distanceTo(point)          // 0 inside
depot.isWithin(point, meters)

const db = Spatio.memory()
db.setShape('fences', 'depot', depot)
db.shapesContaining('fences', new Point(-74.006, 40.712))   // ['depot']
```

`LineString` takes `[lon, lat]` vertices and offers `coords`, `length()`,
`distanceTo(point)`, `isWithin(point, meters)`, `intersects(other)` and
`intersections(other)` (a `Point[]`).

Invalid shapes (bad coordinates, a negative radius, a corridor with fewer than
two vertices, a polygon with fewer than three) are rejected by `setShape`, not
by the constructors. Shapes are persisted by `Spatio.open` databases.

Distance metrics are `'haversine'`, `'geodesic'`, `'rhumb'` and `'euclidean'`.

**Timestamps are seconds since the Unix epoch**, not milliseconds, so use
`Date.now() / 1000`.

`config` accepts `bufferCapacity` and `persistenceBufferSize`. `bufferCapacity` (default 100) is the number of updates kept per object. For `Spatio.memory()` that is all the history kept, and `delete` removes it.

## Errors

Failures throw an `Error` with a `code`: `InvalidInput`, `ObjectNotFound`,
`DatabaseClosed`, `IoError` or `GenericFailure`. Arguments of the wrong type throw
with `InvalidArg`.

## Development

```bash
npm ci
npm run build:debug
npm test
npm run typecheck
```

## License

MIT
