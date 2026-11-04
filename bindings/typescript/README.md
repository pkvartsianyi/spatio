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

// Analytics
db.distanceBetween(namespace, id1, id2, 'haversine')
db.distanceTo(namespace, id, point, 'geodesic')
db.convexHull(namespace)
db.boundingBox(namespace)       // { minX, minY, maxX, maxY } or null
db.stats()
```

Distance metrics are `'haversine'`, `'geodesic'`, `'rhumb'` and `'euclidean'`.

**Timestamps are seconds since the Unix epoch**, not milliseconds, so use
`Date.now() / 1000`.

`config` accepts `bufferCapacity` and `persistenceBufferSize`.

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
