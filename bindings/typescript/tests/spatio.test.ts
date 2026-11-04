import assert from 'node:assert/strict'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { beforeEach, describe, it } from 'node:test'

import { Point, Polygon, Spatio } from '../index.js'

const ns = 'test'
const code = (expected: string) => (err: unknown) => (err as { code: string }).code === expected

let db: Spatio

beforeEach(() => {
  db = Spatio.memory()
})

describe('Point', () => {
  it('exposes coordinates and their aliases', () => {
    const p = new Point(-74, 40.7, 10)
    assert.deepEqual([p.x, p.y, p.z], [-74, 40.7, 10])
    assert.deepEqual([p.lon, p.lat, p.alt], [-74, 40.7, 10])
    assert.equal(new Point(1, 2).z, 0)
    assert.equal(String(p), 'Point(x=-74.0000, y=40.7000, z=10.0000)')
  })

  it('measures distance in meters', () => {
    const distance = new Point(0, 0).distanceTo(new Point(0, 1))
    assert.ok(Math.abs(distance - 111_195) < 100)
  })
})

describe('objects', () => {
  it('round-trips metadata', () => {
    const metadata = { name: 'drone', battery: 0.5, tags: ['a', 'b'], nested: { n: 8_000_000 } }
    db.upsert(ns, 'd1', new Point(1, 2, 3), metadata)

    const found = db.get(ns, 'd1')
    assert.ok(found)
    assert.ok(found.point instanceof Point)
    assert.equal(found.point.z, 3)
    assert.deepEqual(found.metadata, metadata)
  })

  it('returns null for a missing object', () => {
    assert.equal(db.get(ns, 'nope'), null)
  })

  it('deletes', () => {
    db.upsert(ns, 'd1', new Point(1, 2))
    db.delete(ns, 'd1')
    assert.equal(db.get(ns, 'd1'), null)
  })

  it('uses an explicit timestamp', () => {
    db.upsert(ns, 'd1', new Point(1, 2), null, { timestamp: 1000 })
    assert.equal(db.get(ns, 'd1')?.timestamp, 1000)
  })

  it('keeps namespaces apart', () => {
    db.upsert('a', 'd1', new Point(0, 0))
    assert.equal(db.queryRadius('b', new Point(0, 0), 100).length, 0)
  })
})

describe('queries', () => {
  beforeEach(() => {
    db.upsert(ns, 'origin', new Point(0, 0, 0), { kind: 'ground' })
    db.upsert(ns, 'above', new Point(0, 0, 1000))
    db.upsert(ns, 'far', new Point(10, 10, 0))
  })

  it('queryRadius returns neighbors with distance', () => {
    const results = db.queryRadius(ns, new Point(0, 0), 1000)
    assert.deepEqual(results.map((r) => r.objectId).sort(), ['above', 'origin'])
    assert.ok(results.every((r) => r.point instanceof Point && typeof r.distance === 'number'))
  })

  it('applies limit', () => {
    assert.equal(db.queryRadius(ns, new Point(0, 0), 1000, 1).length, 1)
  })

  it('queryNear finds objects around an object', () => {
    const ids = db.queryNear(ns, 'origin', 1000).map((r) => r.objectId)
    assert.ok(ids.includes('above'))
    assert.ok(!ids.includes('far'))
  })

  it('knn orders by distance', () => {
    const results = db.knn(ns, new Point(9, 9), 1)
    assert.equal(results[0].objectId, 'far')
    assert.equal(db.knnNearObject(ns, 'far', 3).length, 3)
  })

  it('queryBbox and queryWithinBbox3D', () => {
    assert.equal(db.queryBbox(ns, -1, -1, 1, 1).length, 2)

    const ids = db.queryWithinBbox3D(ns, -1, -1, -1, 1, 1, 1).map((r) => r.objectId)
    assert.deepEqual(ids, ['origin'])
  })

  it('queryWithinCylinder filters on altitude', () => {
    const results = db.queryWithinCylinder(ns, new Point(0, 0), 500, 1500, 100)
    assert.deepEqual(results.map((r) => r.objectId), ['above'])
  })

  it('queries relative to an object', () => {
    assert.ok(db.queryBboxNearObject(ns, 'origin', 1, 1).length >= 1)
    assert.ok(db.queryBbox3DNearObject(ns, 'origin', 1, 1, 10).length >= 1)
    assert.ok(db.queryCylinderNearObject(ns, 'origin', 500, 1500, 100).length >= 1)
  })

  it('queryPolygon', () => {
    const square = new Polygon([[-1, -1], [1, -1], [1, 1], [-1, 1], [-1, -1]])
    const ids = db.queryPolygon(ns, square).map((r) => r.objectId)
    assert.ok(ids.includes('origin'))
    assert.ok(!ids.includes('far'))
  })

  it('computes distances', () => {
    const between = db.distanceBetween(ns, 'origin', 'far', 'haversine')
    assert.ok(between && between > 1_000_000)
    assert.ok(db.distanceTo(ns, 'origin', new Point(0, 1), 'geodesic'))
    assert.equal(db.distanceBetween(ns, 'origin', 'nope', 'rhumb'), null)
  })

  it('computes hull and bounding box', () => {
    assert.ok(db.convexHull(ns))
    assert.deepEqual(db.boundingBox(ns), { minX: 0, minY: 0, maxX: 10, maxY: 10 })
    assert.equal(db.boundingBox('empty'), null)
  })
})

describe('trajectories', () => {
  it('stores and queries history', () => {
    const trajectory = [1000, 1001, 1002].map((timestamp, i) => ({
      point: new Point(i, i),
      timestamp,
    }))
    db.insertTrajectory(ns, 'd1', trajectory)

    const history = db.queryTrajectory(ns, 'd1', 1000, 1002)
    assert.equal(history.length, 3)
    assert.ok(history[0].point instanceof Point)
    assert.deepEqual(history.map((h) => h.timestamp).sort(), [1000, 1001, 1002])
  })

  it('rejects 3D points', () => {
    assert.throws(
      () => db.insertTrajectory(ns, 'd1', [{ point: new Point(0, 0, 50), timestamp: 1000 }]),
      /z must be 0/,
    )
  })
})

describe('Polygon', () => {
  const ring: [number, number][] = [[0, 0], [4, 0], [4, 4], [0, 0]]
  const hole: [number, number][] = [[1, 1], [2, 1], [2, 2], [1, 1]]

  it('exposes rings', () => {
    const polygon = new Polygon(ring, [hole])
    assert.deepEqual(polygon.exterior, ring)
    assert.deepEqual(polygon.interiors, [hole])
  })

  it('round-trips GeoJSON', () => {
    const polygon = Polygon.fromGeojson(new Polygon(ring).toGeojson())
    assert.deepEqual(polygon.exterior, ring)
  })

  it('rejects bad GeoJSON', () => {
    assert.throws(() => Polygon.fromGeojson('nope'), code('InvalidInput'))
  })
})

describe('errors', () => {
  it('rejects non-finite coordinates', () => {
    assert.throws(() => db.upsert(ns, 'd1', new Point(NaN, 0)), code('InvalidInput'))
  })

  it('reports a missing reference object', () => {
    assert.throws(() => db.queryNear(ns, 'nope', 10), code('ObjectNotFound'))
  })

  it('rejects bad timestamps', () => {
    assert.throws(
      () => db.upsert(ns, 'd1', new Point(0, 0), null, { timestamp: -1 }),
      code('InvalidInput'),
    )
  })

  for (const limit of [-1, 0.5, NaN, Infinity]) {
    it(`rejects limit ${limit}`, () => {
      assert.throws(() => db.queryRadius(ns, new Point(0, 0), 10, limit), code('InvalidInput'))
    })
  }

  it('rejects negative k', () => {
    assert.throws(() => db.knn(ns, new Point(0, 0), -1), code('InvalidInput'))
  })

  it('rejects bad config', () => {
    assert.throws(() => Spatio.memory({ bufferCapacity: 0 }), code('InvalidInput'))
    assert.throws(() => Spatio.memory({ bufferCapacity: -1 }), code('InvalidInput'))
  })

  it('rejects circular metadata', () => {
    const metadata: Record<string, unknown> = {}
    metadata.self = metadata
    assert.throws(() => db.upsert(ns, 'd1', new Point(0, 0), metadata))
  })
})

describe('lifecycle', () => {
  it('reports stats', () => {
    db.upsert(ns, 'd1', new Point(0, 0))
    assert.equal(db.stats().hotStateObjects, 1)
  })

  it('refuses writes after close', () => {
    db.close()
    assert.throws(() => db.upsert(ns, 'd1', new Point(0, 0)), code('DatabaseClosed'))
  })

  it('persists across reopen', () => {
    const dir = mkdtempSync(join(tmpdir(), 'spatio-'))
    const path = join(dir, 'test.db')
    try {
      const first = Spatio.open(path)
      first.upsert(ns, 'd1', new Point(1, 2), { kept: true })
      first.close()

      const second = Spatio.open(path)
      assert.deepEqual(second.get(ns, 'd1')?.metadata, { kept: true })
      second.close()
    } finally {
      rmSync(dir, { recursive: true, force: true })
    }
  })
})
