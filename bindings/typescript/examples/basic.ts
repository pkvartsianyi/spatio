import { Point, Spatio } from '../index.js'

const db = Spatio.memory()

const nyc = new Point(-74.006, 40.7128)
db.upsert('cities', 'nyc', nyc, { population: 8_000_000 })
db.upsert('cities', 'brooklyn', new Point(-73.9442, 40.6782), { population: 2_600_000 })

for (const { objectId, distance } of db.queryRadius('cities', nyc, 100_000, 10)) {
  console.log(`${objectId}: ${(distance / 1000).toFixed(1)} km`)
}

db.close()
