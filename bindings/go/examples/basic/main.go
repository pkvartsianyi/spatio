// Command basic mirrors the Python quick-start: open an in-memory database,
// upsert a couple of cities, and run a radius query.
package main

import (
	"fmt"
	"log"

	"github.com/twpayne/go-geom"

	spatio "github.com/pkvartsianyi/spatio/bindings/go"
)

func main() {
	version, err := spatio.Version()
	if err != nil {
		log.Fatalf("loading spatio: %v", err)
	}
	fmt.Printf("spatio native library v%s\n", version)

	db, err := spatio.OpenMemory()
	if err != nil {
		log.Fatalf("open: %v", err)
	}
	defer db.Close()

	nyc := geom.NewPointFlat(geom.XY, []float64{-74.0060, 40.7128})
	if err := db.Upsert("cities", "nyc", nyc, map[string]any{"population": 8_000_000}); err != nil {
		log.Fatalf("upsert nyc: %v", err)
	}
	newark := geom.NewPointFlat(geom.XY, []float64{-74.1724, 40.7357})
	if err := db.Upsert("cities", "newark", newark, map[string]any{"population": 311_000}); err != nil {
		log.Fatalf("upsert newark: %v", err)
	}

	nearby, err := db.QueryRadius("cities", nyc, 100_000, 10)
	if err != nil {
		log.Fatalf("query: %v", err)
	}
	fmt.Printf("found %d cities within 100km of NYC:\n", len(nearby))
	for _, n := range nearby {
		meta, _ := n.Metadata()
		fmt.Printf("  %-7s %8.0f m  pop=%v\n", n.ObjectID, n.Distance, meta["population"])
	}
}
