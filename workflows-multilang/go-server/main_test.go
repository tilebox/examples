package main

import (
	"encoding/json"
	"testing"
)

func TestScheduleImageCapturePayload(t *testing.T) {
	task := ScheduleImageCapture{
		Location: Geometry{
			Geometry: GeoJSONPoint{Type: "Point", Coordinates: [2]float64{-73.98, 40.75}},
			CRS:      "EPSG:4326",
		},
		Resolution:    Resolution{X: 30, Y: -30},
		SpectralBands: []float64{489, 560.6, 666.5},
	}

	payload, err := json.Marshal(task)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"location":{"geometry":{"type":"Point","coordinates":[-73.98,40.75]},"crs":"EPSG:4326"},"resolution":{"x":30,"y":-30},"spectral_bands":[489,560.6,666.5]}`
	if string(payload) != want {
		t.Fatalf("unexpected task payload:\nwant %s\n got %s", want, payload)
	}
}
