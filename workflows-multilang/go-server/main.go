package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/tilebox/tilebox-go/workflows/v1"
)

type ScheduleImageCapture struct {
	Location      Geometry   `json:"location"`
	Resolution    Resolution `json:"resolution"`
	SpectralBands []float64  `json:"spectral_bands"`
}

// Geometry and Resolution match Tilebox's ODC Geo task serialization.
type Geometry struct {
	Geometry GeoJSONPoint `json:"geometry"`
	CRS      string       `json:"crs"`
}

type GeoJSONPoint struct {
	Type        string     `json:"type"`
	Coordinates [2]float64 `json:"coordinates"`
}

type Resolution struct {
	X float64 `json:"x"`
	Y float64 `json:"y"`
}

// No need to define the Execute method since we're only submitting the task

// Identifier must match with the task identifier in the Python runner
func (t *ScheduleImageCapture) Identifier() workflows.TaskIdentifier {
	return workflows.NewTaskIdentifier("tilebox.com/schedule_image_capture", "v1.0")
}

// Start an HTTP server to submit jobs
func main() {
	log.Println("Server starting on http://localhost:8080")

	client := workflows.NewClient()
	http.HandleFunc("/submit", submitHandler(client))
	log.Fatal(http.ListenAndServe(":8080", nil))
}

// Submit a job based on some query parameters
func submitHandler(client *workflows.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		latArg := r.URL.Query().Get("lat")
		lonArg := r.URL.Query().Get("lon")
		resolutionArg := r.URL.Query().Get("resolution")
		bandsArg := r.URL.Query().Get("bands[]")

		latFloat, err := strconv.ParseFloat(latArg, 64)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		lonFloat, err := strconv.ParseFloat(lonArg, 64)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		resolutionM, err := strconv.ParseFloat(resolutionArg, 64)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		var spectralBands []float64
		for _, bandArg := range strings.Split(bandsArg, ",") {
			band, err := strconv.ParseFloat(bandArg, 64)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			spectralBands = append(spectralBands, band)
		}

		job, err := client.Jobs.Submit(r.Context(), "Schedule Image capture",
			[]workflows.Task{
				&ScheduleImageCapture{
					Location: Geometry{
						Geometry: GeoJSONPoint{Type: "Point", Coordinates: [2]float64{lonFloat, latFloat}},
						CRS:      "EPSG:4326",
					},
					Resolution:    Resolution{X: resolutionM, Y: -resolutionM},
					SpectralBands: spectralBands,
				},
			},
		)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		_, _ = io.WriteString(w, fmt.Sprintf("Job submitted: %s\n", job.ID))
	}
}
