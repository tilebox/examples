package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/tilebox/tilebox-go/workflows/v1"
)

type ScheduleImageCapture struct {
	// json tags must match the Python task definition
	Location      [2]float64 `json:"location"` // lat_lon
	ResolutionM   int        `json:"resolution_m"`
	SpectralBands []float64  `json:"spectral_bands"` // spectral bands in nm
}

// No need to define the Execute method since we're only submitting the task

// Identifier must match with the task identifier in the Python runner
func (t *ScheduleImageCapture) Identifier() workflows.TaskIdentifier {
	return workflows.NewTaskIdentifier("tilebox.com/schedule_image_capture", "v1.0")
}

// Start an HTTP server to submit jobs
func main() {
	ctx := context.Background()
	client := workflows.NewClient()

	cluster, err := client.Clusters.Get(ctx, "test-cluster-tZD9Ca2qsqt4V")
	if err != nil {
		log.Fatal(err)
	}

	http.HandleFunc("/submit", submitHandler(client, cluster))

	log.Println("Server starting on http://localhost:8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

// Submit a job based on some query parameters
func submitHandler(client *workflows.Client, cluster *workflows.Cluster) http.HandlerFunc {
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

		resolutionM, err := strconv.Atoi(resolutionArg)
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

		job, err := client.Jobs.Submit(r.Context(), "Schedule Image capture", cluster,
			[]workflows.Task{
				&ScheduleImageCapture{
					Location:      [2]float64{latFloat, lonFloat},
					ResolutionM:   resolutionM,
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
