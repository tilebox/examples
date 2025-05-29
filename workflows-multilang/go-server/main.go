package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/tilebox/tilebox-go/workflows/v1"
)

type TaskingWorkflow struct {
	City            string    `json:"city"` // json tags must match the Python task definition
	Time            time.Time `json:"time"`
	ImageResolution string    `json:"image_resolution"`
}

// No need to define the Execute method since we're only submitting the task

// Identifier must match with the task identifier in the Python runner
func (t *TaskingWorkflow) Identifier() workflows.TaskIdentifier {
	return workflows.NewTaskIdentifier("tilebox.com/tasking_workflow", "v1.0")
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
		city := r.URL.Query().Get("city")
		timeArg := r.URL.Query().Get("time")
		resolution := r.URL.Query().Get("resolution")

		if city == "" {
			http.Error(w, "city is required", http.StatusBadRequest)
			return
		}

		taskingTime, err := time.Parse(time.RFC3339, timeArg)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		job, err := client.Jobs.Submit(r.Context(), fmt.Sprintf("tasking/%s", city), cluster,
			[]workflows.Task{
				&TaskingWorkflow{
					City:            city,
					Time:            taskingTime,
					ImageResolution: resolution,
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
