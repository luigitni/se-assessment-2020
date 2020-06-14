package square

import (
	"context"
	"github.com/jmoiron/sqlx"
	"log"
	"net/http"
)

func NewStartController(db *sqlx.DB) *HTTPHandler {
	return &HTTPHandler{
		Controller: &startController{},
		DB:         db,
	}
}

// handles requests made at the /start endpoint
type startController struct{}

func (h *startController) Process(ctx context.Context, w http.ResponseWriter) {

	// check if the batch job is running
	db := DatabaseFromContext(ctx)

	// if the job is running return a 429, as from specifications
	state := GetJobState(db)

	switch state {
	case JobStateRunning:
		w.WriteHeader(http.StatusTooManyRequests)
		return
	case JobStateUndefined:
		http.Error(w, "uknown state", http.StatusServiceUnavailable)
		return
	case JobStatePaused:
		if err := resume(db); err != nil {
			log.Printf("error resuming job: %s", err.Error())
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	// else start the process and return
	go StartProcessing(db)

	w.WriteHeader(http.StatusAccepted)

}
