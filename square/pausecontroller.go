package square

import (
	"context"
	"github.com/jmoiron/sqlx"
	"net/http"
)

func NewPauseController(db *sqlx.DB) *HTTPHandler {
	return &HTTPHandler{
		Controller: &pauseController{},
		DB:         db,
	}
}

// handles requests made at the /pause endpoint
type pauseController struct{}

func (h *pauseController) Process(ctx context.Context, w http.ResponseWriter) {

	// check if the batch job is running
	db := DatabaseFromContext(ctx)

	// if the job is not running we cannot pause it
	// return a 412 - precondition failed
	state := GetJobState(db)

	if state == JobStateUndefined {
		http.Error(w, "uknown state", http.StatusServiceUnavailable)
		return
	}

	if state != JobStateRunning {
		w.WriteHeader(http.StatusPreconditionFailed)
		return
	}

	// else write the pause command
	if err := pause(db); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	w.WriteHeader(http.StatusAccepted)
}
