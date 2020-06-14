package square

import (
	"context"
	"encoding/json"
	"github.com/jmoiron/sqlx"
	"net/http"
)

func NewStatController(db *sqlx.DB) *HTTPHandler {
	return &HTTPHandler{
		Controller: &StatController{},
		DB:         db,
	}
}

// handles requests made at the /start endpoint
type StatController struct{}

func (h *StatController) Process(ctx context.Context, w http.ResponseWriter) {

	// check if the batch job is running
	db := DatabaseFromContext(ctx)

	// if the job is not started return a 412, as from specifications
	state := GetJobState(db)

	if state == JobStateUndefined {
		http.Error(w, "uknown state", http.StatusServiceUnavailable)
		return
	}

	if state == JobStateNeverStarted {
		w.WriteHeader(http.StatusPreconditionFailed)
		return
	}

	count, err := processedRows(db)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// return the count of already processed items as a json object
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	if err := json.NewEncoder(w).Encode(struct {
		Count int64 `json:"count"`
	}{count}); err != nil {
		panic(err)
	}
}
