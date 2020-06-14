package square

import (
	"context"
	"github.com/jmoiron/sqlx"
	"net/http"
)

// context key for database
type keyDatabaseConnection string

const sqlKey keyDatabaseConnection = "__sql_connection"

// the interface that endpoints must implement to respond to requests
type Controller interface {
	Process(ctx context.Context, w http.ResponseWriter)
}

// a wrapper around the actual controller
// it implements the http handler interface
type HTTPHandler struct {
	Controller Controller
	*sqlx.DB
}

// a simple wrapper/lifecycle to manage the controller resources
// should we require resources management after an API call has been processed we can do it here
// was at first conceived to uniform http response formatting but i did not required additional complexity in the end
func (handler *HTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {

	ctx := context.Background()

	ctx = context.WithValue(ctx, sqlKey, handler.DB)
	handler.Controller.Process(ctx, w)
}

// retrieves the database instance from current context
func DatabaseFromContext(ctx context.Context) *sqlx.DB {
	if bundle := ctx.Value(sqlKey); bundle != nil {
		return bundle.(*sqlx.DB)
	}
	return nil
}
