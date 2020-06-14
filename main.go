package main

import (
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	_ "github.com/lib/pq"
	"log"
	"net"
	"net/http"
	"os"
	"square/square"
	"time"
)

// database connection env var key
const EnvDBConnection = "SQUARE_ASS_DB_CONNECTION"

// listens to postgres processing start/resume notifications over channel NotifyChannel
func listen(conn string, db *sqlx.DB) {
	l := pq.NewListener(conn, time.Second, time.Second, nil)
	if err := l.Listen(square.NotifyChannel); err != nil {
		log.Fatal(err)
	}

	for {
		<-l.Notify
		go square.StartProcessing(db)
		fmt.Printf("notification received: processing started\n")
	}
}

func main() {

	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		log.Fatal(err)
	}

	addr := listener.Addr().(*net.TCPAddr)
	ip := addr.IP
	port := addr.Port

	// find a suitable port and listen to that port
	log.Printf("listening on address %s:%d", ip, port)

	// connection must be in the form of "user=username dbname=yourdatabasename password=yourpassword)
	conn := os.Getenv(EnvDBConnection)
	if conn == "" {
		log.Fatalf("no db connection configuration found. Please insert db configuration in %q env variable", EnvDBConnection)
	}

	// instantiate the database.
	// Connection pooling is handled by the library
	db, err := sqlx.Connect("postgres", conn)
	if err != nil {
		log.Fatalf("error connecting to database %s:", err.Error())
		return
	}
	defer db.Close()

	// set up the NOTIFY listener
	go listen(conn, db)

	// if the process has started, the instance joins the processing
	state := square.GetJobState(db)

	if state == square.JobStateUndefined {
		log.Fatal("undefined processing state")
	}

	if state == square.JobStateRunning {
		go square.StartProcessing(db)
	}

	// setup the API endpoints

	// checks if the process was already started.
	// If it is behaves like a circuit-breaker, returning a 429 http status
	// If not responds accordingly and starts it.
	// If the process is paused it resumes it
	http.Handle("/start", square.NewStartController(db))

	// checks if the process is running or finished.
	// if it's running or finished returns the number of items processed
	// if the process is not started returns a 412 http status
	http.Handle("/stat", square.NewStatController(db))

	// pauses the items processing.
	// if the process is already paused or is not running returns a 412 http status
	http.Handle("/pause", square.NewPauseController(db))

	// listen to the allocated port
	if err := http.Serve(listener, nil); err != nil {
		log.Fatal(err)
	}

}
