package square

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/jmoiron/sqlx"
	"log"
	"time"
)

// how many items the instance will process, per transaction cycle
// the smaller the batch size the more accesses to database will be made and the longer it will take
// for the whole processing to complete
const batchSize = 2

// the item to process
// processing means saving in Result the sum of First and Second fields
type Processable struct {
	Id     int
	First  int64
	Second int64
	Result sql.NullInt64
}

// the notify channel to which start and resume request are pushed
const NotifyChannel = "__sq_notify_chan"

// represents the possible states of the processing
type JobState string

const (
	JobStateRunning      = "__running"       // the processing is currently running
	JobStatePaused       = "__paused"        // the process has been paused
	JobStateNotRunning   = "__not_running"   // the process is not running at the moment
	JobStateNeverStarted = "__never_started" // pristine state: no process has ever started. Used in Stat
	JobStateUndefined    = "__undefined"     // unable to determine the job state. this happens in case of database failure
)

var ErrProcessDone = errors.New("square: done processing records")
var ErrProcessPaused = errors.New("square: processing has been paused")

// returns true if a batch processing is currently running
func GetJobState(db *sqlx.DB) JobState {

	// check that nobody has already initiated the request
	var count int64
	res := db.QueryRow("SELECT count(id) FROM jobs")
	err := res.Scan(&count)
	if err != nil {
		return JobStateUndefined
	}

	// no jobs have been started
	if count == 0 {
		return JobStateNeverStarted
	}

	var paused *time.Time

	res = db.QueryRow("SELECT paused FROM jobs WHERE started IS NOT NULL AND ended IS NULL")
	err = res.Scan(&paused)
	if err != nil && err != sql.ErrNoRows {
		return JobStateUndefined
	}
	if err == sql.ErrNoRows {
		// no running job found
		// check if the process has been started at some point.
		// if so there are no currently running processes
		// else we are in a pristine state and stat precoditions are missing

		return JobStateNotRunning
	}

	if paused != nil {
		return JobStatePaused
	}

	return JobStateRunning
}

// checks if a job is currently running
// if not sets it up and runs it
func StartProcessing(db *sqlx.DB) {
	// write that the job has started so that other instance can start as soon as they are spawned
	db = sqlx.NewDb(db.DB, "postgres")

	tx, err := db.Begin()
	if err != nil {
		return
	}

	var jid int64
	// check that nobody has already initiated the request
	res := tx.QueryRow("SELECT id FROM jobs WHERE started IS NOT NULL AND ended IS NULL")
	err = res.Scan(&jid)

	if err != sql.ErrNoRows && err != nil {
		log.Printf("error reading running jobs: %s", err.Error())
		tx.Rollback()
		return
	}

	if err == sql.ErrNoRows {
		// no one has initiated the processing yet, we do start it
		res := tx.QueryRow("INSERT INTO jobs (started) values ($1) RETURNING id", time.Now().UTC())
		if err := res.Scan(&jid); err != nil {
			tx.Rollback()
			log.Printf("error writing job: %s", err.Error())
			return
		}

		// notify all the listeners to start processing
		if _, err := tx.Exec(fmt.Sprintf("NOTIFY %s", NotifyChannel)); err != nil {
			tx.Rollback()
			log.Printf("error notifying start of processing: %s", err)
			return
		}

	}

	if err := tx.Commit(); err != nil {
		log.Print("error committing the job start signal")
		tx.Rollback()
		return
	}

	log.Println("starting batch processing")
	for err == nil {
		err = processBatches(db, jid)
	}

	if err == ErrProcessDone {
		// close the job
		if _, err = db.Exec("UPDATE jobs SET ended = $1 WHERE id = $2 AND ended IS NULL", time.Now().UTC(), jid); err != nil {
			log.Printf("error closing the job: %s", err.Error())
			return
		}
		log.Printf("batch processing is done")
	} else {
		log.Printf("batch processing has been interrupted")
	}
}

// pauses the batch processing
// adding a time stamp to the jobs table
func pause(db *sqlx.DB) error {

	// find the currently running job
	// write that the job has started so that other instance can start as soon as they are spawned
	tx, err := db.Begin()
	if err != nil {
		return err
	}

	var jid int64
	// check that nobody has already initiated the request
	row := tx.QueryRow("SELECT id FROM jobs WHERE started IS NOT NULL AND ended IS NULL AND paused IS NULL")
	err = row.Scan(&jid)

	// no running job, can't pause anything
	// rollback and return
	if err == sql.ErrNoRows {
		log.Printf("no running job found. Can't pause")
		tx.Rollback()
		return nil
	}

	// else we have a runtime error to handle
	if err != nil && err != sql.ErrNoRows {
		log.Printf("error reading running jobs: %s", err.Error())
		tx.Rollback()
		return err
	}

	// actually pause the job
	if _, err := tx.Exec("UPDATE jobs SET paused = $1 WHERE id = $2", time.Now().UTC(), jid); err != nil {
		tx.Rollback()
		log.Printf("error writing job: %s", err.Error())
		return err
	}

	return tx.Commit()
}

// resumes a paused job
// if the job is not paused then silently rolls back
func resume(db *sqlx.DB) error {

	// find the currently running job
	// write that the job has started so that other instance can start as soon as they are spawned
	tx, err := db.Begin()
	if err != nil {
		return err
	}

	var jid int64
	// check that nobody has already initiated the request
	row := tx.QueryRow("SELECT id FROM jobs WHERE started IS NOT NULL AND ended IS NULL AND paused IS NOT NULL")
	err = row.Scan(&jid)

	// no resumable jobs
	// rollback and return
	if err == sql.ErrNoRows {
		log.Printf("no resumable jobs found. Can't resume")
		tx.Rollback()
		return nil
	}

	if err != nil && err != sql.ErrNoRows {
		log.Printf("error reading running jobs: %s", err.Error())
		tx.Rollback()
		return err
	}

	// resume the paused job
	if _, err := tx.Exec("UPDATE jobs SET paused = NULL WHERE id = $1", jid); err != nil {
		tx.Rollback()
		log.Printf("error writing job: %s", err.Error())
		return err
	}

	// notify all the listeners to resume processing
	if _, err := tx.Exec(fmt.Sprintf("NOTIFY %s", NotifyChannel)); err != nil {
		tx.Rollback()
		log.Printf("error notifying start of processing: %s", err)
		return err
	}

	return tx.Commit()
}

// handles the actual processing of the data
// the processing happens in batches, of size batchSize
// at every batch it creates a transaction and locks batchSize unclaimed rows
// until the batch is over
func processBatches(db *sqlx.DB, jid int64) error {

	// stop the processing if it has been paused
	rows, err := db.Query("SELECT 1 FROM jobs WHERE paused IS NOT NULL AND id = $1", jid)
	if err != nil {
		return err
	}

	isPaused := rows.Next()
	rows.Close()

	if isPaused {
		log.Print("processing has been paused")
		return ErrProcessPaused
	}

	// no pause has been called, keep going
	tx, err := db.Begin()
	if err != nil {
		log.Printf("error starting transaction: %s", err.Error())
		return err
	}

	defer func() {
		if err != nil {
			tx.Rollback()
		}
		err = tx.Commit()
	}()

	rows, err = tx.Query("SELECT id, first, second FROM processables WHERE result IS NULL FOR UPDATE SKIP LOCKED LIMIT $1", batchSize)
	if err != nil {
		log.Print("error executing query")
		return err
	}
	defer rows.Close()

	var procs []Processable

	count := 0
	for rows.Next() {
		p := Processable{}
		err = rows.Scan(&p.Id, &p.First, &p.Second)
		if err != nil {
			log.Printf("error scanning processables %d", p.Id)
			return err
		}

		p.Result.Int64 = p.First + p.Second
		p.Result.Valid = true
		procs = append(procs, p)
		count += 1
	}

	if count == 0 {
		return ErrProcessDone
	}

	for _, v := range procs {
		_, err := tx.Exec("UPDATE processables SET result = $1 WHERE id = $2", v.Result, v.Id)
		if err != nil {
			log.Printf("error updating row %d: %s", v.Id, err.Error())
			return err
		}
	}

	return nil
}

func processedRows(db *sqlx.DB) (int64, error) {
	var count int64
	res := db.QueryRow("SELECT COUNT(id) FROM processables WHERE result IS NOT NULL")
	if err := res.Scan(&count); err != nil {
		es := fmt.Errorf("error reading from database: %s", err.Error())
		return count, es
	}

	return count, nil
}
