## SE assessment 14/06/2020

#Installation

Clone the repository
```shell script
git clone git@github.com:luigitni/se-assessment-2020.git
```

build the executable
```shell script
go build -o <preferred package name>
```

then run it

### Configuration

Database connection must be set in an environment variable named "SQUARE_ASS_DB_CONNECTION".

The connection string must be in the PostgreSQL format: "user=username dbname=yourdatabasename password=yourpassword"

The file ```square/processing.go``` contains a variable called ```batchSize``` that can be changed to increase or decreased the number of ```processable``` items being processed in a loop.
Lower values make the process slower and easier to interact with using endpoints.

There's no public configuration API available, since requirements did not mention it. 

### Data

Database migration scripts are contained into the schema folder.
```test-populate.sql``` contains the script to populate the processables tables with 100k random records.

It's safe to truncate the table and repopulate it if the processing has finished.

