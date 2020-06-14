-- database creation left intentionally blank because I was not sure how you were going about with
-- the infrastructure. square as database name had high chances of conflicts :)

create table jobs (
    id       serial not null
        constraint jobs_pk
            primary key,
    started  time,
    ended    time,
    paused   time
);

-- auto-generated definition
create table processables (
    id     serial not null
        constraint processable_pk
            primary key,
    first  integer,
    second integer,
    result integer
);