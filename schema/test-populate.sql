-- populates the data table with 100k random processables

insert into processables (first, second)
select
    floor(random() * 100000 + 1)::int,
    floor(random() * 100000 + 1)::int
FROM generate_series(1, 100000) s(i)