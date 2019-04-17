CREATE TABLE IF NOT EXISTS "postgres_table" (
    id UUID PRIMARY KEY,
    text_column TEXT,
    char_column VARCHAR(50),
    int_column INTEGER,
    float_column NUMERIC,
    bool_column BOOLEAN,
    ts_column TIMESTAMP
);

INSERT INTO "postgres_table" VALUES (
    'ea09d13c-f441-4550-9492-115f8b409c96',
    'some text',
    'some chars',
    42,
    3.14,
    true,
    '2018-01-04'::timestamp
);

INSERT INTO "postgres_table" VALUES (
    '4289a9e3-32d5-4bad-b79b-034c528e8f41',
    'some other text',
    null,
    100,
    3.141592,
    true,
    '2018-02-04'::timestamp
 );

INSERT INTO "postgres_table" VALUES (
    '7530a381-526a-42aa-a9ba-97fb2bca283f',
    'some more text',
    'some more chars',
    101,
    null,
    false,
    '2018-02-05'::timestamp
);