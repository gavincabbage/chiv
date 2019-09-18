CREATE TABLE IF NOT EXISTS test_table (
    id INTEGER PRIMARY KEY,
    text_column TEXT,
    char_column VARCHAR(50),
    int_column INTEGER,
    float_column DOUBLE,
    bool_column BOOLEAN,
    ts_column TIMESTAMP
);

INSERT INTO test_table VALUES (
    1,
    'some text',
    'some chars',
    42,
    3.14,
    true,
    '2018-01-04 00:00:00'
);

INSERT INTO test_table VALUES (
    2,
    'some other text',
    null,
    100,
    3.141592,
    true,
    '2018-02-04 00:00:00'
 );

INSERT INTO test_table VALUES (
    3,
    'some more text',
    'some more chars',
    101,
    null,
    false,
    '2018-02-05 00:00:00'
);