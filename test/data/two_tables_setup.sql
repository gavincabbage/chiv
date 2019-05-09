CREATE TABLE IF NOT EXISTS first_table (
    text_column TEXT,
    integer_column INTEGER
);

INSERT INTO first_table VALUES (
    'some text',
    12
),(
    'lorem ipsum',
    13
);

CREATE TABLE IF NOT EXISTS second_table (
    text_column TEXT,
    integer_column INTEGER
);

INSERT INTO second_table VALUES (
    'some second text',
    22
),(
    'lorem second ipsum',
    23
),(
    'final row',
    24
);