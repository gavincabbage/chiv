CREATE TABLE IF NOT EXISTS first_table (
    id uuid,
    first_text TEXT,
    first_int INTEGER
);

INSERT INTO first_table VALUES (
    'eb3898fc-6727-4264-a3eb-0ec1f96fc511',
    'first row',
    22
),(
    '34182855-aef8-4cfa-a299-d442b30dbfd1',
    'lorem ipsum',
    23
),(
    'ddd09249-9461-4177-8b31-12bd0c21271a',
    'final row',
    24
);

CREATE TABLE IF NOT EXISTS second_table (
    id uuid,
    second_text TEXT,
    second_int INTEGER
);

INSERT INTO second_table VALUES (
    'eb3898fc-6727-4264-a3eb-0ec1f96fc511',
    'some text',
    12
),(
    '34182855-aef8-4cfa-a299-d442b30dbfd1',
    'dolor est',
    13
);