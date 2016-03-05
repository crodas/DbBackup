CREATE TABLE foobar (
    id int not null primary key autoincrement,
    col1 int default(250) NULL
);

CREATE INDEX col1_index ON foobar (col1);
