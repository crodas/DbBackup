BEGIN;
CREATE TABLE foobar (
    id integer not null primary key auto_increment,
    col1 int default NULL,
    col2 varchar(250) default "cesar;  rodas"
);
CREATE INDEX foo ON foobar (col2);
COMMIT
