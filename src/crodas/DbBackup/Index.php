<?php
/*
  +---------------------------------------------------------------------------------+
  | Copyright (c) 2016 César Rodas                                                  |
  +---------------------------------------------------------------------------------+
  | Redistribution and use in source and binary forms, with or without              |
  | modification, are permitted provided that the following conditions are met:     |
  | 1. Redistributions of source code must retain the above copyright               |
  |    notice, this list of conditions and the following disclaimer.                |
  |                                                                                 |
  | 2. Redistributions in binary form must reproduce the above copyright            |
  |    notice, this list of conditions and the following disclaimer in the          |
  |    documentation and/or other materials provided with the distribution.         |
  |                                                                                 |
  | 3. All advertising materials mentioning features or use of this software        |
  |    must display the following acknowledgement:                                  |
  |    This product includes software developed by César D. Rodas.                  |
  |                                                                                 |
  | 4. Neither the name of the César D. Rodas nor the                               |
  |    names of its contributors may be used to endorse or promote products         |
  |    derived from this software without specific prior written permission.        |
  |                                                                                 |
  | THIS SOFTWARE IS PROVIDED BY CÉSAR D. RODAS ''AS IS'' AND ANY                   |
  | EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED       |
  | WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE          |
  | DISCLAIMED. IN NO EVENT SHALL CÉSAR D. RODAS BE LIABLE FOR ANY                  |
  | DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES      |
  | (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;    |
  | LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND     |
  | ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT      |
  | (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS   |
  | SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE                     |
  +---------------------------------------------------------------------------------+
  | Authors: César Rodas <crodas@php.net>                                           |
  +---------------------------------------------------------------------------------+
*/
namespace crodas\DbBackup;

use RuntimeException;
use SQL\Table;
use SQL\TableDiff;
use SQL\Writer\MySQL;
use PDO;

class Index
{
    protected $dbh;
    protected $update;
    protected $store;
    protected $save;
    protected $version;
    protected $table = array();

    protected $tables = array();
    protected $tableName;
    protected $pk;

    public function __construct($file, $sqlfile)
    {
        $this->dbh = new PDO('sqlite:' . $file);
        $this->dbh->exec("CREATE TABLE versions(id integer primary key autoincrement, filename varchar(250) not null)");
        $this->dbh->exec("CREATE TABLE tables(table_id VARCHAR(250) not null primary key, table_sql longtext)");
        $this->dbh->exec("CREATE TABLE rows(row_id varchar(250) not null NOT NULL, hash varchar(10) not null primary key, bitmap varchar(250))");
        $this->dbh->exec('CREATE INDEX version ON rows (version)');
        $this->dbh->exec('ATTACH DATABASE ":memory:" AS Memory');
        $this->dbh->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        $this->dbh->exec('CREATE TABLE Memory.rows (rowid INTEGER)');

        $this->dbh->exec('BEGIN');

        $this->store  = $this->dbh->prepare("INSERT INTO rows VALUES(:rowId, :hash, :bit)");
        $this->update = $this->dbh->prepare("UPDATE rows SET hash=:hash,bitmap=:bit WHERE row_id = :rowId");
        $this->save   = $this->dbh->prepare("INSERT INTO Memory.rows VALUES(?)");
        $this->table  = array(
            'find' => $this->dbh->prepare("SELECT table_sql FROM tables WHERE table_id = ?"),
            'create' => $this->dbh->prepare("INSERT INTO tables VALUES(?, ?)"),
            'update' => $this->dbh->prepare('UPDATE tables SET table_sql = ? WHERE table_id = ?'),
        );
        
        $insert = $this->dbh->prepare("INSERT INTO versions VALUES(null, ?)");
        $this->version = $insert->execute(array($sqlfile));
    }

    public function getDeletes(Callable $each)
    {
        $delete = array();
        $this->dbh->query("COMMIT;");
        $this->dbh->query("BEGIN;");
        //var_dump(iterator_to_Array($this->dbh->query("Select * from Memory.rows")));
        foreach ($this->dbh->query("SELECT r.row_id FROM rows r WHERE r.row_id not in (SELECT m.rowid FROM Memory.rows m)") as $row) {
            $row = json_decode($row['row_id']);
            $table  = array_shift($row);
            if (empty($this->tables[$table])) {
                throw new RuntimeException("Cannot find the definition of the table {$table}");
            }
            $ids    = $this->tables[$table]->getPrimaryKey();
            $fields = array();
            foreach ($row as $i => $value) {
                $fields[$ids[$i]->getName()] = $value;
            }
            $each($table, $fields);
        }
        $this->dbh->query("DELETE FROM rows WHERE row_id not in (SELECT m.rowid FROM Memory.rows m)");
    }

    public function hasPrimaryKey()
    {
        return !empty($this->pk);
    }

    public function getTableChanges(Table $table)
    {
        $this->tables[$table->getName()] = $table;
        $this->tableName = $table->getName();
        $this->pk = $table->getPrimaryKey();

        // MySQL puts table schema and indexes in a single statement
        $mysql = new MySQL;
        $this->table['find']->execute(array($table->getName()));
        $previous = $this->table['find']->fetch();
        if ($previous === false) {
            $this->table['create']->execute(array($table->getName(), $mysql->createTable($table)));
            return array($table);
        }

        $this->table['update']->execute(array($table->getName(), $mysql->createTable($table)));

        $diff = new TableDiff;
        return $diff->diff($previous['table_sql'], $mysql->createTable($table));
    }

    public function getTableSQL()
    {
        return (string)$this->tables[$this->tableName];
    }

    public function __destruct()
    {
        $this->dbh->exec("COMMIT");
    }

    public function rowBitmap(Array $row)
    {
        $bits = "";
        foreach ($row as $value) {
            $bits .= substr(hash('sha256', $value, true), 0, 4);
        }
        return $bits;
    }

    public function compareBitmap(Array $row, $bitmap)
    {
        $newbitmap = str_split($this->rowBitmap($row), 4);
        $bitmap    = str_split($bitmap, 4);
        $changes   = array();
        $i = 0 ;
        foreach ($row as $key => $value) {
            if (empty($bitmap[$i]) || $newbitmap[$i] !== $bitmap[$i]) {
                $changes[$key] = $value;
            }
            ++$i;
        }

        return $changes;
    }

    public function check(Array $rows, Callable $function)
    {
        $hashes = array();
        foreach ($rows as $row) {
            $pk    = array();
            $rowId = array($this->tableName);
            foreach ($this->pk as $column) {
                $pk[$column->getName()] = $row[$column->getName()];
                $rowId[] = $row[$column->getName()];
            }
            $hashes[json_encode($rowId)] = array(
                'primaryKey' => $pk, 
                'hash'  => substr(hash('sha256', serialize(array_values($row)), true), 0, 10), 
                'table' => $this->tableName, 
                'row'   => $row
            );
        }

        $stmt = $this->dbh->prepare("SELECT rowid, * FROM rows WHERE row_id IN (" . implode(',', array_fill(0, count($hashes), '?'))  . ")");
        $stmt->execute(array_keys($hashes));

        foreach ($stmt as $oldRow) {
            $this->save->execute(array($oldRow['row_id']));
            $row = $hashes[$oldRow['row_id']];
            if ($oldRow['hash'] !== $row['hash']) {
                $function('CHANGED', $this->compareBitmap($row['row'], $oldRow['bitmap']), $row['table'], $row['primaryKey']);
                $this->update->execute(array('rowId' => $oldRow['row_id'], 'hash' => $row['hash'], 'bit' => $this->rowBitmap($row['row'])));
            }
            unset($hashes[$oldRow['row_id']]);
        }

        foreach ($hashes as $rowId => $row) {
            $this->save->execute(array($rowId));
            $function('NEW', $row['row'], $row['table'], $row['primaryKey']);
            $this->store->execute(array('rowId' => $rowId, 'hash' => $row['hash'], 'bit' => $this->rowBitmap($row['row'])));
        }
    }
}
