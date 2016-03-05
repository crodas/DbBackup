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
namespace crodas\DbBackup\Engine;

use PDO;
use SQLParser;
use SQL\Table;
use SQL\AlterTable\AddIndex;
use RuntimeException;

abstract class Database
{
    protected $dbh;

    abstract public function getTables();

    abstract public function getCreateTableSQL($table);

    public function execute($sql, Array $args)
    {
        if (empty($args)) {
            return $this->dbh->query($sql);
        }

        $stmt = $this->dbh->prepare($sql);
        $stmt->execute($args);
        return $stmt;
    }


    final public function __construct(PDO $pdo)
    {
        $this->dbh = $pdo;
        $this->init();
    }

    final public function getCreateTable($table)
    {
        $sql = $this->getCreateTableSQL($table);
        $parser = new SQLParser;
        $sql    = $parser->parse($sql);
        if (!($sql[0] instanceof Table)) {
            throw new RuntimeException("Wrong SQL, expecting a CREATE TABLE, got an " . get_class($sql[0]));
        }
        $table = array_shift($sql);
        foreach ($sql as $stmt) {
            if ($stmt instanceof AddIndex) {
                $table->addIndex($stmt);
            }
        }

        return $table;
    }

    protected function init()
    {
    }

    public function keyValue(Array $data, $join = ", ")
    {
        $str = array();
        foreach ($data as $key => $value) {
            $str[] = $this->escapeRowName($key) . '=' . $this->escape($value);
        }
        return implode($join, $str);
    }

    public function escape($value)
    {
        if (is_numeric($value)) {
            return $value + 0;
        }
        if ($value === NULL) {
            return 'NULL';
        }
        return '"' . addslashes($value) . '"';
    }

    public function fields(Array $data)
    {
        return implode(", ", array_map(array($this, 'escape'), $data));
    }

    public function escapeRowName($row)
    {
        if (preg_match('/^[a-z0-9_]+$/i', $row) && empty(\SQL\ReservedWords::$words[$row])) {
            return $row;
        }
        return var_export($row, true);
    }

    public function getAll($table)
    { 
        return $this->query('SELECT * FROM ' . $this->escapeRowName($table));
    }

    public function query()
    {
        $args = func_get_args();
        $sql  = array_shift($args); 

        return $this->execute($sql, $args);
    }

    public function first()
    {
        $args = func_get_args();
        $sql  = array_shift($args); 
        $rows = $this->execute($sql, $args);
        foreach ($rows as $row) {
            return $row;
        }
    }

    public function pluck()
    {
        $args = func_get_args();
        $sql  = array_shift($args); 
        $rows = $this->execute($sql, $args);

        $result = array();
        foreach ($rows as $row) {
            $result[] = current($row);
        }

        return $result;
    }

}
