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

use crodas\DbBackup\Engine\Database;

class Writer
{
    protected $fp;
    protected $db;
    protected $file;
    protected $wrote = false;

    protected $lastOp;
    protected $lastTable;

    public function __construct(Database $db, $file)
    {
        $this->db   = $db;
        $this->fp   = fopen($file, 'w');
        $this->file = $file;
    }

    public function __destruct()
    {
        if ($this->wrote) {
            $this->writeStream('COMMIT');
            fclose($this->fp);
        } else {
            fclose($this->fp);
            unlink($this->file);
        }
    }

    protected function writeStream($bytes)
    {
        if (!$this->wrote) {
            fwrite($this->fp, "BEGIN;\n");
            $this->wrote = true;
        }
        fwrite($this->fp, $bytes . ";\n");
    }

    public function insert($table, Array $fields)
    {
        if ($this->lastOp === 'insert' && $this->lastTable === $table) {
            fseek($this->fp, -2, SEEK_CUR);
            return $this->writeStream(",(" . $this->db->fields($fields) . ")");
        }
        $this->writeStream("INSERT INTO " .  $this->db->escapeRowName($table) . " VALUES(" . $this->db->fields($fields) . ")");
        $this->lastOp = 'insert';
        $this->lastTable = $table;
    }

    public function writeSQL($sql)
    {
        $this->writeStream($sql);
        $this->lastOp = null;
        $this->lastTable = null;
    }

    public function delete($table, Array $fields)
    {
        $this->writeStream("DELETE FROM " . $this->db->escapeRowName($table) . " WHERE " . $this->db->keyValue($fields, ' AND '));
    }
    
    public function update($table, Array $fields, Array $primaryKey)
    {
        $this->writeStream("UPDATE " . $this->db->escapeRowName($table) 
            . " SET " . $this->db->keyValue($fields, ',') 
            . " WHERE " . $this->db->keyValue($primaryKey, ' AND ')
        );
        $this->lastOp = 'update';
        $this->lastTable = $table;
    }
}
