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
namespace crodas;

use PDO;
use SQLParser;
use RuntimeException;
use crodas\DbBackup\Engine\Database;
use crodas\DbBackup\Index;
use crodas\DbBackup\Writer;

class DbBackup
{
    protected $db;
    protected $dbh;
    protected $dir;
    protected $sql;
    protected $index;

    protected function urlToPDO($url)
    {
        $url    = preg_replace('#^(sqlite3?):///?#', '$1://localhost/', $url);
        $params = parse_url($url);
        if ($params === false) {
            throw new InvalidArgumentException("{$url} is not a valid PDO URL");
        }
        $params['path'] = substr($params['path'], 1);
        if (strpos($params['scheme'], 'sqlite') === 0) {
            return new PDO("sqlite:" . $params['path']);
        }
        foreach (['user', 'pass'] as $var) {
            if (empty($params[$var])) {
                $params[$var] = '';
            }
        }
        return new PDO("{$params['scheme']}:host={$params['host']};dbname={$params['path']}", $params['user'], $params['pass']);
    }

    public function restore($file)
    {
        if (!is_file($file)) {
            throw new RuntimeException("$file is not a valid file");
        }

        $buffer = "";
        $fp = fopen($file, 'rb');

        while (!feof($fp)) {
            $buffer .= fread($fp, 8096);
            $len  = strlen($buffer);
            $prev = 0;
            $stop = null;
            for ($i = 0; $i < $len; ++$i) {
                switch ($buffer[$i]) {
                case ';';
                    $this->dbh->exec(substr($buffer, $prev, $i - $prev));
                    $prev = $i+1;
                    break;
                case '"':
                case "'":
                case '`':
                    $stop = $buffer[$i++];
                    for (; $i < $len && $buffer[$i] !== $stop; ++$i) {
                        if ($buffer[$i] === '\\') {
                            ++$i;
                        }
                    }
                    break;
                }
            }

            $buffer = substr($buffer, $prev);
        }

        $buffer = rtrim($buffer);
        if (!empty($buffer)) {
            $this->dbh->exec($buffer);
        }



    }

    protected function pdoToEngine($dbh)
    {
        if (is_string($dbh)) {
            $dbh = $this->urlToPDO($dbh);
        }
        $dbh->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        $dbh->setAttribute(PDO::MYSQL_ATTR_USE_BUFFERED_QUERY, false);
        $dbh->setAttribute(PDO::ATTR_DEFAULT_FETCH_MODE, PDO::FETCH_ASSOC);

        $engine = __NAMESPACE__ . '\DbBackup\Engine\\' . $dbh->getAttribute(PDO::ATTR_DRIVER_NAME); 
        if (!class_exists($engine)) {
            throw new RuntimeException("Cannot find Engine class {$engine}");
        }

        $this->dbh = $dbh;

        return new $engine($dbh);
    }

    public function __construct($dbh, $dir)
    {
        if (!is_dir($dir) && !mkdir($dir)) {
            throw new RuntimeException("$dir is not a valid function");
        }

        $this->db  = $this->pdoToEngine($dbh);
        $this->dir = $dir . '/';
    }

    public function dump()
    {
        $sql   = $this->dir . date('Y-m-d_h_i_s') . '-' . uniqid(true) . '.sql';
        $index = new Index($this->dir . 'index.db', $sql);

        $file = new Writer($this->db, $sql);
        $save = function($status, $row, $table, $primaryKey) use($file) {
            switch ($status) {
            case 'NEW':
                $file->insert($table, $row);
                break;
            case 'CHANGED':
                $file->update($table, $row, $primaryKey);
                break;
            }
        };

        foreach ($this->db->getTables() as $table) {
            $definition = $this->db->getCreateTable($table);
            foreach ($index->getTableChanges($definition) as $change) {
                $file->writeSQL($change);
            }

            if (!$index->hasPrimaryKey()) {
                continue;
            }

            $rows = array();
            foreach ($this->db->getAll($table) as $row) {
                $rows[] = $row;
                if (count($rows) === 20) {
                    $index->check($rows, $save);
                    $rows = array();
                }
            }

            if (count($rows) > 0) {
                $index->check($rows, $save);
            }
        }

        $db = $this->db;
        $index->getDeletes(function($table, $row) use ($file) {
            $file->delete($table, $row);
        });

    }
}
