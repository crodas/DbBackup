<?php

class SimpleTest extends PHPUnit_Framework_TestCase
{
    public static function provider()
    {
        $args = array();
        foreach (glob(__DIR__ . '/changes/*/') as $dir) {
            $args[] = array(
                $dir . '/initial.sql',
                glob($dir . '/change*'),
                $dir . '/expected.sql',
            );
        }
        
        return $args;
    }

    public static function rmdir($dir) { 
        if (!is_dir($dir)) {
            return;
        }
        $files = array_diff(scandir($dir), array('.','..')); 
        foreach ($files as $file) { 
            is_dir("$dir/$file") ? self::rmdir("$dir/$file") : unlink("$dir/$file"); 
        } 
        return rmdir($dir); 
    } 

    protected function reset()
    {
        self::rmdir(__DIR__ . '/temp');
        mkdir(__DIR__ . '/temp');

        $pdo = new PDO("mysql:host=localhost", "root");
        $pdo->exec("drop database test_1");
        $pdo->exec("drop database test_2");
        $pdo->exec("create database test_1");
        $pdo->exec("create database test_2");
    }

    /**
     *  @dataProvider provider
     */
    public function testCases($initial, Array $changes, $expected)
    {
        $this->reset();
        $conn1 = "mysql://root@localhost/test_1";
        $conn2 = "mysql://root@localhost/test_2";
        $backup = new crodas\DbBackup($conn1, __DIR__ . '/temp/backup');
        $backup->restore($initial); 
        $backup->dump();
        foreach ($changes as $change) {
            $backup->restore($change); 
        }
        $backup->dump();

        $newest = new crodas\DbBackup($conn2, __DIR__ . '/temp/backup1');
        $sqls = glob(__DIR__ . '/temp/backup/*.sql');
        foreach ($sqls as $change) {
            $newest->restore($change); 
        }


        $old = new crodas\DbBackup($conn1, __DIR__ . '/temp/old');
        $new = new crodas\DbBackup($conn2, __DIR__ . '/temp/two');
        $old->dump();
        $new->dump();

        $sql1 = glob(__DIR__ . '/temp/old/*.sql');
        $sql2 = glob(__DIR__ . '/temp/two/*.sql');

        $this->assertEquals(
            file_get_contents($sql1[0]),
            file_get_contents($sql2[0]),
            "Initial $initial SQL"
        );
    }
}
