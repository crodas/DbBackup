# DbBckup [![Build Status](https://travis-ci.org/crodas/DbBackup.svg?branch=master)](https://travis-ci.org/crodas/DbBackup)
Easiest incremental backup tools for (My?)SQL databases.

## Install

You need [composer](https://getcomposer.org/) in order to install `crodas/DbBackup`.

```
composer require crodas/db-backup
```

Now you need to use it.

```php
require __DIR__ . '/vendor/autoload.php';

$backup = new crodas\DbBackup("mysql://root@localhost/foo", __DIR__ . '/backup-dir/');
$backup->dump(); // Create a new backup
```

## TODO

1. Expose a single binary (phar?)

