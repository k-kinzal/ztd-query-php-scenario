<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use Tests\Support\VersionRecorder;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/** @spec SPEC-isolated-fixture-lifecycle */
final class SqliteFixtureLifecycleTest extends TestCase
{
    public function testFixtureLifecycleLeavesPhysicalSeedAndNextSessionUntouched(): void
    {
        $raw = new PDO('sqlite::memory:', null, null, [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
        $raw->exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');
        $raw->exec("INSERT INTO users VALUES (99, 'Physical seed')");
        VersionRecorder::setVersionInfo(self::class,
            $raw->query('SELECT sqlite_version()')->fetchColumn(),
            \Composer\InstalledVersions::getPrettyVersion('k-kinzal/ztd-query-pdo-adapter'));
        $physicalBefore = $raw->query('SELECT * FROM users')->fetchAll(PDO::FETCH_ASSOC);
        $ztd = ZtdPdo::fromPdo($raw);
        self::assertSame([], $ztd->query('SELECT * FROM users')->fetchAll(PDO::FETCH_ASSOC));
        self::assertSame(1, $ztd->exec("INSERT INTO users (id, name) VALUES (1, 'Alice')"));
        self::assertSame([['id' => 1, 'name' => 'Alice']],
            $ztd->query('SELECT * FROM users')->fetchAll(PDO::FETCH_ASSOC));
        self::assertSame(1, $ztd->exec("UPDATE users SET name = 'Updated' WHERE id = 1"));
        self::assertSame('Updated', $ztd->query('SELECT name FROM users WHERE id = 1')->fetchColumn());
        self::assertSame(1, $ztd->exec('DELETE FROM users WHERE id = 1'));
        self::assertSame([], $ztd->query('SELECT * FROM users')->fetchAll(PDO::FETCH_ASSOC));
        self::assertSame($physicalBefore, $raw->query('SELECT * FROM users')->fetchAll(PDO::FETCH_ASSOC));
        $next = ZtdPdo::fromPdo($raw);
        self::assertSame([], $next->query('SELECT * FROM users')->fetchAll(PDO::FETCH_ASSOC));
        self::assertSame($physicalBefore, $raw->query('SELECT * FROM users')->fetchAll(PDO::FETCH_ASSOC));
    }
}
