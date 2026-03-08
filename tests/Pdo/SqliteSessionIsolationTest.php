<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/** @spec SPEC-2.4 */
class SqliteSessionIsolationTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE session_test (id INTEGER PRIMARY KEY, val TEXT)';
    }

    protected function getTableNames(): array
    {
        return ['session_test'];
    }

    public function testShadowDataNotSharedBetweenInstances(): void
    {
        $raw1 = new PDO('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        $raw1->exec('CREATE TABLE session_test (id INTEGER PRIMARY KEY, val TEXT)');

        $raw2 = new PDO('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        $raw2->exec('CREATE TABLE session_test (id INTEGER PRIMARY KEY, val TEXT)');

        $pdo1 = ZtdPdo::fromPdo($raw1);
        $pdo2 = ZtdPdo::fromPdo($raw2);

        $pdo1->exec("INSERT INTO session_test (id, val) VALUES (1, 'from_pdo1')");

        $stmt = $pdo1->query('SELECT * FROM session_test WHERE id = 1');
        $this->assertCount(1, $stmt->fetchAll(PDO::FETCH_ASSOC));

        $stmt = $pdo2->query('SELECT * FROM session_test WHERE id = 1');
        $this->assertCount(0, $stmt->fetchAll(PDO::FETCH_ASSOC));
    }

    public function testShadowDataNotPersistedAcrossLifecycle(): void
    {
        $raw = new PDO('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        $raw->exec('CREATE TABLE session_test (id INTEGER PRIMARY KEY, val TEXT)');

        $pdo = ZtdPdo::fromPdo($raw);
        $pdo->exec("INSERT INTO session_test (id, val) VALUES (1, 'temporary')");

        $stmt = $pdo->query('SELECT * FROM session_test WHERE id = 1');
        $this->assertCount(1, $stmt->fetchAll(PDO::FETCH_ASSOC));

        // Simulate end of lifecycle by creating a new wrapper
        $pdo2 = ZtdPdo::fromPdo($raw);
        $stmt = $pdo2->query('SELECT * FROM session_test WHERE id = 1');
        $this->assertCount(0, $stmt->fetchAll(PDO::FETCH_ASSOC));
    }
}
