<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests UPSERT (INSERT ... ON DUPLICATE KEY UPDATE) and REPLACE INTO via MySQLi.
 *
 * Cross-platform parity with MysqlUpsertTest (PDO).
 * @spec SPEC-4.2a
 */
class UpsertTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_upsert_test (id INT PRIMARY KEY, val VARCHAR(255))';
    }

    protected function getTableNames(): array
    {
        return ['mi_upsert_test'];
    }


    public function testInsertOnDuplicateKeyUpdateInserts(): void
    {
        $this->mysqli->query("INSERT INTO mi_upsert_test (id, val) VALUES (1, 'hello') ON DUPLICATE KEY UPDATE val = VALUES(val)");

        $result = $this->mysqli->query('SELECT val FROM mi_upsert_test WHERE id = 1');
        $this->assertSame('hello', $result->fetch_assoc()['val']);
    }

    public function testInsertOnDuplicateKeyUpdateUpdates(): void
    {
        $this->mysqli->query("INSERT INTO mi_upsert_test (id, val) VALUES (1, 'original')");
        $this->mysqli->query("INSERT INTO mi_upsert_test (id, val) VALUES (1, 'updated') ON DUPLICATE KEY UPDATE val = VALUES(val)");

        $result = $this->mysqli->query('SELECT val FROM mi_upsert_test WHERE id = 1');
        $this->assertSame('updated', $result->fetch_assoc()['val']);
    }

    public function testReplaceIntoInserts(): void
    {
        $this->mysqli->query("REPLACE INTO mi_upsert_test (id, val) VALUES (1, 'hello')");

        $result = $this->mysqli->query('SELECT val FROM mi_upsert_test WHERE id = 1');
        $this->assertSame('hello', $result->fetch_assoc()['val']);
    }

    public function testReplaceIntoReplaces(): void
    {
        $this->mysqli->query("INSERT INTO mi_upsert_test (id, val) VALUES (1, 'original')");
        $this->mysqli->query("REPLACE INTO mi_upsert_test (id, val) VALUES (1, 'replaced')");

        $result = $this->mysqli->query('SELECT val FROM mi_upsert_test WHERE id = 1');
        $this->assertSame('replaced', $result->fetch_assoc()['val']);
    }

    public function testUpsertIsolation(): void
    {
        $this->mysqli->query("INSERT INTO mi_upsert_test (id, val) VALUES (1, 'hello') ON DUPLICATE KEY UPDATE val = VALUES(val)");

        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_upsert_test');
        $this->assertSame(0, (int) $result->fetch_assoc()['cnt']);
    }
}
