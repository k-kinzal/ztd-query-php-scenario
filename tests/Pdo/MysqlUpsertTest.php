<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests UPSERT (INSERT ... ON DUPLICATE KEY UPDATE) and REPLACE INTO
 * in ZTD mode on MySQL via PDO.
 * @spec SPEC-4.2a
 */
class MysqlUpsertTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mysql_upsert_test (id INT PRIMARY KEY, val VARCHAR(255))';
    }

    protected function getTableNames(): array
    {
        return ['mysql_upsert_test'];
    }


    public function testInsertOnDuplicateKeyUpdateInserts(): void
    {
        $this->pdo->exec("INSERT INTO mysql_upsert_test (id, val) VALUES (1, 'hello') ON DUPLICATE KEY UPDATE val = VALUES(val)");

        $stmt = $this->pdo->query('SELECT * FROM mysql_upsert_test WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('hello', $rows[0]['val']);
    }

    public function testInsertOnDuplicateKeyUpdateUpdates(): void
    {
        $this->pdo->exec("INSERT INTO mysql_upsert_test (id, val) VALUES (1, 'original')");
        $this->pdo->exec("INSERT INTO mysql_upsert_test (id, val) VALUES (1, 'updated') ON DUPLICATE KEY UPDATE val = VALUES(val)");

        $stmt = $this->pdo->query('SELECT * FROM mysql_upsert_test WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('updated', $rows[0]['val']);
    }

    public function testReplaceIntoInserts(): void
    {
        $this->pdo->exec("REPLACE INTO mysql_upsert_test (id, val) VALUES (1, 'hello')");

        $stmt = $this->pdo->query('SELECT * FROM mysql_upsert_test WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('hello', $rows[0]['val']);
    }

    public function testReplaceIntoReplaces(): void
    {
        $this->pdo->exec("INSERT INTO mysql_upsert_test (id, val) VALUES (1, 'original')");
        $this->pdo->exec("REPLACE INTO mysql_upsert_test (id, val) VALUES (1, 'replaced')");

        $stmt = $this->pdo->query('SELECT * FROM mysql_upsert_test WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('replaced', $rows[0]['val']);
    }

    public function testUpsertIsolation(): void
    {
        $this->pdo->exec("INSERT INTO mysql_upsert_test (id, val) VALUES (1, 'hello') ON DUPLICATE KEY UPDATE val = VALUES(val)");

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT * FROM mysql_upsert_test');
        $this->assertCount(0, $stmt->fetchAll(PDO::FETCH_ASSOC));
    }
}
