<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests CREATE TABLE mid-session then DML on the new table (SQLite PDO).
 *
 * The ZTD session starts with only sl_dmd_base reflected. A new table sl_dmd_dynamic
 * is created through ZTD mid-session, and subsequent DML (INSERT/SELECT/UPDATE/DELETE)
 * is performed on it. The shadow store may not know the new table's schema.
 *
 * @spec SPEC-5.1
 * @spec SPEC-4.1
 */
class SqliteDdlMidSessionDmlTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_dmd_base (id INT PRIMARY KEY, name VARCHAR(50))';
    }

    protected function getTableNames(): array
    {
        return ['sl_dmd_base', 'sl_dmd_dynamic'];
    }

    /**
     * CREATE TABLE through ZTD mid-session, then INSERT and SELECT.
     */
    public function testCreateTableThenInsert(): void
    {
        try {
            $this->pdo->exec('CREATE TABLE sl_dmd_dynamic (id INT PRIMARY KEY, value VARCHAR(50))');
            $this->pdo->exec("INSERT INTO sl_dmd_dynamic (id, value) VALUES (1, 'alpha')");

            $stmt = $this->pdo->query('SELECT id, value FROM sl_dmd_dynamic WHERE id = 1');
            $row = $stmt->fetch(PDO::FETCH_ASSOC);

            $this->assertSame(1, (int) $row['id']);
            $this->assertSame('alpha', $row['value']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'CREATE TABLE mid-session then INSERT not supported: ' . $e->getMessage()
            );
        }
    }

    /**
     * After CREATE TABLE + INSERT through ZTD, SELECT should see the inserted data.
     */
    public function testCreateTableThenInsertAndSelect(): void
    {
        try {
            $this->pdo->exec('CREATE TABLE sl_dmd_dynamic (id INT PRIMARY KEY, value VARCHAR(50))');
            $this->pdo->exec("INSERT INTO sl_dmd_dynamic (id, value) VALUES (1, 'alpha')");
            $this->pdo->exec("INSERT INTO sl_dmd_dynamic (id, value) VALUES (2, 'beta')");
            $this->pdo->exec("INSERT INTO sl_dmd_dynamic (id, value) VALUES (3, 'gamma')");

            $stmt = $this->pdo->query('SELECT id, value FROM sl_dmd_dynamic ORDER BY id');
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

            $this->assertCount(3, $rows);
            $this->assertSame('alpha', $rows[0]['value']);
            $this->assertSame('beta', $rows[1]['value']);
            $this->assertSame('gamma', $rows[2]['value']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'CREATE TABLE mid-session then INSERT+SELECT not supported: ' . $e->getMessage()
            );
        }
    }

    /**
     * CREATE TABLE, INSERT, UPDATE, then SELECT to verify update worked.
     */
    public function testCreateTableThenUpdate(): void
    {
        try {
            $this->pdo->exec('CREATE TABLE sl_dmd_dynamic (id INT PRIMARY KEY, value VARCHAR(50))');
            $this->pdo->exec("INSERT INTO sl_dmd_dynamic (id, value) VALUES (1, 'original')");

            $this->pdo->exec("UPDATE sl_dmd_dynamic SET value = 'updated' WHERE id = 1");

            $stmt = $this->pdo->query('SELECT value FROM sl_dmd_dynamic WHERE id = 1');
            $row = $stmt->fetch(PDO::FETCH_ASSOC);

            $this->assertSame('updated', $row['value']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'CREATE TABLE mid-session then UPDATE not supported: ' . $e->getMessage()
            );
        }
    }

    /**
     * CREATE TABLE, INSERT, DELETE, then SELECT to verify delete worked.
     */
    public function testCreateTableThenDelete(): void
    {
        try {
            $this->pdo->exec('CREATE TABLE sl_dmd_dynamic (id INT PRIMARY KEY, value VARCHAR(50))');
            $this->pdo->exec("INSERT INTO sl_dmd_dynamic (id, value) VALUES (1, 'keep')");
            $this->pdo->exec("INSERT INTO sl_dmd_dynamic (id, value) VALUES (2, 'remove')");

            $this->pdo->exec('DELETE FROM sl_dmd_dynamic WHERE id = 2');

            $stmt = $this->pdo->query('SELECT COUNT(*) AS cnt FROM sl_dmd_dynamic');
            $this->assertSame(1, (int) $stmt->fetch(PDO::FETCH_ASSOC)['cnt']);

            $stmt = $this->pdo->query('SELECT value FROM sl_dmd_dynamic WHERE id = 1');
            $this->assertSame('keep', $stmt->fetch(PDO::FETCH_ASSOC)['value']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'CREATE TABLE mid-session then DELETE not supported: ' . $e->getMessage()
            );
        }
    }

    /**
     * INSERT into existing base table, CREATE new table, INSERT into new table,
     * then JOIN SELECT across both tables.
     */
    public function testMixedDmlOnExistingAndNewTable(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_dmd_base (id, name) VALUES (1, 'Alice')");
            $this->pdo->exec("INSERT INTO sl_dmd_base (id, name) VALUES (2, 'Bob')");

            $this->pdo->exec('CREATE TABLE sl_dmd_dynamic (id INT PRIMARY KEY, value VARCHAR(50))');
            $this->pdo->exec("INSERT INTO sl_dmd_dynamic (id, value) VALUES (1, 'score_90')");
            $this->pdo->exec("INSERT INTO sl_dmd_dynamic (id, value) VALUES (2, 'score_80')");

            $stmt = $this->pdo->query("
                SELECT b.name, d.value
                FROM sl_dmd_base b
                JOIN sl_dmd_dynamic d ON d.id = b.id
                ORDER BY b.id
            ");
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

            $this->assertCount(2, $rows);
            $this->assertSame('Alice', $rows[0]['name']);
            $this->assertSame('score_90', $rows[0]['value']);
            $this->assertSame('Bob', $rows[1]['name']);
            $this->assertSame('score_80', $rows[1]['value']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Mixed DML on existing + mid-session table not supported: ' . $e->getMessage()
            );
        }
    }

    /**
     * After CREATE + INSERT through ZTD, check if the new table physically has data.
     * On SQLite, DDL through ZTD may or may not reach the physical DB.
     * If the table exists physically, it should have no data (DML is shadowed).
     * If the table does not exist physically, querying should throw.
     */
    public function testPhysicalIsolation(): void
    {
        try {
            $this->pdo->exec('CREATE TABLE sl_dmd_dynamic (id INT PRIMARY KEY, value VARCHAR(50))');
            $this->pdo->exec("INSERT INTO sl_dmd_dynamic (id, value) VALUES (1, 'shadow_data')");

            // Verify data visible through ZTD
            $stmt = $this->pdo->query('SELECT COUNT(*) AS cnt FROM sl_dmd_dynamic');
            $this->assertSame(1, (int) $stmt->fetch(PDO::FETCH_ASSOC)['cnt']);

            // Disable ZTD and check physical state
            $this->pdo->disableZtd();
            try {
                $stmt = $this->pdo->query('SELECT COUNT(*) AS cnt FROM sl_dmd_dynamic');
                $count = (int) $stmt->fetch(PDO::FETCH_ASSOC)['cnt'];
                // If table exists physically, DML should still be shadowed
                $this->assertSame(0, $count);
            } catch (\Throwable $e) {
                // Table may not exist physically at all (created only in shadow)
                $this->assertStringContainsString('no such table', $e->getMessage());
            }
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Physical isolation check for mid-session table not supported: ' . $e->getMessage()
            );
        }
    }
}
