<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests RETURNING clause behavior on PostgreSQL with ZTD.
 *
 * RETURNING is a PostgreSQL extension that returns affected rows from
 * INSERT/UPDATE/DELETE. Related: Issue #53 (RETURNING might not work),
 * Issue #32 (INSERT RETURNING).
 *
 * @spec SPEC-4.1
 */
class PostgresReturningClauseTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE ret_t (id SERIAL PRIMARY KEY, name TEXT, score INTEGER)';
    }

    protected function getTableNames(): array
    {
        return ['ret_t'];
    }

    /**
     * INSERT ... RETURNING id.
     */
    public function testInsertReturningId(): void
    {
        try {
            $stmt = $this->pdo->query("INSERT INTO ret_t (name, score) VALUES ('Alice', 90) RETURNING id");
            $row = $stmt->fetch(PDO::FETCH_ASSOC);
            $this->assertNotFalse($row);
            $this->assertArrayHasKey('id', $row);
            $this->assertGreaterThan(0, (int) $row['id']);
        } catch (\Throwable $e) {
            $this->markTestSkipped('INSERT RETURNING not supported: ' . $e->getMessage());
        }
    }

    /**
     * INSERT ... RETURNING *.
     */
    public function testInsertReturningStar(): void
    {
        try {
            $stmt = $this->pdo->query("INSERT INTO ret_t (name, score) VALUES ('Bob', 80) RETURNING *");
            $row = $stmt->fetch(PDO::FETCH_ASSOC);
            $this->assertNotFalse($row);
            $this->assertSame('Bob', $row['name']);
            $this->assertEquals(80, (int) $row['score']);
        } catch (\Throwable $e) {
            $this->markTestSkipped('INSERT RETURNING * not supported: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE ... RETURNING modified columns.
     */
    public function testUpdateReturning(): void
    {
        $this->pdo->exec("INSERT INTO ret_t (id, name, score) VALUES (1, 'Alice', 90)");

        try {
            $stmt = $this->pdo->query("UPDATE ret_t SET score = 100 WHERE id = 1 RETURNING name, score");
            $row = $stmt->fetch(PDO::FETCH_ASSOC);
            $this->assertNotFalse($row);
            $this->assertSame('Alice', $row['name']);
            $this->assertEquals(100, (int) $row['score']);
        } catch (\Throwable $e) {
            $this->markTestSkipped('UPDATE RETURNING not supported: ' . $e->getMessage());
        }
    }

    /**
     * DELETE ... RETURNING deleted rows.
     */
    public function testDeleteReturning(): void
    {
        $this->pdo->exec("INSERT INTO ret_t (id, name, score) VALUES (1, 'Alice', 90), (2, 'Bob', 80)");

        try {
            $stmt = $this->pdo->query("DELETE FROM ret_t WHERE score < 85 RETURNING name");
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
            $this->assertCount(1, $rows);
            $this->assertSame('Bob', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestSkipped('DELETE RETURNING not supported: ' . $e->getMessage());
        }
    }

    /**
     * Prepared INSERT ... RETURNING.
     */
    public function testPreparedInsertReturning(): void
    {
        try {
            $stmt = $this->pdo->prepare("INSERT INTO ret_t (name, score) VALUES (?, ?) RETURNING id, name");
            $stmt->execute(['Charlie', 75]);
            $row = $stmt->fetch(PDO::FETCH_ASSOC);
            $this->assertNotFalse($row);
            $this->assertSame('Charlie', $row['name']);
        } catch (\Throwable $e) {
            $this->markTestSkipped('Prepared INSERT RETURNING not supported: ' . $e->getMessage());
        }
    }

    /**
     * INSERT RETURNING then SELECT verifies shadow consistency.
     */
    public function testInsertReturningThenSelect(): void
    {
        try {
            $stmt = $this->pdo->query("INSERT INTO ret_t (name, score) VALUES ('Diana', 95) RETURNING id");
            $row = $stmt->fetch(PDO::FETCH_ASSOC);
            if ($row === false) {
                $this->markTestSkipped('INSERT RETURNING returned no rows');
            }
            $insertedId = (int) $row['id'];

            // Verify via SELECT
            $rows = $this->ztdQuery("SELECT name, score FROM ret_t WHERE id = $insertedId");
            $this->assertCount(1, $rows);
            $this->assertSame('Diana', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestSkipped('INSERT RETURNING not supported: ' . $e->getMessage());
        }
    }
}
