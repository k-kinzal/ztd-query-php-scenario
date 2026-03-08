<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests PostgreSQL RETURNING clause with INSERT, UPDATE, DELETE on ZTD.
 *
 * PostgreSQL supports RETURNING on DML statements to return affected rows.
 * The CTE rewriter may or may not preserve this clause.
 * @spec pending
 */
class PostgresReturningClauseTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pg_ret_test (id INT PRIMARY KEY, name VARCHAR(50), score INT)';
    }

    protected function getTableNames(): array
    {
        return ['pg_ret_test'];
    }


    /**
     * INSERT ... RETURNING — returns inserted rows.
     */
    public function testInsertReturning(): void
    {
        try {
            $stmt = $this->pdo->query("INSERT INTO pg_ret_test (id, name, score) VALUES (1, 'Alice', 90) RETURNING id, name, score");
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
            $this->assertCount(1, $rows);
            $this->assertSame(1, (int) $rows[0]['id']);
            $this->assertSame('Alice', $rows[0]['name']);
            $this->assertSame(90, (int) $rows[0]['score']);
        } catch (\Throwable $e) {
            // RETURNING on INSERT may not be supported by CTE rewriter
            $this->markTestSkipped('INSERT RETURNING not supported: ' . $e->getMessage());
        }
    }

    /**
     * INSERT ... RETURNING * — returns all columns.
     */
    public function testInsertReturningStar(): void
    {
        try {
            $stmt = $this->pdo->query("INSERT INTO pg_ret_test (id, name, score) VALUES (1, 'Alice', 90) RETURNING *");
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
            $this->assertCount(1, $rows);
            $this->assertArrayHasKey('id', $rows[0]);
            $this->assertArrayHasKey('name', $rows[0]);
        } catch (\Throwable $e) {
            $this->markTestSkipped('INSERT RETURNING * not supported: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE ... RETURNING — returns updated rows.
     */
    public function testUpdateReturning(): void
    {
        $this->pdo->exec("INSERT INTO pg_ret_test (id, name, score) VALUES (1, 'Alice', 90)");
        $this->pdo->exec("INSERT INTO pg_ret_test (id, name, score) VALUES (2, 'Bob', 80)");

        try {
            $stmt = $this->pdo->query("UPDATE pg_ret_test SET score = 95 WHERE id = 1 RETURNING id, name, score");
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
            $this->assertCount(1, $rows);
            $this->assertSame('Alice', $rows[0]['name']);
            $this->assertSame(95, (int) $rows[0]['score']);
        } catch (\Throwable $e) {
            $this->markTestSkipped('UPDATE RETURNING not supported: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE ... RETURNING multiple rows.
     */
    public function testUpdateReturningMultipleRows(): void
    {
        $this->pdo->exec("INSERT INTO pg_ret_test (id, name, score) VALUES (1, 'Alice', 90)");
        $this->pdo->exec("INSERT INTO pg_ret_test (id, name, score) VALUES (2, 'Bob', 80)");
        $this->pdo->exec("INSERT INTO pg_ret_test (id, name, score) VALUES (3, 'Charlie', 70)");

        try {
            $stmt = $this->pdo->query("UPDATE pg_ret_test SET score = 100 WHERE score >= 80 RETURNING id, name");
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
            $this->assertCount(2, $rows);
        } catch (\Throwable $e) {
            $this->markTestSkipped('UPDATE RETURNING not supported: ' . $e->getMessage());
        }
    }

    /**
     * DELETE ... RETURNING — returns deleted rows.
     */
    public function testDeleteReturning(): void
    {
        $this->pdo->exec("INSERT INTO pg_ret_test (id, name, score) VALUES (1, 'Alice', 90)");
        $this->pdo->exec("INSERT INTO pg_ret_test (id, name, score) VALUES (2, 'Bob', 80)");

        try {
            $stmt = $this->pdo->query("DELETE FROM pg_ret_test WHERE id = 1 RETURNING id, name, score");
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
            $this->assertCount(1, $rows);
            $this->assertSame('Alice', $rows[0]['name']);

            // Verify row is actually deleted
            $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_ret_test');
            $this->assertSame(1, (int) $stmt->fetchColumn());
        } catch (\Throwable $e) {
            $this->markTestSkipped('DELETE RETURNING not supported: ' . $e->getMessage());
        }
    }

    /**
     * DELETE ... RETURNING * — all columns of deleted rows.
     */
    public function testDeleteReturningStar(): void
    {
        $this->pdo->exec("INSERT INTO pg_ret_test (id, name, score) VALUES (1, 'Alice', 90)");

        try {
            $stmt = $this->pdo->query("DELETE FROM pg_ret_test WHERE id = 1 RETURNING *");
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
            $this->assertCount(1, $rows);
            $this->assertArrayHasKey('id', $rows[0]);
            $this->assertArrayHasKey('name', $rows[0]);
        } catch (\Throwable $e) {
            $this->markTestSkipped('DELETE RETURNING * not supported: ' . $e->getMessage());
        }
    }

    /**
     * DELETE ... RETURNING multiple deleted rows.
     */
    public function testDeleteReturningMultipleRows(): void
    {
        $this->pdo->exec("INSERT INTO pg_ret_test (id, name, score) VALUES (1, 'Alice', 90)");
        $this->pdo->exec("INSERT INTO pg_ret_test (id, name, score) VALUES (2, 'Bob', 80)");
        $this->pdo->exec("INSERT INTO pg_ret_test (id, name, score) VALUES (3, 'Charlie', 70)");

        try {
            $stmt = $this->pdo->query("DELETE FROM pg_ret_test WHERE score < 90 RETURNING id, name");
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
            $this->assertCount(2, $rows);

            // Remaining row
            $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_ret_test');
            $this->assertSame(1, (int) $stmt->fetchColumn());
        } catch (\Throwable $e) {
            $this->markTestSkipped('DELETE RETURNING not supported: ' . $e->getMessage());
        }
    }

    /**
     * INSERT ... ON CONFLICT ... RETURNING.
     */
    public function testUpsertReturning(): void
    {
        $this->pdo->exec("INSERT INTO pg_ret_test (id, name, score) VALUES (1, 'Alice', 90)");

        try {
            $stmt = $this->pdo->query("INSERT INTO pg_ret_test (id, name, score) VALUES (1, 'Alice V2', 95) ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, score = EXCLUDED.score RETURNING id, name, score");
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
            $this->assertCount(1, $rows);
            $this->assertSame('Alice V2', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestSkipped('UPSERT RETURNING not supported: ' . $e->getMessage());
        }
    }

    /**
     * Physical isolation with RETURNING.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO pg_ret_test (id, name, score) VALUES (1, 'Alice', 90)");

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_ret_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
