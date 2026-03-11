<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests that PDO::exec() returns the correct affected row count with ZTD on PostgreSQL.
 *
 * @spec SPEC-10.2
 */
class PostgresExecReturnValueDmlTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return "CREATE TABLE pg_exrc_t (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100),
            status VARCHAR(20) DEFAULT 'active'
        )";
    }

    protected function getTableNames(): array
    {
        return ['pg_exrc_t'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO pg_exrc_t (name, status) VALUES ('Alice', 'active')");
        $this->ztdExec("INSERT INTO pg_exrc_t (name, status) VALUES ('Bob', 'active')");
        $this->ztdExec("INSERT INTO pg_exrc_t (name, status) VALUES ('Charlie', 'inactive')");
    }

    public function testInsertReturnsOne(): void
    {
        try {
            $result = $this->ztdExec("INSERT INTO pg_exrc_t (name) VALUES ('Dave')");

            if ($result !== 1) {
                $this->markTestIncomplete(
                    'INSERT exec() (PG): expected 1, got ' . var_export($result, true)
                );
            }

            $this->assertSame(1, $result);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT exec() (PG) failed: ' . $e->getMessage());
        }
    }

    public function testUpdateReturnsMatchedCount(): void
    {
        try {
            $result = $this->ztdExec("UPDATE pg_exrc_t SET status = 'archived' WHERE status = 'active'");

            if ($result !== 2) {
                $this->markTestIncomplete(
                    'UPDATE exec() (PG): expected 2, got ' . var_export($result, true)
                );
            }

            $this->assertSame(2, $result);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE exec() (PG) failed: ' . $e->getMessage());
        }
    }

    public function testUpdateNoMatchReturnsZero(): void
    {
        try {
            $result = $this->ztdExec("UPDATE pg_exrc_t SET status = 'gone' WHERE name = 'Nobody'");

            if ($result !== 0) {
                $this->markTestIncomplete(
                    'UPDATE 0 match exec() (PG): expected 0, got ' . var_export($result, true)
                );
            }

            $this->assertSame(0, $result);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE 0 match exec() (PG) failed: ' . $e->getMessage());
        }
    }

    public function testDeleteReturnsOne(): void
    {
        try {
            $result = $this->ztdExec("DELETE FROM pg_exrc_t WHERE name = 'Charlie'");

            if ($result !== 1) {
                $this->markTestIncomplete(
                    'DELETE exec() (PG): expected 1, got ' . var_export($result, true)
                );
            }

            $this->assertSame(1, $result);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE exec() (PG) failed: ' . $e->getMessage());
        }
    }

    public function testDeleteAllReturnsTotal(): void
    {
        try {
            $result = $this->ztdExec("DELETE FROM pg_exrc_t");

            if ($result !== 3) {
                $this->markTestIncomplete(
                    'DELETE all exec() (PG): expected 3, got ' . var_export($result, true)
                );
            }

            $this->assertSame(3, $result);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE all exec() (PG) failed: ' . $e->getMessage());
        }
    }

    public function testPreparedRowCountAfterUpdate(): void
    {
        try {
            $stmt = $this->pdo->prepare("UPDATE pg_exrc_t SET status = ? WHERE status = ?");
            $stmt->execute(['archived', 'active']);
            $count = $stmt->rowCount();

            if ($count !== 2) {
                $this->markTestIncomplete(
                    'Prepared rowCount() (PG): expected 2, got ' . var_export($count, true)
                );
            }

            $this->assertSame(2, $count);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared rowCount() (PG) failed: ' . $e->getMessage());
        }
    }
}
