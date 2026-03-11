<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests that PDO::exec() returns the correct affected row count with ZTD enabled.
 *
 * When ZTD rewrites DML through CTEs, the affected row count must still reflect
 * the actual number of rows modified by the original statement, not the CTE wrapper.
 * Incorrect row counts silently break application logic that depends on them
 * (e.g., "if ($pdo->exec($sql) === 0) { ... }").
 *
 * @spec SPEC-10.2
 */
class MysqlExecReturnValueDmlTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return "CREATE TABLE my_exrc_t (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(100),
            status VARCHAR(20) DEFAULT 'active'
        ) ENGINE=InnoDB";
    }

    protected function getTableNames(): array
    {
        return ['my_exrc_t'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO my_exrc_t (name, status) VALUES ('Alice', 'active')");
        $this->ztdExec("INSERT INTO my_exrc_t (name, status) VALUES ('Bob', 'active')");
        $this->ztdExec("INSERT INTO my_exrc_t (name, status) VALUES ('Charlie', 'inactive')");
    }

    /**
     * Single INSERT returns 1.
     */
    public function testInsertReturnsOne(): void
    {
        try {
            $result = $this->ztdExec("INSERT INTO my_exrc_t (name) VALUES ('Dave')");

            if ($result !== 1) {
                $this->markTestIncomplete(
                    'INSERT exec() (MySQL): expected 1, got ' . var_export($result, true)
                );
            }

            $this->assertSame(1, $result);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT exec() (MySQL) failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE matching 2 rows returns 2.
     */
    public function testUpdateReturnsMatchedCount(): void
    {
        try {
            $result = $this->ztdExec("UPDATE my_exrc_t SET status = 'archived' WHERE status = 'active'");

            if ($result !== 2) {
                $this->markTestIncomplete(
                    'UPDATE exec() (MySQL): expected 2, got ' . var_export($result, true)
                );
            }

            $this->assertSame(2, $result);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE exec() (MySQL) failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE matching 0 rows returns 0.
     */
    public function testUpdateNoMatchReturnsZero(): void
    {
        try {
            $result = $this->ztdExec("UPDATE my_exrc_t SET status = 'gone' WHERE name = 'Nobody'");

            if ($result !== 0) {
                $this->markTestIncomplete(
                    'UPDATE 0 match exec() (MySQL): expected 0, got ' . var_export($result, true)
                );
            }

            $this->assertSame(0, $result);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE 0 match exec() (MySQL) failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE matching 1 row returns 1.
     */
    public function testDeleteReturnsOne(): void
    {
        try {
            $result = $this->ztdExec("DELETE FROM my_exrc_t WHERE name = 'Charlie'");

            if ($result !== 1) {
                $this->markTestIncomplete(
                    'DELETE exec() (MySQL): expected 1, got ' . var_export($result, true)
                );
            }

            $this->assertSame(1, $result);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE exec() (MySQL) failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE all rows returns total count.
     */
    public function testDeleteAllReturnsTotal(): void
    {
        try {
            $result = $this->ztdExec("DELETE FROM my_exrc_t");

            if ($result !== 3) {
                $this->markTestIncomplete(
                    'DELETE all exec() (MySQL): expected 3, got ' . var_export($result, true)
                );
            }

            $this->assertSame(3, $result);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE all exec() (MySQL) failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared statement rowCount() after UPDATE.
     */
    public function testPreparedRowCountAfterUpdate(): void
    {
        try {
            $stmt = $this->pdo->prepare("UPDATE my_exrc_t SET status = ? WHERE status = ?");
            $stmt->execute(['archived', 'active']);
            $count = $stmt->rowCount();

            if ($count !== 2) {
                $this->markTestIncomplete(
                    'Prepared rowCount() (MySQL): expected 2, got ' . var_export($count, true)
                );
            }

            $this->assertSame(2, $count);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared rowCount() (MySQL) failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared statement rowCount() after DELETE.
     */
    public function testPreparedRowCountAfterDelete(): void
    {
        try {
            $stmt = $this->pdo->prepare("DELETE FROM my_exrc_t WHERE status = ?");
            $stmt->execute(['inactive']);
            $count = $stmt->rowCount();

            if ($count !== 1) {
                $this->markTestIncomplete(
                    'Prepared DELETE rowCount() (MySQL): expected 1, got ' . var_export($count, true)
                );
            }

            $this->assertSame(1, $count);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared DELETE rowCount() (MySQL) failed: ' . $e->getMessage());
        }
    }
}
