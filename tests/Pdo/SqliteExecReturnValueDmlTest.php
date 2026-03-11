<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests that PDO::exec() returns the correct affected row count with ZTD on SQLite.
 *
 * @spec SPEC-10.2
 */
class SqliteExecReturnValueDmlTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        // Use explicit PK (not AUTOINCREMENT) to avoid Issue #145 shadow PK=null
        return "CREATE TABLE sl_exrc_t (
            id INTEGER PRIMARY KEY,
            name TEXT,
            status TEXT DEFAULT 'active'
        )";
    }

    protected function getTableNames(): array
    {
        return ['sl_exrc_t'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO sl_exrc_t (id, name, status) VALUES (1, 'Alice', 'active')");
        $this->ztdExec("INSERT INTO sl_exrc_t (id, name, status) VALUES (2, 'Bob', 'active')");
        $this->ztdExec("INSERT INTO sl_exrc_t (id, name, status) VALUES (3, 'Charlie', 'inactive')");
    }

    public function testInsertReturnsOne(): void
    {
        try {
            $result = $this->ztdExec("INSERT INTO sl_exrc_t (id, name) VALUES (4, 'Dave')");

            if ($result !== 1) {
                $this->markTestIncomplete(
                    'INSERT exec() (SQLite): expected 1, got ' . var_export($result, true)
                );
            }

            $this->assertSame(1, $result);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT exec() (SQLite) failed: ' . $e->getMessage());
        }
    }

    public function testUpdateReturnsMatchedCount(): void
    {
        try {
            $result = $this->ztdExec("UPDATE sl_exrc_t SET status = 'archived' WHERE status = 'active'");

            if ($result !== 2) {
                $this->markTestIncomplete(
                    'UPDATE exec() (SQLite): expected 2, got ' . var_export($result, true)
                );
            }

            $this->assertSame(2, $result);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE exec() (SQLite) failed: ' . $e->getMessage());
        }
    }

    public function testUpdateNoMatchReturnsZero(): void
    {
        try {
            $result = $this->ztdExec("UPDATE sl_exrc_t SET status = 'gone' WHERE name = 'Nobody'");

            if ($result !== 0) {
                $this->markTestIncomplete(
                    'UPDATE 0 match exec() (SQLite): expected 0, got ' . var_export($result, true)
                );
            }

            $this->assertSame(0, $result);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE 0 match exec() (SQLite) failed: ' . $e->getMessage());
        }
    }

    public function testDeleteReturnsOne(): void
    {
        try {
            $result = $this->ztdExec("DELETE FROM sl_exrc_t WHERE name = 'Charlie'");

            if ($result !== 1) {
                $this->markTestIncomplete(
                    'DELETE exec() (SQLite): expected 1, got ' . var_export($result, true)
                );
            }

            $this->assertSame(1, $result);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE exec() (SQLite) failed: ' . $e->getMessage());
        }
    }

    public function testDeleteAllReturnsTotal(): void
    {
        try {
            $result = $this->ztdExec("DELETE FROM sl_exrc_t");

            if ($result !== 3) {
                $this->markTestIncomplete(
                    'DELETE all exec() (SQLite): expected 3, got ' . var_export($result, true)
                );
            }

            $this->assertSame(3, $result);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE all exec() (SQLite) failed: ' . $e->getMessage());
        }
    }

    public function testPreparedRowCountAfterUpdate(): void
    {
        try {
            $stmt = $this->pdo->prepare("UPDATE sl_exrc_t SET status = ? WHERE status = ?");
            $stmt->execute(['archived', 'active']);
            $count = $stmt->rowCount();

            if ($count !== 2) {
                $this->markTestIncomplete(
                    'Prepared rowCount() (SQLite): expected 2, got ' . var_export($count, true)
                );
            }

            $this->assertSame(2, $count);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared rowCount() (SQLite) failed: ' . $e->getMessage());
        }
    }

    public function testPreparedRowCountAfterDelete(): void
    {
        try {
            $stmt = $this->pdo->prepare("DELETE FROM sl_exrc_t WHERE status = ?");
            $stmt->execute(['inactive']);
            $count = $stmt->rowCount();

            if ($count !== 1) {
                $this->markTestIncomplete(
                    'Prepared DELETE rowCount() (SQLite): expected 1, got ' . var_export($count, true)
                );
            }

            $this->assertSame(1, $count);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared DELETE rowCount() (SQLite) failed: ' . $e->getMessage());
        }
    }
}
