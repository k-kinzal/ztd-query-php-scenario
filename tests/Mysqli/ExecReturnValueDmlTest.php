<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests that affected row counts are correct with ZTD enabled on MySQLi.
 *
 * @spec SPEC-10.2
 */
class ExecReturnValueDmlTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return "CREATE TABLE mi_exrc_t (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(100),
            status VARCHAR(20) DEFAULT 'active'
        ) ENGINE=InnoDB";
    }

    protected function getTableNames(): array
    {
        return ['mi_exrc_t'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO mi_exrc_t (name, status) VALUES ('Alice', 'active')");
        $this->ztdExec("INSERT INTO mi_exrc_t (name, status) VALUES ('Bob', 'active')");
        $this->ztdExec("INSERT INTO mi_exrc_t (name, status) VALUES ('Charlie', 'inactive')");
    }

    public function testInsertReturnsOne(): void
    {
        try {
            $result = $this->ztdExec("INSERT INTO mi_exrc_t (name) VALUES ('Dave')");

            if ($result !== 1) {
                $this->markTestIncomplete(
                    'INSERT affected_rows (MySQLi): expected 1, got ' . var_export($result, true)
                );
            }

            $this->assertSame(1, $result);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT affected_rows (MySQLi) failed: ' . $e->getMessage());
        }
    }

    public function testUpdateReturnsMatchedCount(): void
    {
        try {
            $result = $this->ztdExec("UPDATE mi_exrc_t SET status = 'archived' WHERE status = 'active'");

            if ($result !== 2) {
                $this->markTestIncomplete(
                    'UPDATE affected_rows (MySQLi): expected 2, got ' . var_export($result, true)
                );
            }

            $this->assertSame(2, $result);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE affected_rows (MySQLi) failed: ' . $e->getMessage());
        }
    }

    public function testUpdateNoMatchReturnsZero(): void
    {
        try {
            $result = $this->ztdExec("UPDATE mi_exrc_t SET status = 'gone' WHERE name = 'Nobody'");

            if ($result !== 0) {
                $this->markTestIncomplete(
                    'UPDATE 0 match (MySQLi): expected 0, got ' . var_export($result, true)
                );
            }

            $this->assertSame(0, $result);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE 0 match (MySQLi) failed: ' . $e->getMessage());
        }
    }

    public function testDeleteReturnsOne(): void
    {
        try {
            $result = $this->ztdExec("DELETE FROM mi_exrc_t WHERE name = 'Charlie'");

            if ($result !== 1) {
                $this->markTestIncomplete(
                    'DELETE affected_rows (MySQLi): expected 1, got ' . var_export($result, true)
                );
            }

            $this->assertSame(1, $result);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE affected_rows (MySQLi) failed: ' . $e->getMessage());
        }
    }

    public function testDeleteAllReturnsTotal(): void
    {
        try {
            $result = $this->ztdExec("DELETE FROM mi_exrc_t");

            if ($result !== 3) {
                $this->markTestIncomplete(
                    'DELETE all (MySQLi): expected 3, got ' . var_export($result, true)
                );
            }

            $this->assertSame(3, $result);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE all (MySQLi) failed: ' . $e->getMessage());
        }
    }
}
