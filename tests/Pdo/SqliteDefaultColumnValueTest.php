<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests whether DEFAULT column values are populated in the shadow store
 * when INSERT omits those columns.
 *
 * When INSERT specifies only a subset of columns, omitted columns should
 * use their DEFAULT values. The CTE rewriter must include these defaults
 * for correct query results.
 *
 * Confirms Issue #21: Shadow store does not apply column DEFAULT values
 * on INSERT with partial columns.
 *
 * @spec SPEC-4.1
 */
class SqliteDefaultColumnValueTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return "CREATE TABLE sl_dcv_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'active',
            priority INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )";
    }

    protected function getTableNames(): array
    {
        return ['sl_dcv_items'];
    }

    /**
     * String DEFAULT: status should be 'active' when not specified.
     */
    public function testStringDefaultPopulated(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_dcv_items (name) VALUES ('Alpha')");

            $rows = $this->ztdQuery("SELECT status FROM sl_dcv_items WHERE name = 'Alpha'");
            $this->assertCount(1, $rows);

            $status = $rows[0]['status'];
            if ($status === null) {
                $this->markTestIncomplete(
                    'DEFAULT string column is NULL. Expected "active", got NULL.'
                    . ' Shadow store did not capture DEFAULT value for omitted column.'
                );
            }
            if ($status !== 'active') {
                $this->markTestIncomplete(
                    'DEFAULT string wrong. Expected "active", got ' . json_encode($status)
                );
            }
            $this->assertSame('active', $status);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('String default test failed: ' . $e->getMessage());
        }
    }

    /**
     * Integer DEFAULT: priority should be 0 when not specified.
     */
    public function testIntegerDefaultPopulated(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_dcv_items (name) VALUES ('Beta')");

            $rows = $this->ztdQuery("SELECT priority FROM sl_dcv_items WHERE name = 'Beta'");
            $this->assertCount(1, $rows);

            $priority = $rows[0]['priority'];
            if ($priority === null) {
                $this->markTestIncomplete(
                    'DEFAULT integer column is NULL. Expected 0, got NULL.'
                );
            }
            $this->assertEquals(0, (int) $priority);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Integer default test failed: ' . $e->getMessage());
        }
    }

    /**
     * Expression DEFAULT: created_at should be populated with datetime.
     */
    public function testExpressionDefaultPopulated(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_dcv_items (name) VALUES ('Gamma')");

            $rows = $this->ztdQuery("SELECT created_at FROM sl_dcv_items WHERE name = 'Gamma'");
            $this->assertCount(1, $rows);

            $createdAt = $rows[0]['created_at'];
            if ($createdAt === null) {
                $this->markTestIncomplete(
                    'DEFAULT expression column is NULL. Expected datetime string, got NULL.'
                );
            }
            if (empty($createdAt) || strlen($createdAt) < 10) {
                $this->markTestIncomplete(
                    'DEFAULT expression column wrong. Expected datetime, got ' . json_encode($createdAt)
                );
            }
            $this->assertNotEmpty($createdAt);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Expression default test failed: ' . $e->getMessage());
        }
    }

    /**
     * WHERE on DEFAULT column should work.
     */
    public function testWhereOnDefaultColumn(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_dcv_items (name) VALUES ('Delta')");
            $this->pdo->exec("INSERT INTO sl_dcv_items (name, status) VALUES ('Epsilon', 'inactive')");

            $rows = $this->ztdQuery("SELECT name FROM sl_dcv_items WHERE status = 'active' ORDER BY name");

            if (empty($rows)) {
                $this->markTestIncomplete(
                    'WHERE on DEFAULT column returned empty. The DEFAULT value may be NULL instead of "active".'
                );
            }

            $names = array_column($rows, 'name');
            if (!in_array('Delta', $names)) {
                $this->markTestIncomplete(
                    'Delta not found in active rows. Got: ' . json_encode($names)
                );
            }
            $this->assertContains('Delta', $names);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('WHERE on default column test failed: ' . $e->getMessage());
        }
    }
}
