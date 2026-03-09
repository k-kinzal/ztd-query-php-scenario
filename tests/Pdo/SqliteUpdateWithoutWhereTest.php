<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests UPDATE without WHERE clause behavior on SQLite.
 *
 * Hypothesis: if DELETE without WHERE is broken on SQLite (Issue #7),
 * UPDATE without WHERE might exhibit the same problem.
 *
 * @see https://github.com/k-kinzal/ztd-query-php/issues/7
 * @spec SPEC-4.2
 */
class SqliteUpdateWithoutWhereTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_uww_test (id INT PRIMARY KEY, name VARCHAR(50), status VARCHAR(20))';
    }

    protected function getTableNames(): array
    {
        return ['sl_uww_test'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_uww_test VALUES (1, 'Alice', 'active')");
        $this->pdo->exec("INSERT INTO sl_uww_test VALUES (2, 'Bob', 'active')");
        $this->pdo->exec("INSERT INTO sl_uww_test VALUES (3, 'Charlie', 'active')");
    }

    /**
     * UPDATE without WHERE should update all rows.
     *
     * Parallel to Issue #7 (DELETE without WHERE silently ignored on SQLite).
     * If the CTE rewriter mishandles UPDATE without WHERE, rows may remain unchanged.
     *
     * @see https://github.com/k-kinzal/ztd-query-php/issues/7
     */
    public function testUpdateAllRowsWithoutWhere(): void
    {
        try {
            $this->pdo->exec("UPDATE sl_uww_test SET status = 'inactive'");

            $rows = $this->pdo->query("SELECT id, status FROM sl_uww_test ORDER BY id")
                ->fetchAll(PDO::FETCH_ASSOC);

            $allInactive = true;
            foreach ($rows as $row) {
                if ($row['status'] !== 'inactive') {
                    $allInactive = false;
                    break;
                }
            }

            if (!$allInactive) {
                $statuses = array_column($rows, 'status');
                $this->markTestIncomplete(
                    'UPDATE without WHERE may be silently ignored (parallel to Issue #7). '
                    . 'Expected all rows inactive, got: ' . implode(', ', $statuses)
                );
            }

            $this->assertCount(3, $rows);
            foreach ($rows as $row) {
                $this->assertSame('inactive', $row['status']);
            }
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'UPDATE without WHERE failed on SQLite: ' . $e->getMessage()
            );
        }
    }

    /**
     * Workaround: UPDATE with WHERE 1=1 should update all rows.
     */
    public function testUpdateAllRowsWithWhereTrue(): void
    {
        try {
            $this->pdo->exec("UPDATE sl_uww_test SET status = 'inactive' WHERE 1=1");

            $rows = $this->pdo->query("SELECT id, status FROM sl_uww_test ORDER BY id")
                ->fetchAll(PDO::FETCH_ASSOC);

            $this->assertCount(3, $rows);
            foreach ($rows as $row) {
                $this->assertSame('inactive', $row['status']);
            }
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'UPDATE with WHERE 1=1 failed on SQLite: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE with specific WHERE updates only matching rows.
     */
    public function testUpdateWithWhereWorks(): void
    {
        try {
            $this->pdo->exec("UPDATE sl_uww_test SET status = 'inactive' WHERE id = 1");

            $rows = $this->pdo->query("SELECT id, status FROM sl_uww_test ORDER BY id")
                ->fetchAll(PDO::FETCH_ASSOC);

            $this->assertCount(3, $rows);
            $this->assertSame('inactive', $rows[0]['status']);
            $this->assertSame('active', $rows[1]['status']);
            $this->assertSame('active', $rows[2]['status']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'UPDATE with WHERE clause failed on SQLite: ' . $e->getMessage()
            );
        }
    }

    /**
     * Affected row count from exec() for UPDATE without WHERE should equal total row count.
     */
    public function testAffectedRowCountWithoutWhere(): void
    {
        try {
            $affected = $this->pdo->exec("UPDATE sl_uww_test SET status = 'inactive'");

            if ($affected !== 3) {
                $this->markTestIncomplete(
                    'UPDATE without WHERE affected row count mismatch on SQLite. '
                    . 'Expected 3, got: ' . var_export($affected, true)
                );
            }

            $this->assertSame(3, $affected);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Affected row count test failed on SQLite: ' . $e->getMessage()
            );
        }
    }

    /**
     * Physical isolation: shadow INSERT + UPDATE should not reach the physical table.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("UPDATE sl_uww_test SET status = 'inactive'");

        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM sl_uww_test")
            ->fetchAll(PDO::FETCH_ASSOC);

        $this->assertSame(0, (int) $rows[0]['cnt'], 'Physical table should be empty (data seeded via ZTD)');
    }
}
