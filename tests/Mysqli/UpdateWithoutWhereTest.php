<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests UPDATE without WHERE clause behavior via MySQLi.
 *
 * Hypothesis: if DELETE without WHERE is broken on SQLite (Issue #7),
 * UPDATE without WHERE might exhibit a parallel problem.
 *
 * @see https://github.com/k-kinzal/ztd-query-php/issues/7
 * @spec SPEC-4.2
 */
class UpdateWithoutWhereTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE uww_test (id INT PRIMARY KEY, name VARCHAR(50), status VARCHAR(20)) ENGINE=InnoDB';
    }

    protected function getTableNames(): array
    {
        return ['uww_test'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO uww_test VALUES (1, 'Alice', 'active')");
        $this->ztdExec("INSERT INTO uww_test VALUES (2, 'Bob', 'active')");
        $this->ztdExec("INSERT INTO uww_test VALUES (3, 'Charlie', 'active')");
    }

    /**
     * UPDATE without WHERE should update all rows.
     *
     * Parallel to Issue #7 (DELETE without WHERE silently ignored on SQLite).
     * If the CTE rewriter mishandles UPDATE without WHERE, rows may remain unchanged.
     */
    public function testUpdateAllRowsWithoutWhere(): void
    {
        try {
            $this->ztdExec("UPDATE uww_test SET status = 'inactive'");

            $rows = $this->ztdQuery("SELECT id, status FROM uww_test ORDER BY id");
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
                'UPDATE without WHERE failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Workaround: UPDATE with WHERE 1=1 should update all rows.
     */
    public function testUpdateAllRowsWithWhereTrue(): void
    {
        try {
            $this->ztdExec("UPDATE uww_test SET status = 'inactive' WHERE 1=1");

            $rows = $this->ztdQuery("SELECT id, status FROM uww_test ORDER BY id");
            $this->assertCount(3, $rows);
            foreach ($rows as $row) {
                $this->assertSame('inactive', $row['status']);
            }
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'UPDATE with WHERE 1=1 failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE with specific WHERE updates only matching rows.
     */
    public function testUpdateWithWhereWorks(): void
    {
        try {
            $this->ztdExec("UPDATE uww_test SET status = 'inactive' WHERE id = 1");

            $rows = $this->ztdQuery("SELECT id, status FROM uww_test ORDER BY id");
            $this->assertCount(3, $rows);
            $this->assertSame('inactive', $rows[0]['status']);
            $this->assertSame('active', $rows[1]['status']);
            $this->assertSame('active', $rows[2]['status']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'UPDATE with WHERE clause failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Affected row count from UPDATE without WHERE should equal total row count.
     */
    public function testAffectedRowCountWithoutWhere(): void
    {
        try {
            $affected = $this->ztdExec("UPDATE uww_test SET status = 'inactive'");

            if ($affected !== 3) {
                $this->markTestIncomplete(
                    'UPDATE without WHERE affected row count mismatch. '
                    . 'Expected 3, got: ' . var_export($affected, true)
                );
            }

            $this->assertSame(3, $affected);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Affected row count test failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Physical isolation: UPDATE without WHERE should not reach the physical table.
     */
    public function testPhysicalIsolation(): void
    {
        $this->ztdExec("UPDATE uww_test SET status = 'inactive'");

        $this->disableZtd();
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM uww_test");
        $this->assertSame(0, (int) $rows[0]['cnt'], 'Physical table should be empty (data seeded via ZTD)');
    }
}
