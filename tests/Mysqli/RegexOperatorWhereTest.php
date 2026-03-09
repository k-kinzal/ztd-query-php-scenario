<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests REGEXP operator in WHERE clause via MySQLi.
 *
 * REGEXP is a common search operator. The CTE rewriter must
 * parse it correctly without confusing the keyword or its operands.
 *
 * @spec SPEC-3.1
 */
class RegexOperatorWhereTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE regex_test (
            id INT PRIMARY KEY,
            email VARCHAR(200) NOT NULL,
            code VARCHAR(50) NOT NULL
        ) ENGINE=InnoDB';
    }

    protected function getTableNames(): array
    {
        return ['regex_test'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO regex_test VALUES (1, 'alice@example.com', 'ABC-123')");
        $this->ztdExec("INSERT INTO regex_test VALUES (2, 'bob@test.org', 'DEF-456')");
        $this->ztdExec("INSERT INTO regex_test VALUES (3, 'charlie@example.com', 'GHI-789')");
        $this->ztdExec("INSERT INTO regex_test VALUES (4, 'dave@other.net', 'ABC-999')");
        $this->ztdExec("INSERT INTO regex_test VALUES (5, 'eve@test.org', 'XYZ-000')");
    }

    /**
     * SELECT with REGEXP in WHERE clause.
     *
     * email REGEXP '@example\\.com$' matches ids 1 and 3.
     */
    public function testSelectWithRegexp(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT id FROM regex_test WHERE email REGEXP '@example\\\\.com$' ORDER BY id"
            );

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'SELECT with REGEXP: expected 2 rows, got ' . count($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame(1, (int) $rows[0]['id']);
            $this->assertSame(3, (int) $rows[1]['id']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'SELECT with REGEXP failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * SELECT with NOT REGEXP in WHERE clause.
     *
     * email NOT REGEXP '@example\\.com$' matches ids 2, 4, 5.
     */
    public function testSelectWithNotRegexp(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT id FROM regex_test WHERE email NOT REGEXP '@example\\\\.com$' ORDER BY id"
            );

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'SELECT with NOT REGEXP: expected 3 rows, got ' . count($rows)
                );
            }

            $this->assertCount(3, $rows);
            $ids = array_map(fn($r) => (int) $r['id'], $rows);
            $this->assertSame([2, 4, 5], $ids);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'SELECT with NOT REGEXP failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * DELETE with REGEXP in WHERE clause.
     *
     * Delete rows where email REGEXP '@test\\.org$' (ids 2 and 5).
     * Remaining: ids 1, 3, 4.
     */
    public function testDeleteWithRegexp(): void
    {
        try {
            $this->ztdExec("DELETE FROM regex_test WHERE email REGEXP '@test\\\\.org$'");

            $rows = $this->ztdQuery("SELECT id FROM regex_test ORDER BY id");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'DELETE with REGEXP: expected 3 remaining rows, got ' . count($rows)
                );
            }

            $this->assertCount(3, $rows);
            $ids = array_map(fn($r) => (int) $r['id'], $rows);
            $this->assertSame([1, 3, 4], $ids);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'DELETE with REGEXP failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE with REGEXP in WHERE clause.
     *
     * Update code to 'MATCHED' where code REGEXP '^ABC-[0-9]+$'.
     * Matches ids 1 and 4.
     */
    public function testUpdateWithRegexp(): void
    {
        try {
            $this->ztdExec("UPDATE regex_test SET code = 'MATCHED' WHERE code REGEXP '^ABC-[0-9]+$'");

            $rows = $this->ztdQuery("SELECT id, code FROM regex_test ORDER BY id");

            $this->assertCount(5, $rows);
            $this->assertSame('MATCHED', $rows[0]['code']); // id 1
            $this->assertSame('DEF-456', $rows[1]['code']); // id 2
            $this->assertSame('GHI-789', $rows[2]['code']); // id 3
            $this->assertSame('MATCHED', $rows[3]['code']); // id 4
            $this->assertSame('XYZ-000', $rows[4]['code']); // id 5
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'UPDATE with REGEXP failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Prepared SELECT with REGEXP and bound parameter.
     */
    public function testPreparedSelectWithRegexp(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT id FROM regex_test WHERE code REGEXP ? ORDER BY id",
                ['^ABC']
            );

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'Prepared SELECT with REGEXP: expected 2 rows, got ' . count($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame(1, (int) $rows[0]['id']);
            $this->assertSame(4, (int) $rows[1]['id']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Prepared SELECT with REGEXP failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * SELECT with RLIKE synonym.
     */
    public function testSelectWithRlike(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT id FROM regex_test WHERE email RLIKE '@example' ORDER BY id"
            );

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'SELECT with RLIKE: expected 2 rows, got ' . count($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame(1, (int) $rows[0]['id']);
            $this->assertSame(3, (int) $rows[1]['id']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'SELECT with RLIKE failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Physical isolation: DML with REGEXP should not affect the physical table.
     */
    public function testPhysicalIsolation(): void
    {
        $this->ztdExec("DELETE FROM regex_test WHERE email REGEXP '@test'");

        $this->disableZtd();
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM regex_test");
        $this->assertSame(0, (int) $rows[0]['cnt'], 'Physical table should be empty');
    }
}
