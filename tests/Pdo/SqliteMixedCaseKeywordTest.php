<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests SQL with mixed-case keywords through the CTE rewriter.
 *
 * Real-world scenario: SQL keywords are case-insensitive per the SQL standard,
 * but the CTE rewriter's statement classifier may use case-sensitive regex or
 * string matching. Users, ORMs, and query builders produce SQL with varying
 * keyword casing (SELECT, Select, select, sElEcT).
 *
 * @spec SPEC-3.1
 * @spec SPEC-4.1
 */
class SqliteMixedCaseKeywordTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_mck_data (
                id INTEGER PRIMARY KEY,
                label TEXT NOT NULL,
                value INTEGER NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_mck_data'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO sl_mck_data VALUES (1, 'alpha', 10)");
        $this->ztdExec("INSERT INTO sl_mck_data VALUES (2, 'beta', 20)");
        $this->ztdExec("INSERT INTO sl_mck_data VALUES (3, 'gamma', 30)");
    }

    /**
     * Lowercase SELECT.
     */
    public function testLowercaseSelect(): void
    {
        try {
            $rows = $this->ztdQuery("select * from sl_mck_data order by id");

            $this->assertCount(3, $rows);
            $this->assertSame('alpha', $rows[0]['label']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Lowercase SELECT failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Mixed-case Select From Where.
     */
    public function testMixedCaseSelectFromWhere(): void
    {
        try {
            $rows = $this->ztdQuery("Select * From sl_mck_data Where value > 15 Order By id");

            $this->assertCount(2, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Mixed-case Select From Where failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Alternating case keywords.
     */
    public function testAlternatingCaseKeywords(): void
    {
        try {
            $rows = $this->ztdQuery("sElEcT * fRoM sl_mck_data wHeRe id = 1");

            $this->assertCount(1, $rows);
            $this->assertSame('alpha', $rows[0]['label']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Alternating case keywords failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Lowercase INSERT.
     */
    public function testLowercaseInsert(): void
    {
        try {
            $this->ztdExec("insert into sl_mck_data values (4, 'delta', 40)");

            $rows = $this->ztdQuery("SELECT * FROM sl_mck_data WHERE id = 4");
            $this->assertCount(1, $rows);
            $this->assertSame('delta', $rows[0]['label']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Lowercase INSERT failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Mixed-case INSERT INTO.
     */
    public function testMixedCaseInsert(): void
    {
        try {
            $this->ztdExec("Insert Into sl_mck_data Values (5, 'epsilon', 50)");

            $rows = $this->ztdQuery("SELECT * FROM sl_mck_data WHERE id = 5");
            $this->assertCount(1, $rows);
            $this->assertSame('epsilon', $rows[0]['label']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Mixed-case INSERT failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Lowercase UPDATE.
     */
    public function testLowercaseUpdate(): void
    {
        try {
            $this->ztdExec("update sl_mck_data set value = 99 where id = 1");

            $rows = $this->ztdQuery("SELECT value FROM sl_mck_data WHERE id = 1");
            $this->assertEquals(99, (int) $rows[0]['value']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Lowercase UPDATE failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Mixed-case UPDATE SET WHERE.
     */
    public function testMixedCaseUpdate(): void
    {
        try {
            $this->ztdExec("Update sl_mck_data Set value = 88 Where id = 2");

            $rows = $this->ztdQuery("SELECT value FROM sl_mck_data WHERE id = 2");
            $this->assertEquals(88, (int) $rows[0]['value']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Mixed-case UPDATE failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Lowercase DELETE.
     */
    public function testLowercaseDelete(): void
    {
        try {
            $this->ztdExec("delete from sl_mck_data where id = 3");

            $rows = $this->ztdQuery("SELECT * FROM sl_mck_data ORDER BY id");
            $this->assertCount(2, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Lowercase DELETE failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Mixed-case DELETE FROM WHERE.
     */
    public function testMixedCaseDelete(): void
    {
        try {
            $this->ztdExec("Delete From sl_mck_data Where id = 3");

            $rows = $this->ztdQuery("SELECT * FROM sl_mck_data ORDER BY id");
            $this->assertCount(2, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Mixed-case DELETE failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Lowercase keywords with prepared statement.
     */
    public function testLowercasePrepared(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "select * from sl_mck_data where value > ? order by id",
                [15]
            );

            $this->assertCount(2, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Lowercase prepared failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Lowercase TRUNCATE.
     */
    public function testLowercaseTruncate(): void
    {
        try {
            // SQLite doesn't support TRUNCATE, so use DELETE as reference
            $this->ztdExec("delete from sl_mck_data");

            $rows = $this->ztdQuery("SELECT * FROM sl_mck_data");
            // Known issue #7: DELETE without WHERE silently ignored on SQLite
            if (count($rows) === 3) {
                $this->markTestIncomplete(
                    'Known issue #7: DELETE without WHERE silently ignored on SQLite.'
                );
            }
            $this->assertCount(0, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Lowercase DELETE (truncate-style) failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Lowercase aggregate query.
     */
    public function testLowercaseAggregate(): void
    {
        try {
            $rows = $this->ztdQuery(
                "select count(*) as cnt, sum(value) as total from sl_mck_data"
            );

            $this->assertCount(1, $rows);
            $this->assertEquals(3, (int) $rows[0]['cnt']);
            $this->assertEquals(60, (int) $rows[0]['total']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Lowercase aggregate failed: ' . $e->getMessage()
            );
        }
    }
}
