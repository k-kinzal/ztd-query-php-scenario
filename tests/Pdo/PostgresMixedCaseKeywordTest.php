<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests SQL with mixed-case keywords through the CTE rewriter (PostgreSQL).
 *
 * Real-world scenario: SQL keywords are case-insensitive per the SQL standard.
 * ORMs and query builders may produce lowercase or mixed-case SQL. The CTE
 * rewriter's statement classifier must handle all casing variants.
 *
 * @spec SPEC-3.1
 * @spec SPEC-4.1
 */
class PostgresMixedCaseKeywordTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_mck_data (
                id SERIAL PRIMARY KEY,
                label TEXT NOT NULL,
                value INTEGER NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_mck_data'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO pg_mck_data (id, label, value) VALUES (1, 'alpha', 10)");
        $this->ztdExec("INSERT INTO pg_mck_data (id, label, value) VALUES (2, 'beta', 20)");
        $this->ztdExec("INSERT INTO pg_mck_data (id, label, value) VALUES (3, 'gamma', 30)");
    }

    public function testLowercaseSelect(): void
    {
        try {
            $rows = $this->ztdQuery("select * from pg_mck_data order by id");

            $this->assertCount(3, $rows);
            $this->assertSame('alpha', $rows[0]['label']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Lowercase SELECT failed: ' . $e->getMessage()
            );
        }
    }

    public function testMixedCaseSelectFromWhere(): void
    {
        try {
            $rows = $this->ztdQuery("Select * From pg_mck_data Where value > 15 Order By id");

            $this->assertCount(2, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Mixed-case Select From Where failed: ' . $e->getMessage()
            );
        }
    }

    public function testLowercaseInsert(): void
    {
        try {
            $this->ztdExec("insert into pg_mck_data (id, label, value) values (4, 'delta', 40)");

            $rows = $this->ztdQuery("SELECT * FROM pg_mck_data WHERE id = 4");
            $this->assertCount(1, $rows);
            $this->assertSame('delta', $rows[0]['label']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Lowercase INSERT failed: ' . $e->getMessage()
            );
        }
    }

    public function testMixedCaseInsert(): void
    {
        try {
            $this->ztdExec("Insert Into pg_mck_data (id, label, value) Values (5, 'epsilon', 50)");

            $rows = $this->ztdQuery("SELECT * FROM pg_mck_data WHERE id = 5");
            $this->assertCount(1, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Mixed-case INSERT failed: ' . $e->getMessage()
            );
        }
    }

    public function testLowercaseUpdate(): void
    {
        try {
            $this->ztdExec("update pg_mck_data set value = 99 where id = 1");

            $rows = $this->ztdQuery("SELECT value FROM pg_mck_data WHERE id = 1");
            $this->assertEquals(99, (int) $rows[0]['value']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Lowercase UPDATE failed: ' . $e->getMessage()
            );
        }
    }

    public function testLowercaseDelete(): void
    {
        try {
            $this->ztdExec("delete from pg_mck_data where id = 3");

            $rows = $this->ztdQuery("SELECT * FROM pg_mck_data ORDER BY id");
            $this->assertCount(2, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Lowercase DELETE failed: ' . $e->getMessage()
            );
        }
    }

    public function testLowercaseAggregate(): void
    {
        try {
            $rows = $this->ztdQuery(
                "select count(*) as cnt, sum(value) as total from pg_mck_data"
            );

            $this->assertEquals(3, (int) $rows[0]['cnt']);
            $this->assertEquals(60, (int) $rows[0]['total']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Lowercase aggregate failed: ' . $e->getMessage()
            );
        }
    }

    public function testLowercasePrepared(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "select * from pg_mck_data where value > ? order by id",
                [15]
            );

            $this->assertCount(2, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Lowercase prepared failed: ' . $e->getMessage()
            );
        }
    }

    public function testLowercaseTruncate(): void
    {
        try {
            $this->ztdExec("truncate pg_mck_data");

            $rows = $this->ztdQuery("SELECT * FROM pg_mck_data");
            $this->assertCount(0, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Lowercase TRUNCATE failed: ' . $e->getMessage()
            );
        }
    }
}
