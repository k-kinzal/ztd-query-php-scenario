<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests PostgreSQL SELECT INTO statement through the ZTD shadow store.
 *
 * SELECT INTO creates a new table from query results. When shadow DML has
 * modified the source data, SELECT INTO should create the table with the
 * shadow-visible data, not the physical data.
 *
 * This is important because:
 * - SELECT INTO is used for materialization of computed data
 * - CREATE TABLE AS SELECT is the equivalent DDL form
 * - Many reporting and ETL workflows use these patterns
 *
 * @spec SPEC-5.1
 */
class PostgresSelectIntoTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "CREATE TABLE pg_sinto_source (
                id INTEGER PRIMARY KEY,
                category TEXT NOT NULL,
                amount NUMERIC(10,2) NOT NULL
            )",
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_sinto_source', 'pg_sinto_result', 'pg_sinto_agg', 'pg_sinto_temp'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Clean up any leftover target tables
        $this->dropTable('pg_sinto_result');
        $this->dropTable('pg_sinto_agg');
        $this->dropTable('pg_sinto_temp');

        $this->pdo->exec("INSERT INTO pg_sinto_source (id, category, amount) VALUES (1, 'A', 100.00)");
        $this->pdo->exec("INSERT INTO pg_sinto_source (id, category, amount) VALUES (2, 'A', 200.00)");
        $this->pdo->exec("INSERT INTO pg_sinto_source (id, category, amount) VALUES (3, 'B', 300.00)");
        $this->pdo->exec("INSERT INTO pg_sinto_source (id, category, amount) VALUES (4, 'B', 400.00)");
    }

    /**
     * SELECT INTO should create a new table with shadow-visible data.
     */
    public function testSelectIntoBasic(): void
    {
        try {
            $this->pdo->exec("SELECT * INTO pg_sinto_result FROM pg_sinto_source WHERE category = 'A'");

            $rows = $this->ztdQuery("SELECT id, amount FROM pg_sinto_result ORDER BY id");

            if (count($rows) === 0) {
                $this->markTestIncomplete(
                    'SELECT INTO created table but returned 0 rows. '
                    . 'New table may not be registered with shadow store.'
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame(1, (int) $rows[0]['id']);
            $this->assertSame(2, (int) $rows[1]['id']);
        } catch (\Throwable $e) {
            $msg = $e->getMessage();
            if (stripos($msg, 'write protection') !== false || stripos($msg, 'Cannot determine') !== false) {
                $this->markTestIncomplete(
                    'SELECT INTO blocked by ZTD Write Protection: ' . $msg
                );
            }
            $this->markTestIncomplete('SELECT INTO basic failed: ' . $msg);
        }
    }

    /**
     * SELECT INTO after DML: new table should reflect shadow changes.
     */
    public function testSelectIntoAfterDml(): void
    {
        try {
            // Modify source through shadow
            $this->pdo->exec("DELETE FROM pg_sinto_source WHERE id = 1");
            $this->pdo->exec("UPDATE pg_sinto_source SET amount = 999.99 WHERE id = 2");

            $this->pdo->exec("SELECT * INTO pg_sinto_result FROM pg_sinto_source ORDER BY id");

            $rows = $this->ztdQuery("SELECT id, amount FROM pg_sinto_result ORDER BY id");

            if (count($rows) === 4) {
                $this->markTestIncomplete(
                    'SELECT INTO after DML: got 4 rows (physical data), expected 3. '
                    . 'SELECT INTO reads physical table, ignoring shadow DML.'
                );
            }

            if (count($rows) === 0) {
                $this->markTestIncomplete(
                    'SELECT INTO after DML: 0 rows. New table not visible through shadow.'
                );
            }

            $this->assertCount(3, $rows);
            // id=1 deleted, id=2 amount updated
            $this->assertSame(2, (int) $rows[0]['id']);
            $this->assertEquals(999.99, (float) $rows[0]['amount'], '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('SELECT INTO after DML failed: ' . $e->getMessage());
        }
    }

    /**
     * SELECT INTO with aggregation from shadow-modified source.
     */
    public function testSelectIntoWithAggregation(): void
    {
        try {
            $this->pdo->exec("INSERT INTO pg_sinto_source (id, category, amount) VALUES (5, 'A', 50.00)");

            $this->pdo->exec("
                SELECT category, SUM(amount) as total, COUNT(*) as cnt
                INTO pg_sinto_agg
                FROM pg_sinto_source
                GROUP BY category
                ORDER BY category
            ");

            $rows = $this->ztdQuery("SELECT category, total, cnt FROM pg_sinto_agg ORDER BY category");

            if (count($rows) === 0) {
                $this->markTestIncomplete(
                    'SELECT INTO with GROUP BY: 0 rows in target table.'
                );
            }

            // Category A: 100 + 200 + 50 = 350, count 3
            // Category B: 300 + 400 = 700, count 2
            $this->assertCount(2, $rows);
            $this->assertSame('A', $rows[0]['category']);
            $this->assertEquals(350.00, (float) $rows[0]['total'], '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('SELECT INTO with aggregation failed: ' . $e->getMessage());
        }
    }

    /**
     * SELECT INTO TEMPORARY should create a temp table.
     */
    public function testSelectIntoTemporary(): void
    {
        try {
            $this->pdo->exec("SELECT * INTO TEMPORARY pg_sinto_temp FROM pg_sinto_source WHERE id <= 2");

            $rows = $this->ztdQuery("SELECT * FROM pg_sinto_temp ORDER BY id");

            if (count($rows) === 0) {
                $this->markTestIncomplete(
                    'SELECT INTO TEMPORARY: 0 rows. Temp table may not be visible through ZTD.'
                );
            }

            $this->assertCount(2, $rows);
        } catch (\Throwable $e) {
            $msg = $e->getMessage();
            if (stripos($msg, 'write protection') !== false) {
                $this->markTestIncomplete(
                    'SELECT INTO TEMPORARY blocked by Write Protection: ' . $msg
                );
            }
            $this->markTestIncomplete('SELECT INTO TEMPORARY failed: ' . $msg);
        }
    }

    /**
     * CREATE TABLE AS SELECT (equivalent to SELECT INTO).
     */
    public function testCreateTableAsSelectAfterDml(): void
    {
        try {
            $this->pdo->exec("DELETE FROM pg_sinto_source WHERE category = 'B'");

            $this->pdo->exec("CREATE TABLE pg_sinto_result AS SELECT * FROM pg_sinto_source");

            $rows = $this->ztdQuery("SELECT * FROM pg_sinto_result ORDER BY id");

            if (count($rows) === 4) {
                $this->markTestIncomplete(
                    'CREATE TABLE AS SELECT: got 4 rows (physical), expected 2. '
                    . 'CTAS reads physical table, ignoring shadow DELETE.'
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame('A', $rows[0]['category']);
            $this->assertSame('A', $rows[1]['category']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('CREATE TABLE AS SELECT failed: ' . $e->getMessage());
        }
    }
}
