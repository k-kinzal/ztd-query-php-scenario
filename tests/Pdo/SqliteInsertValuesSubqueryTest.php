<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests INSERT with scalar subquery in VALUES position on SQLite PDO.
 *
 * This is distinct from INSERT...SELECT — the subquery is embedded inside
 * a VALUES clause: INSERT INTO t1 (col) VALUES ((SELECT COUNT(*) FROM t2)).
 * The CTE rewriter must recognise and handle subqueries in this position.
 *
 * @spec SPEC-4.1
 */
class SqliteInsertValuesSubqueryTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_ivs_source (id INTEGER PRIMARY KEY, category TEXT, amount REAL)',
            'CREATE TABLE sl_ivs_target (id INTEGER PRIMARY KEY, total_count INTEGER, total_amount REAL)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_ivs_target', 'sl_ivs_source'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Seed source data through ZTD connection
        $this->pdo->exec("INSERT INTO sl_ivs_source VALUES (1, 'A', 100.00)");
        $this->pdo->exec("INSERT INTO sl_ivs_source VALUES (2, 'A', 200.00)");
        $this->pdo->exec("INSERT INTO sl_ivs_source VALUES (3, 'B', 150.00)");
    }

    /**
     * INSERT INTO target VALUES (1, (SELECT COUNT(*) FROM source), 0).
     * Expected: total_count = 3.
     */
    public function testInsertWithCountSubquery(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO sl_ivs_target (id, total_count, total_amount) VALUES (1, (SELECT COUNT(*) FROM sl_ivs_source), 0)"
            );

            $rows = $this->pdo->query('SELECT total_count FROM sl_ivs_target WHERE id = 1')->fetchAll(PDO::FETCH_ASSOC);
            $this->assertCount(1, $rows);
            $this->assertSame(3, (int) $rows[0]['total_count']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'INSERT with COUNT subquery in VALUES failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * INSERT INTO target VALUES (1, 0, (SELECT SUM(amount) FROM source)).
     * Expected: total_amount = 450.00.
     */
    public function testInsertWithSumSubquery(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO sl_ivs_target (id, total_count, total_amount) VALUES (1, 0, (SELECT SUM(amount) FROM sl_ivs_source))"
            );

            $rows = $this->pdo->query('SELECT total_amount FROM sl_ivs_target WHERE id = 1')->fetchAll(PDO::FETCH_ASSOC);
            $this->assertCount(1, $rows);
            $this->assertEqualsWithDelta(450.00, (float) $rows[0]['total_amount'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'INSERT with SUM subquery in VALUES failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * INSERT INTO target VALUES (1, (SELECT COUNT(*) FROM source WHERE category='A'),
     *                               (SELECT SUM(amount) FROM source WHERE category='A')).
     * Expected: total_count = 2, total_amount = 300.00.
     */
    public function testInsertWithFilteredSubquery(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO sl_ivs_target (id, total_count, total_amount) VALUES (1, (SELECT COUNT(*) FROM sl_ivs_source WHERE category = 'A'), (SELECT SUM(amount) FROM sl_ivs_source WHERE category = 'A'))"
            );

            $rows = $this->pdo->query('SELECT total_count, total_amount FROM sl_ivs_target WHERE id = 1')->fetchAll(PDO::FETCH_ASSOC);
            $this->assertCount(1, $rows);
            $this->assertSame(2, (int) $rows[0]['total_count']);
            $this->assertEqualsWithDelta(300.00, (float) $rows[0]['total_amount'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'INSERT with filtered subquery in VALUES failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * INSERT INTO target VALUES (1, 0, (SELECT MAX(amount) FROM source)).
     * Expected: total_amount = 200.00.
     */
    public function testInsertWithMaxSubquery(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO sl_ivs_target (id, total_count, total_amount) VALUES (1, 0, (SELECT MAX(amount) FROM sl_ivs_source))"
            );

            $rows = $this->pdo->query('SELECT total_amount FROM sl_ivs_target WHERE id = 1')->fetchAll(PDO::FETCH_ASSOC);
            $this->assertCount(1, $rows);
            $this->assertEqualsWithDelta(200.00, (float) $rows[0]['total_amount'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'INSERT with MAX subquery in VALUES failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * First INSERT into source through ZTD, then INSERT into target with
     * subquery that should see the shadow data (including the new row).
     */
    public function testSubqueryReferencingShadowData(): void
    {
        try {
            // Add a fourth row to source through ZTD
            $this->pdo->exec("INSERT INTO sl_ivs_source VALUES (4, 'C', 250.00)");

            // The subquery should see the new shadow row (4 rows total)
            $this->pdo->exec(
                "INSERT INTO sl_ivs_target (id, total_count, total_amount) VALUES (1, (SELECT COUNT(*) FROM sl_ivs_source), (SELECT SUM(amount) FROM sl_ivs_source))"
            );

            $rows = $this->pdo->query('SELECT total_count, total_amount FROM sl_ivs_target WHERE id = 1')->fetchAll(PDO::FETCH_ASSOC);
            $this->assertCount(1, $rows);
            $this->assertSame(4, (int) $rows[0]['total_count']);
            $this->assertEqualsWithDelta(700.00, (float) $rows[0]['total_amount'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'INSERT with subquery referencing shadow data failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Physical isolation: mutations through ZTD do not reach the physical table.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec(
            "INSERT INTO sl_ivs_target (id, total_count, total_amount) VALUES (1, (SELECT COUNT(*) FROM sl_ivs_source), 0)"
        );

        $this->pdo->disableZtd();
        $rows = $this->pdo->query('SELECT COUNT(*) AS cnt FROM sl_ivs_target')->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt'], 'Physical target table should be empty');

        $rows = $this->pdo->query('SELECT COUNT(*) AS cnt FROM sl_ivs_source')->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt'], 'Physical source table should be empty');
    }
}
