<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests delete-reinsert cycles and self-referencing subquery in UPDATE WHERE (PostgreSQL PDO).
 * SQL patterns exercised: DELETE then re-INSERT same PK, UPDATE WHERE IN (SELECT from same table),
 * chained delete-reinsert-update on same PK, shadow store PK tracking integrity.
 * @spec SPEC-10.2.173
 */
class PostgresDeleteReinsertCycleTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_dri_product (
                id INTEGER PRIMARY KEY,
                name VARCHAR(200) NOT NULL,
                category VARCHAR(100) NOT NULL,
                price NUMERIC(10,2) NOT NULL,
                status VARCHAR(50) NOT NULL DEFAULT \'active\'
            )',
            'CREATE TABLE pg_dri_price_log (
                id INTEGER PRIMARY KEY,
                product_id INTEGER NOT NULL,
                old_price NUMERIC(10,2),
                new_price NUMERIC(10,2) NOT NULL,
                changed_at DATE NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_dri_price_log', 'pg_dri_product'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_dri_product VALUES (1, 'Widget A', 'electronics', 29.99, 'active')");
        $this->pdo->exec("INSERT INTO pg_dri_product VALUES (2, 'Widget B', 'electronics', 49.99, 'active')");
        $this->pdo->exec("INSERT INTO pg_dri_product VALUES (3, 'Gadget X', 'accessories', 9.99, 'active')");
        $this->pdo->exec("INSERT INTO pg_dri_product VALUES (4, 'Gadget Y', 'accessories', 14.99, 'discontinued')");
        $this->pdo->exec("INSERT INTO pg_dri_product VALUES (5, 'Tool Z', 'tools', 79.99, 'active')");

        $this->pdo->exec("INSERT INTO pg_dri_price_log VALUES (1, 1, 24.99, 29.99, '2025-01-15')");
        $this->pdo->exec("INSERT INTO pg_dri_price_log VALUES (2, 2, 44.99, 49.99, '2025-02-01')");
    }

    /**
     * DELETE a row then re-INSERT with same PK but different values.
     */
    public function testDeleteThenReinsertSamePk(): void
    {
        $affected = $this->ztdExec("DELETE FROM pg_dri_product WHERE id = 3");
        $this->assertEquals(1, $affected);

        $rows = $this->ztdQuery("SELECT * FROM pg_dri_product WHERE id = 3");
        $this->assertCount(0, $rows);

        $this->pdo->exec("INSERT INTO pg_dri_product VALUES (3, 'Gadget X Pro', 'electronics', 19.99, 'active')");

        $rows = $this->ztdQuery("SELECT name, category, price FROM pg_dri_product WHERE id = 3");
        $this->assertCount(1, $rows);
        $this->assertSame('Gadget X Pro', $rows[0]['name']);
        $this->assertSame('electronics', $rows[0]['category']);
        $this->assertEqualsWithDelta(19.99, (float) $rows[0]['price'], 0.01);
    }

    /**
     * DELETE then re-INSERT, then UPDATE the re-inserted row.
     */
    public function testDeleteReinsertThenUpdate(): void
    {
        $this->ztdExec("DELETE FROM pg_dri_product WHERE id = 1");
        $this->pdo->exec("INSERT INTO pg_dri_product VALUES (1, 'Widget A v2', 'electronics', 34.99, 'active')");
        $this->ztdExec("UPDATE pg_dri_product SET price = 39.99 WHERE id = 1");

        $rows = $this->ztdQuery("SELECT name, price FROM pg_dri_product WHERE id = 1");
        $this->assertCount(1, $rows);
        $this->assertSame('Widget A v2', $rows[0]['name']);
        $this->assertEqualsWithDelta(39.99, (float) $rows[0]['price'], 0.01);
    }

    /**
     * Total row count remains correct after delete-reinsert cycle.
     */
    public function testRowCountAfterDeleteReinsert(): void
    {
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM pg_dri_product");
        $this->assertEquals(5, (int) $rows[0]['cnt']);

        $this->ztdExec("DELETE FROM pg_dri_product WHERE id = 4");
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM pg_dri_product");
        $this->assertEquals(4, (int) $rows[0]['cnt']);

        $this->pdo->exec("INSERT INTO pg_dri_product VALUES (4, 'Gadget Y Reborn', 'accessories', 16.99, 'active')");
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM pg_dri_product");
        $this->assertEquals(5, (int) $rows[0]['cnt']);
    }

    /**
     * UPDATE WHERE IN (SELECT from same table) — self-referencing subquery.
     *
     * NEW FINDING: On PostgreSQL, the CTE rewriter generates SQL that references
     * the table name twice ("duplicate alias"), causing "table name specified more than once".
     * Works correctly on MySQL (MySQLi + PDO) and SQLite.
     */
    public function testUpdateWhereInSelfReferencing(): void
    {
        try {
            $this->ztdExec(
                "UPDATE pg_dri_product SET status = 'featured'
                 WHERE id IN (SELECT id FROM pg_dri_product WHERE category = 'electronics')"
            );

            $rows = $this->ztdQuery(
                "SELECT id, status FROM pg_dri_product WHERE category = 'electronics' ORDER BY id"
            );
            $this->assertCount(2, $rows);
            $this->assertSame('featured', $rows[0]['status']);
            $this->assertSame('featured', $rows[1]['status']);

            $rows = $this->ztdQuery(
                "SELECT id, status FROM pg_dri_product WHERE category != 'electronics' ORDER BY id"
            );
            foreach ($rows as $row) {
                $this->assertNotSame('featured', $row['status']);
            }
        } catch (\Exception $e) {
            $this->assertStringContainsString('specified more than once', $e->getMessage());
            $this->markTestIncomplete(
                'NEW FINDING: UPDATE WHERE IN (SELECT from same table) fails on PostgreSQL — '
                . 'CTE rewriter generates duplicate table reference. Works on MySQL and SQLite.'
            );
        }
    }

    /**
     * UPDATE SET price with self-referencing category subquery.
     *
     * NEW FINDING: On PostgreSQL, the CTE rewriter incorrectly expands the table
     * reference inside the scalar subquery in UPDATE WHERE, producing syntax error.
     * Works correctly on MySQL (MySQLi + PDO) and SQLite.
     */
    public function testUpdateWithSelfReferencingCategorySubquery(): void
    {
        try {
            $this->ztdExec(
                "UPDATE pg_dri_product SET price = ROUND(price * 1.10, 2)
                 WHERE category = (SELECT category FROM pg_dri_product ORDER BY price DESC LIMIT 1)"
            );

            $rows = $this->ztdQuery("SELECT price FROM pg_dri_product WHERE id = 5");
            $this->assertEqualsWithDelta(87.99, (float) $rows[0]['price'], 0.01);

            $rows = $this->ztdQuery("SELECT price FROM pg_dri_product WHERE id = 1");
            $this->assertEqualsWithDelta(29.99, (float) $rows[0]['price'], 0.01);
        } catch (\Exception $e) {
            $this->assertStringContainsString('syntax error', $e->getMessage());
            $this->markTestIncomplete(
                'NEW FINDING: UPDATE WHERE = (scalar subquery from same table) fails on PostgreSQL — '
                . 'CTE rewriter incorrectly expands table reference inside subquery. Works on MySQL and SQLite.'
            );
        }
    }

    /**
     * Mixed exec() and prepare() on same data in same session.
     */
    public function testMixedExecAndPrepare(): void
    {
        $this->pdo->exec("INSERT INTO pg_dri_product VALUES (6, 'New Item', 'electronics', 99.99, 'active')");

        $rows = $this->ztdPrepareAndExecute(
            "SELECT name, price FROM pg_dri_product WHERE category = ? AND price > ?",
            ['electronics', 40.00]
        );

        $this->assertGreaterThanOrEqual(2, count($rows));
        $names = array_column($rows, 'name');
        $this->assertContains('Widget B', $names);
        $this->assertContains('New Item', $names);
    }

    /**
     * Delete-reinsert cycle with JOIN query to verify cross-table consistency.
     */
    public function testDeleteReinsertWithJoinVerification(): void
    {
        $this->ztdExec("DELETE FROM pg_dri_product WHERE id = 1");
        $this->pdo->exec("INSERT INTO pg_dri_product VALUES (1, 'Widget A Renewed', 'electronics', 35.99, 'active')");

        $rows = $this->ztdQuery(
            "SELECT p.name, pl.old_price, pl.new_price
             FROM pg_dri_product p
             JOIN pg_dri_price_log pl ON pl.product_id = p.id
             WHERE p.id = 1"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Widget A Renewed', $rows[0]['name']);
        $this->assertEqualsWithDelta(24.99, (float) $rows[0]['old_price'], 0.01);
    }

    /**
     * Physical isolation check.
     */
    public function testPhysicalIsolation(): void
    {
        $this->ztdExec("DELETE FROM pg_dri_product WHERE id = 3");
        $this->pdo->exec("INSERT INTO pg_dri_product VALUES (3, 'Replaced', 'other', 1.00, 'active')");

        $rows = $this->ztdQuery("SELECT name FROM pg_dri_product WHERE id = 3");
        $this->assertSame('Replaced', $rows[0]['name']);

        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM pg_dri_product")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt']);
    }
}
