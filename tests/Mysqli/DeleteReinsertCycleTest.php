<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests delete-reinsert cycles and self-referencing subquery in UPDATE WHERE (MySQLi).
 * SQL patterns exercised: DELETE then re-INSERT same PK, UPDATE WHERE IN (SELECT from same table),
 * chained delete-reinsert-update on same PK, shadow store PK tracking integrity.
 * @spec SPEC-10.2.173
 */
class DeleteReinsertCycleTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_dri_product (
                id INT PRIMARY KEY,
                name VARCHAR(200) NOT NULL,
                category VARCHAR(100) NOT NULL,
                price DECIMAL(10,2) NOT NULL,
                status VARCHAR(50) NOT NULL DEFAULT \'active\'
            ) ENGINE=InnoDB',
            'CREATE TABLE mi_dri_price_log (
                id INT PRIMARY KEY,
                product_id INT NOT NULL,
                old_price DECIMAL(10,2),
                new_price DECIMAL(10,2) NOT NULL,
                changed_at DATE NOT NULL
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_dri_price_log', 'mi_dri_product'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_dri_product VALUES (1, 'Widget A', 'electronics', 29.99, 'active')");
        $this->mysqli->query("INSERT INTO mi_dri_product VALUES (2, 'Widget B', 'electronics', 49.99, 'active')");
        $this->mysqli->query("INSERT INTO mi_dri_product VALUES (3, 'Gadget X', 'accessories', 9.99, 'active')");
        $this->mysqli->query("INSERT INTO mi_dri_product VALUES (4, 'Gadget Y', 'accessories', 14.99, 'discontinued')");
        $this->mysqli->query("INSERT INTO mi_dri_product VALUES (5, 'Tool Z', 'tools', 79.99, 'active')");

        $this->mysqli->query("INSERT INTO mi_dri_price_log VALUES (1, 1, 24.99, 29.99, '2025-01-15')");
        $this->mysqli->query("INSERT INTO mi_dri_price_log VALUES (2, 2, 44.99, 49.99, '2025-02-01')");
    }

    /**
     * DELETE a row then re-INSERT with same PK but different values.
     */
    public function testDeleteThenReinsertSamePk(): void
    {
        $affected = $this->ztdExec("DELETE FROM mi_dri_product WHERE id = 3");
        $this->assertEquals(1, $affected);

        $rows = $this->ztdQuery("SELECT * FROM mi_dri_product WHERE id = 3");
        $this->assertCount(0, $rows);

        $this->mysqli->query("INSERT INTO mi_dri_product VALUES (3, 'Gadget X Pro', 'electronics', 19.99, 'active')");

        $rows = $this->ztdQuery("SELECT name, category, price FROM mi_dri_product WHERE id = 3");
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
        $this->ztdExec("DELETE FROM mi_dri_product WHERE id = 1");
        $this->mysqli->query("INSERT INTO mi_dri_product VALUES (1, 'Widget A v2', 'electronics', 34.99, 'active')");
        $this->ztdExec("UPDATE mi_dri_product SET price = 39.99 WHERE id = 1");

        $rows = $this->ztdQuery("SELECT name, price FROM mi_dri_product WHERE id = 1");
        $this->assertCount(1, $rows);
        $this->assertSame('Widget A v2', $rows[0]['name']);
        $this->assertEqualsWithDelta(39.99, (float) $rows[0]['price'], 0.01);
    }

    /**
     * Total row count remains correct after delete-reinsert cycle.
     */
    public function testRowCountAfterDeleteReinsert(): void
    {
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_dri_product");
        $this->assertEquals(5, (int) $rows[0]['cnt']);

        $this->ztdExec("DELETE FROM mi_dri_product WHERE id = 4");
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_dri_product");
        $this->assertEquals(4, (int) $rows[0]['cnt']);

        $this->mysqli->query("INSERT INTO mi_dri_product VALUES (4, 'Gadget Y Reborn', 'accessories', 16.99, 'active')");
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_dri_product");
        $this->assertEquals(5, (int) $rows[0]['cnt']);
    }

    /**
     * UPDATE WHERE IN (SELECT from same table) — self-referencing subquery.
     */
    public function testUpdateWhereInSelfReferencing(): void
    {
        $this->ztdExec(
            "UPDATE mi_dri_product SET status = 'featured'
             WHERE id IN (SELECT id FROM mi_dri_product WHERE category = 'electronics')"
        );

        $rows = $this->ztdQuery(
            "SELECT id, status FROM mi_dri_product WHERE category = 'electronics' ORDER BY id"
        );
        $this->assertCount(2, $rows);
        $this->assertSame('featured', $rows[0]['status']);
        $this->assertSame('featured', $rows[1]['status']);

        $rows = $this->ztdQuery(
            "SELECT id, status FROM mi_dri_product WHERE category != 'electronics' ORDER BY id"
        );
        foreach ($rows as $row) {
            $this->assertNotSame('featured', $row['status']);
        }
    }

    /**
     * UPDATE SET price with self-referencing category subquery.
     */
    public function testUpdateWithSelfReferencingCategorySubquery(): void
    {
        $this->ztdExec(
            "UPDATE mi_dri_product SET price = ROUND(price * 1.10, 2)
             WHERE category = (SELECT category FROM mi_dri_product ORDER BY price DESC LIMIT 1)"
        );

        $rows = $this->ztdQuery("SELECT price FROM mi_dri_product WHERE id = 5");
        $this->assertEqualsWithDelta(87.99, (float) $rows[0]['price'], 0.01);

        $rows = $this->ztdQuery("SELECT price FROM mi_dri_product WHERE id = 1");
        $this->assertEqualsWithDelta(29.99, (float) $rows[0]['price'], 0.01);
    }

    /**
     * Mixed query() and prepare() on same data in same session.
     */
    public function testMixedQueryAndPrepare(): void
    {
        $this->mysqli->query("INSERT INTO mi_dri_product VALUES (6, 'New Item', 'electronics', 99.99, 'active')");

        $rows = $this->ztdPrepareAndExecute(
            "SELECT name, price FROM mi_dri_product WHERE category = ? AND price > ?",
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
        $this->ztdExec("DELETE FROM mi_dri_product WHERE id = 1");
        $this->mysqli->query("INSERT INTO mi_dri_product VALUES (1, 'Widget A Renewed', 'electronics', 35.99, 'active')");

        $rows = $this->ztdQuery(
            "SELECT p.name, pl.old_price, pl.new_price
             FROM mi_dri_product p
             JOIN mi_dri_price_log pl ON pl.product_id = p.id
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
        $this->ztdExec("DELETE FROM mi_dri_product WHERE id = 3");
        $this->mysqli->query("INSERT INTO mi_dri_product VALUES (3, 'Replaced', 'other', 1.00, 'active')");

        $rows = $this->ztdQuery("SELECT name FROM mi_dri_product WHERE id = 3");
        $this->assertSame('Replaced', $rows[0]['name']);

        $this->disableZtd();
        $result = $this->mysqli->query("SELECT COUNT(*) AS cnt FROM mi_dri_product");
        $row = $result->fetch_assoc();
        $this->assertSame(0, (int) $row['cnt']);
    }
}
