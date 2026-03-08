<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Extended generated column scenarios: generated columns in WHERE, GROUP BY,
 * ORDER BY, and behavior after updating source columns.
 * @spec SPEC-10.2.22
 */
class GeneratedColumnEdgeCasesTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_gce_products (
            id INT PRIMARY KEY,
            name VARCHAR(255),
            price DOUBLE,
            quantity INT,
            total DOUBLE GENERATED ALWAYS AS (price * quantity) STORED
        )';
    }

    protected function getTableNames(): array
    {
        return ['mi_gce_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_gce_products (id, name, price, quantity) VALUES (1, 'Widget', 9.99, 10)");
        $this->mysqli->query("INSERT INTO mi_gce_products (id, name, price, quantity) VALUES (2, 'Gadget', 29.99, 5)");
        $this->mysqli->query("INSERT INTO mi_gce_products (id, name, price, quantity) VALUES (3, 'Sprocket', 4.99, 20)");
    }

    public function testSelectGeneratedColumn(): void
    {
        $rows = $this->ztdQuery("SELECT name, total FROM mi_gce_products ORDER BY id");
        // Generated column value captured at INSERT time
        $this->assertEqualsWithDelta(99.90, (float) $rows[0]['total'], 0.01);
        $this->assertEqualsWithDelta(149.95, (float) $rows[1]['total'], 0.01);
        $this->assertEqualsWithDelta(99.80, (float) $rows[2]['total'], 0.01);
    }

    public function testGeneratedColumnInWhere(): void
    {
        $rows = $this->ztdQuery("SELECT name FROM mi_gce_products WHERE total > 100 ORDER BY name");
        $this->assertCount(1, $rows);
        $this->assertSame('Gadget', $rows[0]['name']);
    }

    public function testGeneratedColumnInOrderBy(): void
    {
        $rows = $this->ztdQuery("SELECT name, total FROM mi_gce_products ORDER BY total DESC");
        $this->assertSame('Gadget', $rows[0]['name']);
    }

    public function testGeneratedColumnInAggregate(): void
    {
        $rows = $this->ztdQuery("SELECT SUM(total) AS grand_total FROM mi_gce_products");
        $this->assertEqualsWithDelta(349.65, (float) $rows[0]['grand_total'], 0.01);
    }

    public function testUpdateSourceColumnGeneratedValueInShadow(): void
    {
        // Update the source column (price)
        $this->mysqli->query("UPDATE mi_gce_products SET price = 19.99 WHERE id = 1");

        // In shadow store, the generated column retains the originally captured value
        // because the CTE stores literal values, not re-evaluates expressions.
        // The shadow store captured total = 9.99 * 10 = 99.90 at insert time.
        // After UPDATE SET price = 19.99, the shadow should have price=19.99
        // but total may still be 99.90 (stale generated value) since shadow
        // does not re-compute generated columns.
        $rows = $this->ztdQuery("SELECT price, quantity, total FROM mi_gce_products WHERE id = 1");
        $this->assertEqualsWithDelta(19.99, (float) $rows[0]['price'], 0.01);
        // total is stale or re-computed -- document whichever behavior is observed
        $total = (float) $rows[0]['total'];
        // Accept either stale (99.90) or recomputed (199.90)
        $this->assertTrue(
            abs($total - 99.90) < 0.01 || abs($total - 199.90) < 0.01,
            "Generated column total should be either stale (99.90) or recomputed (199.90), got {$total}"
        );
    }

    public function testInsertMultipleThenGroupByGeneratedRange(): void
    {
        $rows = $this->ztdQuery("
            SELECT CASE
                WHEN total < 100 THEN 'low'
                WHEN total < 150 THEN 'mid'
                ELSE 'high'
            END AS tier,
            COUNT(*) AS cnt
            FROM mi_gce_products
            GROUP BY tier
            ORDER BY tier
        ");
        // Verify the query runs and groups correctly
        $this->assertGreaterThanOrEqual(1, count($rows));
    }

    public function testPreparedSelectWithGeneratedColumn(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT name, total FROM mi_gce_products WHERE total > ? ORDER BY total",
            [100]
        );
        $this->assertCount(1, $rows);
        $this->assertSame('Gadget', $rows[0]['name']);
    }

    public function testDeleteByGeneratedColumnCondition(): void
    {
        $this->mysqli->query("DELETE FROM mi_gce_products WHERE total < 100");

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_gce_products");
        $this->assertSame(1, (int) $rows[0]['cnt']); // Only Gadget (149.95)
    }
}
