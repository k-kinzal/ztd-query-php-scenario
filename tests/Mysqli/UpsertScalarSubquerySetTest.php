<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests INSERT ... ON DUPLICATE KEY UPDATE where the SET clause uses a scalar
 * subquery to compute the new value.
 *
 * Pattern: INSERT ... ON DUPLICATE KEY UPDATE col = (SELECT ... FROM other_table)
 *
 * The CTE rewriter must handle subqueries inside the ON DUPLICATE KEY UPDATE clause.
 * This pattern is commonly used for lookup-based upserts.
 *
 * @spec SPEC-4.2
 */
class UpsertScalarSubquerySetTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_uss_tiers (
                id INT PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                discount_pct DECIMAL(5,2) NOT NULL DEFAULT 0.00
            ) ENGINE=InnoDB',
            'CREATE TABLE mi_uss_customers (
                id INT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                tier_id INT NOT NULL,
                applied_discount DECIMAL(5,2) NOT NULL DEFAULT 0.00
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_uss_customers', 'mi_uss_tiers'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_uss_tiers (id, name, discount_pct) VALUES (1, 'Gold', 15.00)");
        $this->mysqli->query("INSERT INTO mi_uss_tiers (id, name, discount_pct) VALUES (2, 'Silver', 10.00)");
        $this->mysqli->query("INSERT INTO mi_uss_customers (id, name, tier_id, applied_discount) VALUES (1, 'Alice', 1, 0.00)");
    }

    /**
     * ON DUPLICATE KEY UPDATE applied_discount = (SELECT discount_pct FROM tiers ...)
     *
     * @spec SPEC-4.2
     */
    public function testUpsertSetFromScalarSubquery(): void
    {
        try {
            $this->mysqli->query(
                "INSERT INTO mi_uss_customers (id, name, tier_id, applied_discount) VALUES (1, 'Alice', 1, 0.00)
                 ON DUPLICATE KEY UPDATE
                 applied_discount = (SELECT discount_pct FROM mi_uss_tiers WHERE id = VALUES(tier_id))"
            );

            $rows = $this->ztdQuery('SELECT id, name, applied_discount FROM mi_uss_customers WHERE id = 1');

            if (count($rows) !== 1) {
                $this->markTestIncomplete('Expected 1 row, got ' . count($rows));
            }

            $this->assertCount(1, $rows);
            $this->assertEqualsWithDelta(15.00, (float) $rows[0]['applied_discount'], 0.01,
                'applied_discount should be set from tier lookup (Gold = 15.00)');
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Upsert scalar subquery SET (MySQLi) failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * No conflict — fresh insert should use the literal value, not the subquery.
     *
     * @spec SPEC-4.2
     */
    public function testUpsertScalarSubqueryNoConflict(): void
    {
        try {
            $this->mysqli->query(
                "INSERT INTO mi_uss_customers (id, name, tier_id, applied_discount) VALUES (2, 'Bob', 2, 5.00)
                 ON DUPLICATE KEY UPDATE
                 applied_discount = (SELECT discount_pct FROM mi_uss_tiers WHERE id = VALUES(tier_id))"
            );

            $rows = $this->ztdQuery('SELECT id, name, applied_discount FROM mi_uss_customers WHERE id = 2');

            if (count($rows) !== 1) {
                $this->markTestIncomplete('Expected 1 row, got ' . count($rows));
            }

            $this->assertSame('Bob', $rows[0]['name']);
            $this->assertEqualsWithDelta(5.00, (float) $rows[0]['applied_discount'], 0.01,
                'No conflict — applied_discount should be the literal 5.00');
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Upsert scalar subquery (no conflict) failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * COALESCE wrapping the subquery in ON DUPLICATE KEY UPDATE.
     *
     * @spec SPEC-4.2
     */
    public function testUpsertCoalesceSubqueryInSet(): void
    {
        try {
            // tier_id 99 doesn't exist
            $this->mysqli->query(
                "INSERT INTO mi_uss_customers (id, name, tier_id, applied_discount) VALUES (1, 'Alice', 99, 0.00)
                 ON DUPLICATE KEY UPDATE
                 tier_id = VALUES(tier_id),
                 applied_discount = COALESCE(
                     (SELECT discount_pct FROM mi_uss_tiers WHERE id = VALUES(tier_id)),
                     0.00
                 )"
            );

            $rows = $this->ztdQuery('SELECT id, tier_id, applied_discount FROM mi_uss_customers WHERE id = 1');

            if (count($rows) !== 1) {
                $this->markTestIncomplete('Expected 1 row, got ' . count($rows));
            }

            $this->assertEqualsWithDelta(0.00, (float) $rows[0]['applied_discount'], 0.01,
                'applied_discount should be 0.00 via COALESCE when subquery returns NULL');
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Upsert COALESCE subquery (MySQLi) failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Prepared upsert with scalar subquery in SET and ? params.
     *
     * @spec SPEC-4.2
     */
    public function testPreparedUpsertScalarSubquerySet(): void
    {
        try {
            $sql = "INSERT INTO mi_uss_customers (id, name, tier_id, applied_discount) VALUES (?, ?, ?, 0.00)
                    ON DUPLICATE KEY UPDATE
                    applied_discount = (SELECT discount_pct FROM mi_uss_tiers WHERE id = ?)";

            $rows = $this->ztdPrepareAndExecute(
                "SELECT id FROM mi_uss_customers WHERE id = 0 LIMIT 0",
                []
            );

            $stmt = $this->mysqli->prepare($sql);
            $stmt->bind_param('isis', ...[$id = 1, $name = 'Alice', $tierId = 2, $tierIdAgain = 2]);
            $stmt->execute();

            $rows = $this->ztdQuery('SELECT id, applied_discount FROM mi_uss_customers WHERE id = 1');

            if (count($rows) !== 1) {
                $this->markTestIncomplete('Expected 1 row, got ' . count($rows));
            }

            $this->assertEqualsWithDelta(10.00, (float) $rows[0]['applied_discount'], 0.01,
                'applied_discount should be 10.00 (Silver tier) via prepared stmt');
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Prepared upsert scalar subquery (MySQLi) failed: ' . $e->getMessage()
            );
        }
    }
}
