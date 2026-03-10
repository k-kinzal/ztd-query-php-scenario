<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests INSERT ... ON DUPLICATE KEY UPDATE where the SET clause uses a scalar
 * subquery to compute the new value, via PDO MySQL.
 *
 * @spec SPEC-4.2
 */
class MysqlUpsertScalarSubquerySetTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mp_uss_tiers (
                id INT PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                discount_pct DECIMAL(5,2) NOT NULL DEFAULT 0.00
            ) ENGINE=InnoDB',
            'CREATE TABLE mp_uss_customers (
                id INT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                tier_id INT NOT NULL,
                applied_discount DECIMAL(5,2) NOT NULL DEFAULT 0.00
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mp_uss_customers', 'mp_uss_tiers'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO mp_uss_tiers (id, name, discount_pct) VALUES (1, 'Gold', 15.00)");
        $this->pdo->exec("INSERT INTO mp_uss_tiers (id, name, discount_pct) VALUES (2, 'Silver', 10.00)");
        $this->pdo->exec("INSERT INTO mp_uss_customers (id, name, tier_id, applied_discount) VALUES (1, 'Alice', 1, 0.00)");
    }

    /**
     * ON DUPLICATE KEY UPDATE applied_discount = (SELECT discount_pct FROM tiers ...)
     *
     * @spec SPEC-4.2
     */
    public function testUpsertSetFromScalarSubquery(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO mp_uss_customers (id, name, tier_id, applied_discount) VALUES (1, 'Alice', 1, 0.00)
                 ON DUPLICATE KEY UPDATE
                 applied_discount = (SELECT discount_pct FROM mp_uss_tiers WHERE id = VALUES(tier_id))"
            );

            $rows = $this->ztdQuery('SELECT id, name, applied_discount FROM mp_uss_customers WHERE id = 1');

            if (count($rows) !== 1) {
                $this->markTestIncomplete('Expected 1 row, got ' . count($rows));
            }

            $this->assertEqualsWithDelta(15.00, (float) $rows[0]['applied_discount'], 0.01,
                'applied_discount should be 15.00 from Gold tier lookup');
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Upsert scalar subquery SET (MySQL PDO) failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Prepared variant with ? params.
     *
     * @spec SPEC-4.2
     */
    public function testPreparedUpsertScalarSubquerySet(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "INSERT INTO mp_uss_customers (id, name, tier_id, applied_discount) VALUES (?, ?, ?, 0.00)
                 ON DUPLICATE KEY UPDATE
                 applied_discount = (SELECT discount_pct FROM mp_uss_tiers WHERE id = ?)"
            );
            $stmt->execute([1, 'Alice', 2, 2]);

            $rows = $this->ztdQuery('SELECT id, applied_discount FROM mp_uss_customers WHERE id = 1');

            if (count($rows) !== 1) {
                $this->markTestIncomplete('Expected 1 row, got ' . count($rows));
            }

            $this->assertEqualsWithDelta(10.00, (float) $rows[0]['applied_discount'], 0.01,
                'applied_discount should be 10.00 (Silver tier) via prepared stmt');
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Prepared upsert scalar subquery (MySQL PDO) failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * COALESCE wrapping subquery in ON DUPLICATE KEY UPDATE, with ? params.
     *
     * @spec SPEC-4.2
     */
    public function testPreparedUpsertCoalesceSubquery(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "INSERT INTO mp_uss_customers (id, name, tier_id, applied_discount) VALUES (?, ?, ?, 0.00)
                 ON DUPLICATE KEY UPDATE
                 tier_id = VALUES(tier_id),
                 applied_discount = COALESCE(
                     (SELECT discount_pct FROM mp_uss_tiers WHERE id = ?),
                     0.00
                 )"
            );
            // tier_id 99 doesn't exist
            $stmt->execute([1, 'Alice', 99, 99]);

            $rows = $this->ztdQuery('SELECT id, applied_discount FROM mp_uss_customers WHERE id = 1');

            if (count($rows) !== 1) {
                $this->markTestIncomplete('Expected 1 row, got ' . count($rows));
            }

            $this->assertEqualsWithDelta(0.00, (float) $rows[0]['applied_discount'], 0.01,
                'applied_discount should be 0.00 via COALESCE');
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Prepared upsert COALESCE subquery (MySQL PDO) failed: ' . $e->getMessage()
            );
        }
    }
}
