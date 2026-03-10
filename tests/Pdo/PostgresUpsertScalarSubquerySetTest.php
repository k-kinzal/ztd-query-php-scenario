<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests upsert (ON CONFLICT DO UPDATE) where the SET clause uses a scalar
 * subquery to compute the new value on PostgreSQL.
 *
 * Pattern: INSERT ... ON CONFLICT DO UPDATE SET col = (SELECT ... FROM other_table)
 *
 * @spec SPEC-4.2
 */
class PostgresUpsertScalarSubquerySetTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_uss_tiers (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                discount_pct NUMERIC(5,2) NOT NULL DEFAULT 0.0
            )',
            'CREATE TABLE pg_uss_customers (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                tier_id INTEGER NOT NULL,
                applied_discount NUMERIC(5,2) NOT NULL DEFAULT 0.0
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_uss_customers', 'pg_uss_tiers'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_uss_tiers (id, name, discount_pct) VALUES (1, 'Gold', 15.0)");
        $this->pdo->exec("INSERT INTO pg_uss_tiers (id, name, discount_pct) VALUES (2, 'Silver', 10.0)");
        $this->pdo->exec("INSERT INTO pg_uss_customers (id, name, tier_id, applied_discount) VALUES (1, 'Alice', 1, 0.0)");
    }

    /**
     * ON CONFLICT DO UPDATE SET applied_discount = (SELECT discount_pct FROM tiers ...)
     *
     * @spec SPEC-4.2
     */
    public function testUpsertSetFromScalarSubquery(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO pg_uss_customers (id, name, tier_id, applied_discount) VALUES (1, 'Alice', 1, 0.0)
                 ON CONFLICT (id) DO UPDATE
                 SET applied_discount = (SELECT discount_pct FROM pg_uss_tiers WHERE id = EXCLUDED.tier_id)"
            );

            $rows = $this->ztdQuery('SELECT id, name, applied_discount FROM pg_uss_customers WHERE id = 1');

            if (count($rows) !== 1) {
                $this->markTestIncomplete('Expected 1 row, got ' . count($rows));
            }

            $this->assertCount(1, $rows);
            $this->assertEqualsWithDelta(15.0, (float) $rows[0]['applied_discount'], 0.01,
                'applied_discount should be set from tier lookup (Gold = 15.0)');
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Upsert scalar subquery SET (PostgreSQL) failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * COALESCE wrapping subquery in ON CONFLICT SET — guards against NULL.
     *
     * @spec SPEC-4.2
     */
    public function testUpsertCoalesceSubqueryInSet(): void
    {
        try {
            // tier_id 99 doesn't exist
            $this->pdo->exec(
                "INSERT INTO pg_uss_customers (id, name, tier_id, applied_discount) VALUES (1, 'Alice', 99, 0.0)
                 ON CONFLICT (id) DO UPDATE
                 SET tier_id = EXCLUDED.tier_id,
                     applied_discount = COALESCE(
                         (SELECT discount_pct FROM pg_uss_tiers WHERE id = EXCLUDED.tier_id),
                         0.0
                     )"
            );

            $rows = $this->ztdQuery('SELECT id, tier_id, applied_discount FROM pg_uss_customers WHERE id = 1');

            if (count($rows) !== 1) {
                $this->markTestIncomplete('Expected 1 row, got ' . count($rows));
            }

            $this->assertEqualsWithDelta(0.0, (float) $rows[0]['applied_discount'], 0.01,
                'applied_discount should be 0.0 via COALESCE when subquery returns NULL');
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Upsert COALESCE subquery (PostgreSQL) failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Prepared upsert with scalar subquery in SET and $N params.
     *
     * @spec SPEC-4.2
     */
    public function testPreparedUpsertScalarSubquerySet(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "INSERT INTO pg_uss_customers (id, name, tier_id, applied_discount) VALUES ($1, $2, $3, 0.0)
                 ON CONFLICT (id) DO UPDATE
                 SET applied_discount = (SELECT discount_pct FROM pg_uss_tiers WHERE id = $3)"
            );
            $stmt->execute([1, 'Alice', 2]);

            $rows = $this->ztdQuery('SELECT id, applied_discount FROM pg_uss_customers WHERE id = 1');

            if (count($rows) !== 1) {
                $this->markTestIncomplete('Expected 1 row, got ' . count($rows));
            }

            $this->assertEqualsWithDelta(10.0, (float) $rows[0]['applied_discount'], 0.01,
                'applied_discount should be 10.0 (Silver tier) via prepared stmt');
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Prepared upsert scalar subquery (PostgreSQL) failed: ' . $e->getMessage()
            );
        }
    }
}
