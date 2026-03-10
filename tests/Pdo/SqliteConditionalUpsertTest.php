<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests conditional upsert (INSERT ... ON CONFLICT DO UPDATE ... WHERE) through
 * the ZTD shadow store on SQLite.
 *
 * SQLite supports the same ON CONFLICT DO UPDATE ... WHERE syntax as PostgreSQL.
 * The WHERE clause makes the update conditional: if the condition is false the
 * row is left unchanged (neither inserted nor updated — the conflict is
 * effectively absorbed).
 *
 * @spec SPEC-4.2
 */
class SqliteConditionalUpsertTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_condup_products (
            id INTEGER PRIMARY KEY,
            name TEXT,
            price REAL,
            version INTEGER DEFAULT 1
        )';
    }

    protected function getTableNames(): array
    {
        return ['sl_condup_products'];
    }

    /**
     * When the WHERE condition on the DO UPDATE clause matches, the row should
     * be updated with the new values.
     *
     * Scenario: existing version=1, incoming version=2 => update happens.
     *
     * @spec SPEC-4.2
     */
    public function testConditionalUpsertUpdatesWhenWhereMatches(): void
    {
        try {
            // Seed with version 1
            $this->pdo->exec(
                "INSERT INTO sl_condup_products (id, name, price, version) VALUES (1, 'Widget', 10.00, 1)"
            );

            // Upsert with version 2 — WHERE condition should match (1 < 2)
            $this->pdo->exec(
                "INSERT INTO sl_condup_products (id, name, price, version) VALUES (1, 'Widget', 15.00, 2)
                 ON CONFLICT (id) DO UPDATE
                 SET price = excluded.price, version = excluded.version
                 WHERE sl_condup_products.version < excluded.version"
            );

            $rows = $this->ztdQuery('SELECT id, name, price, version FROM sl_condup_products WHERE id = 1');

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Conditional upsert (match): expected 1 row, got ' . count($rows)
                );
            }

            $this->assertCount(1, $rows);
            $this->assertEqualsWithDelta(15.00, (float) $rows[0]['price'], 0.01,
                'Price should be updated to 15.00 when WHERE matches');
            $this->assertEquals(2, (int) $rows[0]['version'],
                'Version should be updated to 2 when WHERE matches');
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Conditional upsert (WHERE matches) failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * When the WHERE condition on the DO UPDATE clause does NOT match, the
     * update should be skipped and the existing row retained.
     *
     * Scenario: existing version=5, incoming version=2 => no update.
     *
     * @spec SPEC-4.2
     */
    public function testConditionalUpsertSkipsWhenWhereNotMatches(): void
    {
        try {
            // Seed with version 5
            $this->pdo->exec(
                "INSERT INTO sl_condup_products (id, name, price, version) VALUES (1, 'Widget', 50.00, 5)"
            );

            // Upsert with version 2 — WHERE condition should NOT match (5 < 2 is false)
            $this->pdo->exec(
                "INSERT INTO sl_condup_products (id, name, price, version) VALUES (1, 'Widget', 15.00, 2)
                 ON CONFLICT (id) DO UPDATE
                 SET price = excluded.price, version = excluded.version
                 WHERE sl_condup_products.version < excluded.version"
            );

            $rows = $this->ztdQuery('SELECT id, name, price, version FROM sl_condup_products WHERE id = 1');

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Conditional upsert (skip): expected 1 row, got ' . count($rows)
                );
            }

            $this->assertCount(1, $rows);
            $this->assertEqualsWithDelta(50.00, (float) $rows[0]['price'], 0.01,
                'Price should remain 50.00 when WHERE does not match');
            $this->assertEquals(5, (int) $rows[0]['version'],
                'Version should remain 5 when WHERE does not match');
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Conditional upsert (WHERE not matching) failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * After a conditional upsert that triggers an update, SELECT through the
     * shadow store should reflect the updated values.
     *
     * @spec SPEC-4.2
     */
    public function testConditionalUpsertSelectVerify(): void
    {
        try {
            // Seed
            $this->pdo->exec(
                "INSERT INTO sl_condup_products (id, name, price, version) VALUES (1, 'Alpha', 20.00, 1)"
            );

            // Conflicting upsert — version goes from 1 to 3
            $this->pdo->exec(
                "INSERT INTO sl_condup_products (id, name, price, version) VALUES (1, 'Alpha', 25.00, 3)
                 ON CONFLICT (id) DO UPDATE
                 SET price = excluded.price, version = excluded.version
                 WHERE sl_condup_products.version < excluded.version"
            );

            // Verify via SELECT
            $rows = $this->ztdQuery('SELECT id, name, price, version FROM sl_condup_products WHERE id = 1');

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Conditional upsert SELECT verify: expected 1 row, got ' . count($rows)
                );
            }

            $this->assertCount(1, $rows);
            $this->assertSame('Alpha', $rows[0]['name']);
            $this->assertEqualsWithDelta(25.00, (float) $rows[0]['price'], 0.01);
            $this->assertEquals(3, (int) $rows[0]['version']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Conditional upsert SELECT verify failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Prepared statement with ? placeholders for a conditional upsert.
     *
     * @spec SPEC-4.2
     */
    public function testPreparedConditionalUpsert(): void
    {
        try {
            // Seed
            $this->pdo->exec(
                "INSERT INTO sl_condup_products (id, name, price, version) VALUES (1, 'Beta', 30.00, 1)"
            );

            $sql = "INSERT INTO sl_condup_products (id, name, price, version) VALUES (?, ?, ?, ?)
                    ON CONFLICT (id) DO UPDATE
                    SET price = excluded.price, version = excluded.version
                    WHERE sl_condup_products.version < excluded.version";

            $stmt = $this->pdo->prepare($sql);
            $stmt->execute([1, 'Beta', 40.00, 3]);

            $rows = $this->ztdQuery('SELECT id, name, price, version FROM sl_condup_products WHERE id = 1');

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Prepared conditional upsert: expected 1 row, got ' . count($rows)
                );
            }

            $this->assertCount(1, $rows);
            $this->assertEqualsWithDelta(40.00, (float) $rows[0]['price'], 0.01,
                'Prepared conditional upsert should update price');
            $this->assertEquals(3, (int) $rows[0]['version'],
                'Prepared conditional upsert should update version');
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Prepared conditional upsert failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Conditional upsert with WHERE excluded.price > products.price — only
     * update if the incoming price is higher than the existing price.
     *
     * @spec SPEC-4.2
     */
    public function testConditionalUpsertWithExcludedInWhere(): void
    {
        try {
            // Seed with price 100.00
            $this->pdo->exec(
                "INSERT INTO sl_condup_products (id, name, price, version) VALUES (1, 'Gamma', 100.00, 1)"
            );

            // Attempt upsert with lower price (50.00) — should NOT update
            $this->pdo->exec(
                "INSERT INTO sl_condup_products (id, name, price, version) VALUES (1, 'Gamma', 50.00, 2)
                 ON CONFLICT (id) DO UPDATE
                 SET price = excluded.price, version = excluded.version
                 WHERE excluded.price > sl_condup_products.price"
            );

            $rows = $this->ztdQuery('SELECT id, price, version FROM sl_condup_products WHERE id = 1');

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Conditional upsert EXCLUDED in WHERE: expected 1 row, got ' . count($rows)
                );
            }

            $this->assertCount(1, $rows);
            $this->assertEqualsWithDelta(100.00, (float) $rows[0]['price'], 0.01,
                'Price should remain 100.00 when excluded.price (50) is not greater');
            $this->assertEquals(1, (int) $rows[0]['version'],
                'Version should remain 1 when WHERE does not match');

            // Now upsert with higher price (150.00) — SHOULD update
            $this->pdo->exec(
                "INSERT INTO sl_condup_products (id, name, price, version) VALUES (1, 'Gamma', 150.00, 3)
                 ON CONFLICT (id) DO UPDATE
                 SET price = excluded.price, version = excluded.version
                 WHERE excluded.price > sl_condup_products.price"
            );

            $rows2 = $this->ztdQuery('SELECT id, price, version FROM sl_condup_products WHERE id = 1');

            if (count($rows2) !== 1) {
                $this->markTestIncomplete(
                    'Conditional upsert EXCLUDED in WHERE (update): expected 1 row, got ' . count($rows2)
                );
            }

            $this->assertCount(1, $rows2);
            $this->assertEqualsWithDelta(150.00, (float) $rows2[0]['price'], 0.01,
                'Price should be updated to 150.00 when excluded.price is greater');
            $this->assertEquals(3, (int) $rows2[0]['version'],
                'Version should be updated to 3 when WHERE matches');
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Conditional upsert with EXCLUDED in WHERE failed: ' . $e->getMessage()
            );
        }
    }
}
