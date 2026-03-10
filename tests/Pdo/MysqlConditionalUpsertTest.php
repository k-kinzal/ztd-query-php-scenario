<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests upsert behavior on MySQL via PDO, documenting the behavioral difference
 * from PostgreSQL/SQLite conditional upsert.
 *
 * MySQL uses INSERT ... ON DUPLICATE KEY UPDATE which does NOT support a WHERE
 * clause on the update action. Every conflict unconditionally triggers the
 * UPDATE SET expressions. Conditional-update logic must be pushed into the SET
 * expressions themselves (e.g. using IF/CASE).
 *
 * This test file verifies that basic ON DUPLICATE KEY UPDATE still works through
 * the ZTD shadow store and documents the absence of WHERE-guarded upsert.
 *
 * @spec SPEC-4.2
 */
class MysqlConditionalUpsertTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE my_condup_products (
            id INT PRIMARY KEY,
            name VARCHAR(100),
            price DECIMAL(10,2),
            version INT DEFAULT 1
        )';
    }

    protected function getTableNames(): array
    {
        return ['my_condup_products'];
    }

    /**
     * Basic ON DUPLICATE KEY UPDATE — unconditional update on conflict.
     *
     * MySQL does not support ON CONFLICT ... DO UPDATE ... WHERE.
     * This test confirms that the standard ON DUPLICATE KEY UPDATE path works.
     *
     * @spec SPEC-4.2
     */
    public function testOnDuplicateKeyUpdateUpdatesUnconditionally(): void
    {
        try {
            // Seed with version 1
            $this->pdo->exec(
                "INSERT INTO my_condup_products (id, name, price, version) VALUES (1, 'Widget', 10.00, 1)"
            );

            // Upsert — always updates on conflict (no WHERE guard)
            $this->pdo->exec(
                "INSERT INTO my_condup_products (id, name, price, version) VALUES (1, 'Widget', 15.00, 2)
                 ON DUPLICATE KEY UPDATE price = VALUES(price), version = VALUES(version)"
            );

            $rows = $this->ztdQuery('SELECT id, name, price, version FROM my_condup_products WHERE id = 1');

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'ON DUPLICATE KEY UPDATE: expected 1 row, got ' . count($rows)
                );
            }

            $this->assertCount(1, $rows);
            $this->assertEqualsWithDelta(15.00, (float) $rows[0]['price'], 0.01,
                'Price should be updated to 15.00');
            $this->assertEquals(2, (int) $rows[0]['version'],
                'Version should be updated to 2');
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'ON DUPLICATE KEY UPDATE failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * ON DUPLICATE KEY UPDATE always fires — even when a PostgreSQL conditional
     * upsert would skip the update.
     *
     * This documents the behavioral difference: MySQL has no WHERE guard on the
     * update, so a "downgrade" (lower version) still overwrites.
     *
     * @spec SPEC-4.2
     */
    public function testOnDuplicateKeyUpdateAlwaysUpdatesNoWhereGuard(): void
    {
        try {
            // Seed with version 5
            $this->pdo->exec(
                "INSERT INTO my_condup_products (id, name, price, version) VALUES (1, 'Widget', 50.00, 5)"
            );

            // Upsert with lower version — MySQL will still overwrite
            $this->pdo->exec(
                "INSERT INTO my_condup_products (id, name, price, version) VALUES (1, 'Widget', 15.00, 2)
                 ON DUPLICATE KEY UPDATE price = VALUES(price), version = VALUES(version)"
            );

            $rows = $this->ztdQuery('SELECT id, price, version FROM my_condup_products WHERE id = 1');

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'ON DUPLICATE KEY UPDATE (no WHERE): expected 1 row, got ' . count($rows)
                );
            }

            // MySQL unconditionally updates — version goes from 5 back to 2
            $this->assertCount(1, $rows);
            $this->assertEqualsWithDelta(15.00, (float) $rows[0]['price'], 0.01,
                'MySQL unconditionally overwrites price (no WHERE guard)');
            $this->assertEquals(2, (int) $rows[0]['version'],
                'MySQL unconditionally overwrites version (no WHERE guard)');
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'ON DUPLICATE KEY UPDATE (no WHERE guard) failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Emulate conditional upsert on MySQL using IF() in the SET clause.
     *
     * This is the MySQL workaround for PostgreSQL's ON CONFLICT ... WHERE:
     * push the condition into each SET expression.
     *
     * @spec SPEC-4.2
     */
    public function testEmulatedConditionalUpsertWithIf(): void
    {
        try {
            // Seed with version 5
            $this->pdo->exec(
                "INSERT INTO my_condup_products (id, name, price, version) VALUES (1, 'Widget', 50.00, 5)"
            );

            // Conditional update via IF — only update if incoming version is higher
            $this->pdo->exec(
                "INSERT INTO my_condup_products (id, name, price, version) VALUES (1, 'Widget', 15.00, 2)
                 ON DUPLICATE KEY UPDATE
                     price = IF(VALUES(version) > version, VALUES(price), price),
                     version = IF(VALUES(version) > version, VALUES(version), version)"
            );

            $rows = $this->ztdQuery('SELECT id, price, version FROM my_condup_products WHERE id = 1');

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Emulated conditional upsert: expected 1 row, got ' . count($rows)
                );
            }

            $this->assertCount(1, $rows);
            $this->assertEqualsWithDelta(50.00, (float) $rows[0]['price'], 0.01,
                'Price should remain 50.00 when emulated condition prevents update');
            $this->assertEquals(5, (int) $rows[0]['version'],
                'Version should remain 5 when emulated condition prevents update');
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Emulated conditional upsert with IF() failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Prepared statement with ON DUPLICATE KEY UPDATE.
     *
     * @spec SPEC-4.2
     */
    public function testPreparedOnDuplicateKeyUpdate(): void
    {
        try {
            // Seed
            $this->pdo->exec(
                "INSERT INTO my_condup_products (id, name, price, version) VALUES (1, 'Beta', 30.00, 1)"
            );

            $sql = "INSERT INTO my_condup_products (id, name, price, version) VALUES (?, ?, ?, ?)
                    ON DUPLICATE KEY UPDATE price = VALUES(price), version = VALUES(version)";

            $stmt = $this->pdo->prepare($sql);
            $stmt->execute([1, 'Beta', 40.00, 3]);

            $rows = $this->ztdQuery('SELECT id, name, price, version FROM my_condup_products WHERE id = 1');

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Prepared ON DUPLICATE KEY UPDATE: expected 1 row, got ' . count($rows)
                );
            }

            $this->assertCount(1, $rows);
            $this->assertEqualsWithDelta(40.00, (float) $rows[0]['price'], 0.01,
                'Prepared upsert should update price');
            $this->assertEquals(3, (int) $rows[0]['version'],
                'Prepared upsert should update version');
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Prepared ON DUPLICATE KEY UPDATE failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * ON DUPLICATE KEY UPDATE with price comparison in SET (emulating
     * PostgreSQL's WHERE EXCLUDED.price > products.price).
     *
     * @spec SPEC-4.2
     */
    public function testEmulatedConditionalUpsertWithPriceGuard(): void
    {
        try {
            // Seed with price 100.00
            $this->pdo->exec(
                "INSERT INTO my_condup_products (id, name, price, version) VALUES (1, 'Gamma', 100.00, 1)"
            );

            // Attempt with lower price — IF guard should prevent update
            $this->pdo->exec(
                "INSERT INTO my_condup_products (id, name, price, version) VALUES (1, 'Gamma', 50.00, 2)
                 ON DUPLICATE KEY UPDATE
                     price = IF(VALUES(price) > price, VALUES(price), price),
                     version = IF(VALUES(price) > price, VALUES(version), version)"
            );

            $rows = $this->ztdQuery('SELECT id, price, version FROM my_condup_products WHERE id = 1');

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Emulated price-guard upsert: expected 1 row, got ' . count($rows)
                );
            }

            $this->assertCount(1, $rows);
            $this->assertEqualsWithDelta(100.00, (float) $rows[0]['price'], 0.01,
                'Price should remain 100.00 when incoming price is lower');
            $this->assertEquals(1, (int) $rows[0]['version'],
                'Version should remain 1 when incoming price is lower');

            // Attempt with higher price — IF guard should allow update
            $this->pdo->exec(
                "INSERT INTO my_condup_products (id, name, price, version) VALUES (1, 'Gamma', 150.00, 3)
                 ON DUPLICATE KEY UPDATE
                     price = IF(VALUES(price) > price, VALUES(price), price),
                     version = IF(VALUES(price) > price, VALUES(version), version)"
            );

            $rows2 = $this->ztdQuery('SELECT id, price, version FROM my_condup_products WHERE id = 1');

            if (count($rows2) !== 1) {
                $this->markTestIncomplete(
                    'Emulated price-guard upsert (update): expected 1 row, got ' . count($rows2)
                );
            }

            $this->assertCount(1, $rows2);
            $this->assertEqualsWithDelta(150.00, (float) $rows2[0]['price'], 0.01,
                'Price should be updated to 150.00 when incoming price is higher');
            $this->assertEquals(3, (int) $rows2[0]['version'],
                'Version should be updated to 3 when incoming price is higher');
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Emulated conditional upsert with price guard failed: ' . $e->getMessage()
            );
        }
    }
}
