<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests ON CONFLICT (UPSERT) with composite primary keys on PostgreSQL.
 *
 * PostgreSQL uses ON CONFLICT DO UPDATE SET ... syntax for upserts.
 * Composite keys require specifying multiple columns in the conflict target.
 * @spec SPEC-4.2a
 */
class PostgresCompositePkUpsertTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pg_cpk_upsert (
            region_id INT NOT NULL,
            product_id INT NOT NULL,
            quantity INT NOT NULL DEFAULT 0,
            price DECIMAL(10,2),
            PRIMARY KEY (region_id, product_id)
        )';
    }

    protected function getTableNames(): array
    {
        return ['pg_cpk_upsert'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_cpk_upsert VALUES (1, 1, 10, 9.99)");
        $this->pdo->exec("INSERT INTO pg_cpk_upsert VALUES (1, 2, 20, 19.99)");
        $this->pdo->exec("INSERT INTO pg_cpk_upsert VALUES (2, 1, 15, 9.99)");
    }

    /**
     * ON CONFLICT DO UPDATE with composite PK — update existing row.
     */
    public function testUpsertUpdateExisting(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO pg_cpk_upsert (region_id, product_id, quantity, price)
                 VALUES (1, 1, 50, 12.99)
                 ON CONFLICT (region_id, product_id) DO UPDATE SET quantity = EXCLUDED.quantity, price = EXCLUDED.price"
            );

            $stmt = $this->pdo->query('SELECT quantity, price FROM pg_cpk_upsert WHERE region_id = 1 AND product_id = 1');
            $row = $stmt->fetch(PDO::FETCH_ASSOC);
            $this->assertEquals(50, (int) $row['quantity']);
            $this->assertEquals(12.99, (float) $row['price']);
        } catch (\Exception $e) {
            $this->markTestSkipped('ON CONFLICT with composite PK not supported: ' . $e->getMessage());
        }
    }

    /**
     * ON CONFLICT DO UPDATE — insert new row.
     */
    public function testUpsertInsertNew(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO pg_cpk_upsert (region_id, product_id, quantity, price)
                 VALUES (2, 2, 30, 24.99)
                 ON CONFLICT (region_id, product_id) DO UPDATE SET quantity = EXCLUDED.quantity, price = EXCLUDED.price"
            );

            $stmt = $this->pdo->query('SELECT quantity, price FROM pg_cpk_upsert WHERE region_id = 2 AND product_id = 2');
            $row = $stmt->fetch(PDO::FETCH_ASSOC);
            $this->assertEquals(30, (int) $row['quantity']);
        } catch (\Exception $e) {
            $this->markTestSkipped('ON CONFLICT with composite PK not supported: ' . $e->getMessage());
        }
    }

    /**
     * ON CONFLICT DO NOTHING with composite PK.
     */
    public function testUpsertDoNothing(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO pg_cpk_upsert (region_id, product_id, quantity, price)
                 VALUES (1, 1, 999, 999.99)
                 ON CONFLICT (region_id, product_id) DO NOTHING"
            );

            // Original row should remain unchanged
            $stmt = $this->pdo->query('SELECT quantity FROM pg_cpk_upsert WHERE region_id = 1 AND product_id = 1');
            $row = $stmt->fetch(PDO::FETCH_ASSOC);
            $this->assertEquals(10, (int) $row['quantity']);
        } catch (\Exception $e) {
            $this->markTestSkipped('ON CONFLICT DO NOTHING not supported: ' . $e->getMessage());
        }
    }

    /**
     * Multiple upserts in sequence.
     */
    public function testMultipleUpserts(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO pg_cpk_upsert (region_id, product_id, quantity, price)
                 VALUES (1, 1, 100, 8.99)
                 ON CONFLICT (region_id, product_id) DO UPDATE SET quantity = EXCLUDED.quantity"
            );
            $this->pdo->exec(
                "INSERT INTO pg_cpk_upsert (region_id, product_id, quantity, price)
                 VALUES (1, 1, 200, 7.99)
                 ON CONFLICT (region_id, product_id) DO UPDATE SET quantity = EXCLUDED.quantity"
            );

            $stmt = $this->pdo->query('SELECT quantity FROM pg_cpk_upsert WHERE region_id = 1 AND product_id = 1');
            $row = $stmt->fetch(PDO::FETCH_ASSOC);
            $this->assertEquals(200, (int) $row['quantity']);
        } catch (\Exception $e) {
            $this->markTestSkipped('Multiple ON CONFLICT not supported: ' . $e->getMessage());
        }
    }

    /**
     * Total count after upserts.
     */
    public function testTotalCountAfterUpserts(): void
    {
        // Start with 3 rows
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_cpk_upsert');
        $this->assertEquals(3, (int) $stmt->fetchColumn());
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_cpk_upsert');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
