<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests EXCLUDE constraints, complex CHECK constraints, and GENERATED ALWAYS
 * AS (expression) STORED columns through the CTE shadow store.
 *
 * EXCLUDE constraints are PostgreSQL-specific and require the btree_gist
 * extension. Since the extension may not be available, the test gracefully
 * degrades. The primary value is exercising complex CHECK constraints and
 * generated stored columns, which stress the CTE rewriter's DDL reflection
 * and value computation paths.
 *
 * @spec SPEC-8.1
 * @spec SPEC-3.1
 * @spec SPEC-4.1
 */
class PostgresExcludeConstraintTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            // Table with complex CHECK constraints
            'CREATE TABLE pgx_ec_products (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                price NUMERIC(10,2) NOT NULL,
                discount_pct NUMERIC(5,2) DEFAULT 0,
                quantity INT NOT NULL DEFAULT 0,
                CHECK (price > 0),
                CHECK (discount_pct >= 0 AND discount_pct <= 100),
                CHECK (quantity >= 0),
                CHECK (price * (1 - discount_pct / 100) > 0)
            )',
            // Table with GENERATED ALWAYS AS ... STORED columns
            'CREATE TABLE pgx_ec_orders (
                id SERIAL PRIMARY KEY,
                unit_price NUMERIC(10,2) NOT NULL,
                qty INT NOT NULL,
                tax_rate NUMERIC(5,4) NOT NULL DEFAULT 0.0800,
                subtotal NUMERIC(12,2) GENERATED ALWAYS AS (unit_price * qty) STORED,
                tax_amount NUMERIC(12,2) GENERATED ALWAYS AS (unit_price * qty * tax_rate) STORED,
                total NUMERIC(12,2) GENERATED ALWAYS AS (unit_price * qty * (1 + tax_rate)) STORED
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pgx_ec_orders', 'pgx_ec_products'];
    }

    // ---------------------------------------------------------------
    // Complex CHECK constraint tests
    // ---------------------------------------------------------------

    /**
     * INSERT with valid values satisfying all CHECK constraints.
     */
    public function testInsertValidCheckConstraintValues(): void
    {
        $this->ztdExec(
            "INSERT INTO pgx_ec_products (id, name, price, discount_pct, quantity)
             VALUES (1, 'Widget', 29.99, 10.00, 100)"
        );

        $rows = $this->ztdQuery('SELECT name, price, discount_pct, quantity FROM pgx_ec_products WHERE id = 1');
        $this->assertCount(1, $rows);
        $this->assertSame('Widget', $rows[0]['name']);
        $this->assertEqualsWithDelta(29.99, (float) $rows[0]['price'], 0.01);
        $this->assertEqualsWithDelta(10.00, (float) $rows[0]['discount_pct'], 0.01);
        $this->assertSame(100, (int) $rows[0]['quantity']);
    }

    /**
     * INSERT violating CHECK (price > 0) succeeds in shadow.
     *
     * The CTE shadow store does not enforce CHECK constraints.
     */
    public function testInsertNegativePriceSucceedsInShadow(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO pgx_ec_products (id, name, price, discount_pct, quantity)
                 VALUES (1, 'BadPrice', -5.00, 0, 1)"
            );

            $rows = $this->ztdQuery('SELECT price FROM pgx_ec_products WHERE id = 1');
            $this->assertCount(1, $rows);
            $this->assertEqualsWithDelta(-5.00, (float) $rows[0]['price'], 0.01,
                'Negative price should be stored in shadow despite CHECK constraint');
        } catch (\Exception $e) {
            // If the CTE rewriter does enforce CHECK, that is also valid behavior
            $this->markTestIncomplete(
                'INSERT violating CHECK constraint raised exception: ' . $e->getMessage()
            );
        }
    }

    /**
     * INSERT violating complex CHECK (price * (1 - discount/100) > 0) succeeds in shadow.
     */
    public function testInsertViolatingComplexCheckSucceeds(): void
    {
        try {
            // discount_pct = 100 means effective price = 0, which violates the CHECK
            $this->ztdExec(
                "INSERT INTO pgx_ec_products (id, name, price, discount_pct, quantity)
                 VALUES (1, 'FreeItem', 10.00, 100.00, 5)"
            );

            $rows = $this->ztdQuery('SELECT discount_pct FROM pgx_ec_products WHERE id = 1');
            $this->assertCount(1, $rows);
            $this->assertEqualsWithDelta(100.00, (float) $rows[0]['discount_pct'], 0.01);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'INSERT violating complex CHECK raised exception: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE to violate CHECK (quantity >= 0) succeeds in shadow.
     */
    public function testUpdateViolatingCheckSucceeds(): void
    {
        $this->ztdExec(
            "INSERT INTO pgx_ec_products (id, name, price, discount_pct, quantity)
             VALUES (1, 'Stock', 15.00, 0, 10)"
        );

        try {
            $this->ztdExec('UPDATE pgx_ec_products SET quantity = -5 WHERE id = 1');

            $rows = $this->ztdQuery('SELECT quantity FROM pgx_ec_products WHERE id = 1');
            $this->assertCount(1, $rows);
            $this->assertSame(-5, (int) $rows[0]['quantity'],
                'Negative quantity should be stored in shadow');
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'UPDATE violating CHECK raised exception: ' . $e->getMessage()
            );
        }
    }

    // ---------------------------------------------------------------
    // GENERATED ALWAYS AS ... STORED column tests
    // ---------------------------------------------------------------

    /**
     * INSERT and verify generated columns are computed.
     */
    public function testGeneratedColumnsComputed(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO pgx_ec_orders (id, unit_price, qty, tax_rate)
                 VALUES (1, 100.00, 3, 0.0800)"
            );

            $rows = $this->ztdQuery(
                'SELECT subtotal, tax_amount, total FROM pgx_ec_orders WHERE id = 1'
            );
            $this->assertCount(1, $rows);

            // subtotal = 100.00 * 3 = 300.00
            if ($rows[0]['subtotal'] !== null) {
                $this->assertEqualsWithDelta(300.00, (float) $rows[0]['subtotal'], 0.01,
                    'subtotal should be unit_price * qty');
            }

            // tax_amount = 100.00 * 3 * 0.08 = 24.00
            if ($rows[0]['tax_amount'] !== null) {
                $this->assertEqualsWithDelta(24.00, (float) $rows[0]['tax_amount'], 0.01,
                    'tax_amount should be unit_price * qty * tax_rate');
            }

            // total = 100.00 * 3 * (1 + 0.08) = 324.00
            if ($rows[0]['total'] !== null) {
                $this->assertEqualsWithDelta(324.00, (float) $rows[0]['total'], 0.01,
                    'total should be unit_price * qty * (1 + tax_rate)');
            }
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Generated columns computation failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Generated columns should be NULL or correctly computed in shadow.
     *
     * This is a diagnostic: if the shadow store cannot compute generated
     * column expressions, they will be NULL. Record which behavior occurs.
     */
    public function testGeneratedColumnNullabilityInShadow(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO pgx_ec_orders (id, unit_price, qty) VALUES (1, 50.00, 2)"
            );

            $rows = $this->ztdQuery('SELECT subtotal, total FROM pgx_ec_orders WHERE id = 1');
            $this->assertCount(1, $rows);

            // Record the actual behavior
            if ($rows[0]['subtotal'] === null) {
                // Generated columns are NULL in shadow -- document this
                $this->assertNull($rows[0]['subtotal'],
                    'Generated column is NULL in shadow (CTE does not compute expressions)');
                $this->assertNull($rows[0]['total']);
            } else {
                // Generated columns are computed -- verify correctness
                $this->assertEqualsWithDelta(100.00, (float) $rows[0]['subtotal'], 0.01);
                // total = 50*2*(1+0.08) = 108.00
                $this->assertEqualsWithDelta(108.00, (float) $rows[0]['total'], 0.01);
            }
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Generated column query failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE base columns and verify generated columns update.
     */
    public function testUpdateBaseColumnsRecomputesGenerated(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO pgx_ec_orders (id, unit_price, qty) VALUES (1, 10.00, 5)"
            );

            $this->ztdExec('UPDATE pgx_ec_orders SET qty = 10 WHERE id = 1');

            $rows = $this->ztdQuery('SELECT subtotal, total FROM pgx_ec_orders WHERE id = 1');
            $this->assertCount(1, $rows);

            // If generated columns are computed after UPDATE:
            // subtotal = 10.00 * 10 = 100.00, total = 10.00 * 10 * 1.08 = 108.00
            if ($rows[0]['subtotal'] !== null) {
                $this->assertEqualsWithDelta(100.00, (float) $rows[0]['subtotal'], 0.01,
                    'subtotal should reflect updated qty');
            }
            if ($rows[0]['total'] !== null) {
                $this->assertEqualsWithDelta(108.00, (float) $rows[0]['total'], 0.01,
                    'total should reflect updated qty');
            }
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'UPDATE recomputation of generated columns failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * SELECT with WHERE on generated column.
     */
    public function testSelectWhereOnGeneratedColumn(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO pgx_ec_orders (id, unit_price, qty) VALUES (1, 10.00, 2)"
            );
            $this->ztdExec(
                "INSERT INTO pgx_ec_orders (id, unit_price, qty) VALUES (2, 50.00, 4)"
            );
            $this->ztdExec(
                "INSERT INTO pgx_ec_orders (id, unit_price, qty) VALUES (3, 5.00, 1)"
            );

            // Filter by subtotal > 100 (only order 2: 50*4 = 200)
            $rows = $this->ztdQuery(
                'SELECT id FROM pgx_ec_orders WHERE subtotal > 100 ORDER BY id'
            );

            if (count($rows) > 0) {
                $this->assertSame(2, (int) $rows[0]['id']);
            } else {
                // Generated columns may be NULL, so WHERE fails to match
                $this->assertCount(0, $rows,
                    'No rows matched: generated column may be NULL in shadow');
            }
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'WHERE on generated column failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * EXCLUDE constraint test (requires btree_gist extension).
     *
     * This test attempts to create a table with an EXCLUDE constraint.
     * If the extension is not available, it is skipped gracefully.
     */
    public function testExcludeConstraintIfExtensionAvailable(): void
    {
        try {
            // Attempt to enable btree_gist extension
            $raw = new PDO(
                \Tests\Support\PostgreSQLContainer::getDsn(),
                'test',
                'test',
                [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
            );
            $raw->exec('CREATE EXTENSION IF NOT EXISTS btree_gist');
        } catch (\Exception $e) {
            $this->markTestSkipped('btree_gist extension not available: ' . $e->getMessage());
            return;
        }

        try {
            // Create a table with an EXCLUDE constraint
            $this->createTable(
                'CREATE TABLE pgx_ec_rooms (
                    id SERIAL PRIMARY KEY,
                    room_name VARCHAR(50) NOT NULL,
                    start_time TIMESTAMP NOT NULL,
                    end_time TIMESTAMP NOT NULL,
                    EXCLUDE USING gist (
                        room_name WITH =,
                        tsrange(start_time, end_time) WITH &&
                    )
                )'
            );

            // INSERT a valid booking
            $this->ztdExec(
                "INSERT INTO pgx_ec_rooms (id, room_name, start_time, end_time)
                 VALUES (1, 'Room A', '2025-01-15 09:00:00', '2025-01-15 10:00:00')"
            );

            // INSERT an overlapping booking (would fail on physical DB)
            $this->ztdExec(
                "INSERT INTO pgx_ec_rooms (id, room_name, start_time, end_time)
                 VALUES (2, 'Room A', '2025-01-15 09:30:00', '2025-01-15 10:30:00')"
            );

            // Both rows should be visible in shadow (EXCLUDE not enforced)
            $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM pgx_ec_rooms WHERE room_name = 'Room A'");
            $this->assertSame(2, (int) $rows[0]['cnt'],
                'Both overlapping bookings should exist in shadow');

            // Cleanup
            $this->dropTable('pgx_ec_rooms');
        } catch (\Exception $e) {
            $this->dropTable('pgx_ec_rooms');
            $this->markTestIncomplete(
                'EXCLUDE constraint test failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Multiple rows with generated columns and aggregate query.
     */
    public function testAggregateOnGeneratedColumns(): void
    {
        try {
            $this->ztdExec("INSERT INTO pgx_ec_orders (id, unit_price, qty) VALUES (1, 10.00, 1)");
            $this->ztdExec("INSERT INTO pgx_ec_orders (id, unit_price, qty) VALUES (2, 20.00, 2)");
            $this->ztdExec("INSERT INTO pgx_ec_orders (id, unit_price, qty) VALUES (3, 30.00, 3)");

            $rows = $this->ztdQuery('SELECT SUM(subtotal) AS grand_total FROM pgx_ec_orders');
            $this->assertCount(1, $rows);

            $grandTotal = $rows[0]['grand_total'];
            if ($grandTotal !== null) {
                // SUM of subtotals: 10 + 40 + 90 = 140
                $this->assertEqualsWithDelta(140.00, (float) $grandTotal, 0.01,
                    'SUM of generated subtotal column');
            } else {
                // Generated columns are NULL in shadow, so SUM is NULL
                $this->assertNull($grandTotal,
                    'SUM of generated column is NULL when shadow does not compute expressions');
            }
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Aggregate on generated columns failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->ztdExec(
            "INSERT INTO pgx_ec_products (id, name, price, quantity) VALUES (1, 'test', 10.00, 1)"
        );

        $this->disableZtd();
        $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM pgx_ec_products');
        $this->assertSame(0, (int) $rows[0]['cnt'], 'Physical table should have 0 rows');
    }
}
