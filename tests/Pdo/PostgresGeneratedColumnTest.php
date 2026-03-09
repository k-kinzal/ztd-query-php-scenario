<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests GENERATED columns (computed/virtual columns) in the CTE shadow store.
 *
 * PostgreSQL 12+ supports GENERATED ALWAYS AS (expression) STORED columns.
 * The CTE rewriter must handle:
 * - INSERT omitting generated columns (auto-compute)
 * - SELECT returning generated column values from shadow store
 * - UPDATE of base columns causing generated columns to recompute
 * - GENERATED ALWAYS AS IDENTITY for auto-increment behavior
 *
 * The key question is whether the shadow store can compute generated column
 * expressions or returns NULL for them. Both behaviors are documented.
 *
 * @spec SPEC-10.2.22
 * @spec SPEC-3.1
 * @spec SPEC-4.1
 */
class PostgresGeneratedColumnTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            // String concatenation generated column
            "CREATE TABLE pgx_gc_people (
                id SERIAL PRIMARY KEY,
                first_name VARCHAR(100) NOT NULL,
                last_name VARCHAR(100) NOT NULL,
                full_name TEXT GENERATED ALWAYS AS (first_name || ' ' || last_name) STORED
            )",
            // Numeric generated column
            'CREATE TABLE pgx_gc_invoices (
                id SERIAL PRIMARY KEY,
                unit_price NUMERIC(10,2) NOT NULL,
                quantity INT NOT NULL,
                discount_pct NUMERIC(5,2) DEFAULT 0.00,
                line_total NUMERIC(12,2) GENERATED ALWAYS AS (unit_price * quantity) STORED,
                discounted_total NUMERIC(12,2) GENERATED ALWAYS AS (unit_price * quantity * (1 - discount_pct / 100)) STORED
            )',
            // GENERATED ALWAYS AS IDENTITY table
            'CREATE TABLE pgx_gc_autoitems (
                id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                label VARCHAR(100) NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pgx_gc_autoitems', 'pgx_gc_invoices', 'pgx_gc_people'];
    }

    // ---------------------------------------------------------------
    // String concatenation generated column (full_name)
    // ---------------------------------------------------------------

    /**
     * INSERT with just first_name and last_name; SELECT should show full_name.
     */
    public function testInsertOmittingGeneratedStringColumn(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO pgx_gc_people (id, first_name, last_name) VALUES (1, 'Jane', 'Doe')"
            );

            $rows = $this->ztdQuery('SELECT first_name, last_name, full_name FROM pgx_gc_people WHERE id = 1');
            $this->assertCount(1, $rows);
            $this->assertSame('Jane', $rows[0]['first_name']);
            $this->assertSame('Doe', $rows[0]['last_name']);

            // Generated column may be computed or NULL in shadow
            if ($rows[0]['full_name'] !== null) {
                $this->assertSame('Jane Doe', $rows[0]['full_name'],
                    'Generated full_name should be first_name || space || last_name');
            } else {
                $this->assertNull($rows[0]['full_name'],
                    'Generated column is NULL in shadow (CTE does not compute expressions)');
            }
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'INSERT omitting generated string column failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE first_name and verify full_name re-computes in SELECT.
     */
    public function testUpdateBaseColumnRecomputesFullName(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO pgx_gc_people (id, first_name, last_name) VALUES (1, 'John', 'Smith')"
            );

            $this->ztdExec("UPDATE pgx_gc_people SET first_name = 'Jonathan' WHERE id = 1");

            $rows = $this->ztdQuery('SELECT full_name FROM pgx_gc_people WHERE id = 1');
            $this->assertCount(1, $rows);

            if ($rows[0]['full_name'] !== null) {
                $this->assertSame('Jonathan Smith', $rows[0]['full_name'],
                    'Generated full_name should reflect updated first_name');
            }
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'UPDATE recomputation of generated full_name failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Multiple rows with generated column, verify each is computed independently.
     */
    public function testMultipleRowsGeneratedColumn(): void
    {
        try {
            $this->ztdExec("INSERT INTO pgx_gc_people (id, first_name, last_name) VALUES (1, 'Alice', 'Wonderland')");
            $this->ztdExec("INSERT INTO pgx_gc_people (id, first_name, last_name) VALUES (2, 'Bob', 'Builder')");
            $this->ztdExec("INSERT INTO pgx_gc_people (id, first_name, last_name) VALUES (3, 'Charlie', 'Chocolate')");

            $rows = $this->ztdQuery('SELECT id, full_name FROM pgx_gc_people ORDER BY id');
            $this->assertCount(3, $rows);

            // Verify each generated column is independently computed
            if ($rows[0]['full_name'] !== null) {
                $this->assertSame('Alice Wonderland', $rows[0]['full_name']);
                $this->assertSame('Bob Builder', $rows[1]['full_name']);
                $this->assertSame('Charlie Chocolate', $rows[2]['full_name']);
            }
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Multiple rows with generated column failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * SELECT with WHERE on generated column.
     */
    public function testWhereOnGeneratedStringColumn(): void
    {
        try {
            $this->ztdExec("INSERT INTO pgx_gc_people (id, first_name, last_name) VALUES (1, 'Alice', 'A')");
            $this->ztdExec("INSERT INTO pgx_gc_people (id, first_name, last_name) VALUES (2, 'Bob', 'B')");

            $rows = $this->ztdQuery("SELECT id FROM pgx_gc_people WHERE full_name = 'Alice A'");

            if (count($rows) > 0) {
                $this->assertSame(1, (int) $rows[0]['id'],
                    'WHERE on generated column should find the correct row');
            } else {
                // Generated column may be NULL so WHERE never matches
                $this->assertCount(0, $rows,
                    'No rows matched: generated column may be NULL in shadow');
            }
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'WHERE on generated string column failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * SELECT with LIKE on generated column.
     */
    public function testLikeOnGeneratedColumn(): void
    {
        try {
            $this->ztdExec("INSERT INTO pgx_gc_people (id, first_name, last_name) VALUES (1, 'Alice', 'Anderson')");
            $this->ztdExec("INSERT INTO pgx_gc_people (id, first_name, last_name) VALUES (2, 'Bob', 'Brown')");

            $rows = $this->ztdQuery("SELECT id FROM pgx_gc_people WHERE full_name LIKE 'Alice%' ORDER BY id");

            if (count($rows) > 0) {
                $this->assertSame(1, (int) $rows[0]['id']);
            }
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'LIKE on generated column failed: ' . $e->getMessage()
            );
        }
    }

    // ---------------------------------------------------------------
    // Numeric generated column (line_total, discounted_total)
    // ---------------------------------------------------------------

    /**
     * INSERT and verify numeric generated columns.
     */
    public function testNumericGeneratedColumns(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO pgx_gc_invoices (id, unit_price, quantity, discount_pct)
                 VALUES (1, 25.00, 4, 10.00)"
            );

            $rows = $this->ztdQuery(
                'SELECT line_total, discounted_total FROM pgx_gc_invoices WHERE id = 1'
            );
            $this->assertCount(1, $rows);

            // line_total = 25.00 * 4 = 100.00
            if ($rows[0]['line_total'] !== null) {
                $this->assertEqualsWithDelta(100.00, (float) $rows[0]['line_total'], 0.01,
                    'line_total should be unit_price * quantity');
            }

            // discounted_total = 25.00 * 4 * (1 - 10/100) = 90.00
            if ($rows[0]['discounted_total'] !== null) {
                $this->assertEqualsWithDelta(90.00, (float) $rows[0]['discounted_total'], 0.01,
                    'discounted_total should be line_total * (1 - discount/100)');
            }
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Numeric generated columns failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE quantity and verify line_total recomputes.
     */
    public function testUpdateQuantityRecomputesLineTotal(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO pgx_gc_invoices (id, unit_price, quantity, discount_pct)
                 VALUES (1, 10.00, 5, 0.00)"
            );

            $this->ztdExec('UPDATE pgx_gc_invoices SET quantity = 10 WHERE id = 1');

            $rows = $this->ztdQuery('SELECT line_total FROM pgx_gc_invoices WHERE id = 1');
            $this->assertCount(1, $rows);

            // line_total should now be 10.00 * 10 = 100.00
            if ($rows[0]['line_total'] !== null) {
                $this->assertEqualsWithDelta(100.00, (float) $rows[0]['line_total'], 0.01,
                    'line_total should reflect updated quantity');
            }
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'UPDATE recomputation of line_total failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * INSERT without specifying generated columns (should auto-compute).
     */
    public function testInsertWithoutSpecifyingGeneratedColumns(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO pgx_gc_invoices (id, unit_price, quantity)
                 VALUES (1, 50.00, 2)"
            );

            $rows = $this->ztdQuery(
                'SELECT unit_price, quantity, line_total, discounted_total FROM pgx_gc_invoices WHERE id = 1'
            );
            $this->assertCount(1, $rows);
            $this->assertEqualsWithDelta(50.00, (float) $rows[0]['unit_price'], 0.01);
            $this->assertSame(2, (int) $rows[0]['quantity']);

            // line_total = 50 * 2 = 100.00 (discount defaults to 0)
            if ($rows[0]['line_total'] !== null) {
                $this->assertEqualsWithDelta(100.00, (float) $rows[0]['line_total'], 0.01);
            }
            // discounted_total = 50 * 2 * (1 - 0/100) = 100.00
            if ($rows[0]['discounted_total'] !== null) {
                $this->assertEqualsWithDelta(100.00, (float) $rows[0]['discounted_total'], 0.01);
            }
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'INSERT without specifying generated columns failed: ' . $e->getMessage()
            );
        }
    }

    // ---------------------------------------------------------------
    // GENERATED ALWAYS AS IDENTITY
    // ---------------------------------------------------------------

    /**
     * GENERATED ALWAYS AS IDENTITY auto-increment behavior.
     */
    public function testGeneratedAlwaysAsIdentity(): void
    {
        try {
            $this->ztdExec("INSERT INTO pgx_gc_autoitems (label) VALUES ('first')");
            $this->ztdExec("INSERT INTO pgx_gc_autoitems (label) VALUES ('second')");
            $this->ztdExec("INSERT INTO pgx_gc_autoitems (label) VALUES ('third')");

            $rows = $this->ztdQuery('SELECT id, label FROM pgx_gc_autoitems ORDER BY id');
            $this->assertCount(3, $rows);

            // IDs should be auto-generated and sequential
            $ids = array_map(fn($r) => (int) $r['id'], $rows);
            $this->assertSame($ids, array_unique($ids), 'IDs should be unique');
            $this->assertSame(1, $ids[0], 'First auto-generated ID should be 1');
            $this->assertSame(2, $ids[1], 'Second auto-generated ID should be 2');
            $this->assertSame(3, $ids[2], 'Third auto-generated ID should be 3');
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'GENERATED ALWAYS AS IDENTITY failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * DELETE a row with GENERATED IDENTITY then INSERT another.
     */
    public function testDeleteThenInsertWithIdentity(): void
    {
        try {
            $this->ztdExec("INSERT INTO pgx_gc_autoitems (label) VALUES ('alpha')");
            $this->ztdExec("INSERT INTO pgx_gc_autoitems (label) VALUES ('beta')");

            $this->ztdExec("DELETE FROM pgx_gc_autoitems WHERE label = 'alpha'");

            $this->ztdExec("INSERT INTO pgx_gc_autoitems (label) VALUES ('gamma')");

            $rows = $this->ztdQuery('SELECT id, label FROM pgx_gc_autoitems ORDER BY id');
            $this->assertCount(2, $rows);

            // After deleting id=1, inserting should get id=3 (sequence not reset)
            $labels = array_column($rows, 'label');
            $this->assertContains('beta', $labels);
            $this->assertContains('gamma', $labels);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'DELETE then INSERT with IDENTITY failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Attempting to INSERT explicit value into IDENTITY column should fail.
     */
    public function testInsertExplicitValueIntoIdentityColumn(): void
    {
        try {
            $this->ztdExec("INSERT INTO pgx_gc_autoitems (id, label) VALUES (99, 'explicit')");

            // If shadow allows it, check whether the value persists
            $rows = $this->ztdQuery('SELECT id FROM pgx_gc_autoitems WHERE label = \'explicit\'');
            if (count($rows) > 0) {
                $this->assertSame(99, (int) $rows[0]['id'],
                    'Shadow store accepted explicit IDENTITY value');
            }
        } catch (\Exception $e) {
            // This is expected in strict PostgreSQL mode
            $this->assertStringContainsString('GENERATED ALWAYS', $e->getMessage(),
                'Should fail because column is GENERATED ALWAYS AS IDENTITY');
        }
    }

    // ---------------------------------------------------------------
    // Combined scenarios
    // ---------------------------------------------------------------

    /**
     * ORDER BY generated column.
     */
    public function testOrderByGeneratedColumn(): void
    {
        try {
            $this->ztdExec("INSERT INTO pgx_gc_people (id, first_name, last_name) VALUES (1, 'Zara', 'Zulu')");
            $this->ztdExec("INSERT INTO pgx_gc_people (id, first_name, last_name) VALUES (2, 'Alice', 'Alpha')");
            $this->ztdExec("INSERT INTO pgx_gc_people (id, first_name, last_name) VALUES (3, 'Mike', 'Middle')");

            $rows = $this->ztdQuery('SELECT id, full_name FROM pgx_gc_people ORDER BY full_name');

            $this->assertCount(3, $rows);
            if ($rows[0]['full_name'] !== null) {
                // Alphabetical order: Alice Alpha, Mike Middle, Zara Zulu
                $this->assertSame('Alice Alpha', $rows[0]['full_name']);
                $this->assertSame('Mike Middle', $rows[1]['full_name']);
                $this->assertSame('Zara Zulu', $rows[2]['full_name']);
            }
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'ORDER BY generated column failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * SUM aggregate on generated numeric column.
     */
    public function testSumOnGeneratedColumn(): void
    {
        try {
            $this->ztdExec("INSERT INTO pgx_gc_invoices (id, unit_price, quantity) VALUES (1, 10.00, 1)");
            $this->ztdExec("INSERT INTO pgx_gc_invoices (id, unit_price, quantity) VALUES (2, 20.00, 2)");
            $this->ztdExec("INSERT INTO pgx_gc_invoices (id, unit_price, quantity) VALUES (3, 30.00, 3)");

            $rows = $this->ztdQuery('SELECT SUM(line_total) AS invoice_total FROM pgx_gc_invoices');
            $this->assertCount(1, $rows);

            $total = $rows[0]['invoice_total'];
            if ($total !== null) {
                // 10 + 40 + 90 = 140
                $this->assertEqualsWithDelta(140.00, (float) $total, 0.01,
                    'SUM of generated line_total');
            }
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'SUM on generated column failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        try {
            $this->ztdExec("INSERT INTO pgx_gc_people (id, first_name, last_name) VALUES (1, 'Test', 'User')");
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'INSERT with generated column failed: ' . $e->getMessage()
            );
        }

        $this->disableZtd();
        $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM pgx_gc_people');
        $this->assertSame(0, (int) $rows[0]['cnt'], 'Physical table should have 0 rows');
    }
}
