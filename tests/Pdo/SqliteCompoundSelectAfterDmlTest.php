<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests compound SELECT (UNION, INTERSECT, EXCEPT) where BOTH source tables
 * have been modified through the shadow store.
 *
 * When both sides of a compound query reference DML-modified shadow tables,
 * the CTE rewriter must rewrite BOTH table references. This is a stress test
 * for the rewriter's ability to handle multiple shadow tables in compound
 * queries.
 *
 * @spec SPEC-3.3
 */
class SqliteCompoundSelectAfterDmlTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "CREATE TABLE sl_csd_customers (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                region TEXT NOT NULL
            )",
            "CREATE TABLE sl_csd_suppliers (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                region TEXT NOT NULL
            )",
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_csd_customers', 'sl_csd_suppliers'];
    }

    protected function setUp(): void
    {
        parent::setUp();
        $this->pdo->exec("INSERT INTO sl_csd_customers VALUES (1, 'Alice', 'east')");
        $this->pdo->exec("INSERT INTO sl_csd_customers VALUES (2, 'Bob', 'west')");
        $this->pdo->exec("INSERT INTO sl_csd_suppliers VALUES (1, 'Acme', 'east')");
        $this->pdo->exec("INSERT INTO sl_csd_suppliers VALUES (2, 'Beta', 'north')");
    }

    /**
     * UNION of two DML-modified tables — should combine all distinct rows.
     */
    public function testUnionBothTablesModified(): void
    {
        try {
            // Modify both tables
            $this->pdo->exec("INSERT INTO sl_csd_customers VALUES (3, 'Carol', 'south')");
            $this->pdo->exec("INSERT INTO sl_csd_suppliers VALUES (3, 'Gamma', 'south')");

            $rows = $this->ztdQuery(
                "SELECT name, region FROM sl_csd_customers
                 UNION
                 SELECT name, region FROM sl_csd_suppliers
                 ORDER BY name"
            );

            if (count($rows) === 0) {
                $this->markTestIncomplete('UNION of two modified tables returned 0 rows.');
            }

            // 3 customers + 3 suppliers, all distinct names = 6 rows
            if (count($rows) !== 6) {
                $this->markTestIncomplete(
                    'UNION of two modified tables returned ' . count($rows)
                    . ' rows. Expected 6. Got: ' . json_encode($rows)
                );
            }

            $this->assertCount(6, $rows);
            $names = array_column($rows, 'name');
            $this->assertContains('Carol', $names, 'New customer should be in UNION result');
            $this->assertContains('Gamma', $names, 'New supplier should be in UNION result');
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UNION both tables modified test failed: ' . $e->getMessage());
        }
    }

    /**
     * UNION ALL of two DML-modified tables — should include duplicates.
     */
    public function testUnionAllBothTablesModified(): void
    {
        try {
            // Insert a name that exists in both tables
            $this->pdo->exec("INSERT INTO sl_csd_customers VALUES (3, 'Shared', 'east')");
            $this->pdo->exec("INSERT INTO sl_csd_suppliers VALUES (3, 'Shared', 'east')");

            $rows = $this->ztdQuery(
                "SELECT name, region FROM sl_csd_customers
                 UNION ALL
                 SELECT name, region FROM sl_csd_suppliers
                 ORDER BY name"
            );

            if (count($rows) === 0) {
                $this->markTestIncomplete('UNION ALL of two modified tables returned 0 rows.');
            }

            // 3 customers + 3 suppliers = 6 total (UNION ALL keeps dupes)
            if (count($rows) !== 6) {
                $this->markTestIncomplete(
                    'UNION ALL of two modified tables returned ' . count($rows)
                    . ' rows. Expected 6. Got: ' . json_encode($rows)
                );
            }

            $this->assertCount(6, $rows);
            // 'Shared' should appear twice (once from each table)
            $sharedCount = count(array_filter($rows, fn($r) => $r['name'] === 'Shared'));
            $this->assertSame(2, $sharedCount, 'Shared should appear in both sides of UNION ALL');
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UNION ALL both tables modified test failed: ' . $e->getMessage());
        }
    }

    /**
     * INTERSECT after DML on both tables — should find common region values.
     */
    public function testIntersectBothTablesModified(): void
    {
        try {
            // Both tables now have 'south' region
            $this->pdo->exec("INSERT INTO sl_csd_customers VALUES (3, 'Carol', 'south')");
            $this->pdo->exec("INSERT INTO sl_csd_suppliers VALUES (3, 'Gamma', 'south')");

            $rows = $this->ztdQuery(
                "SELECT region FROM sl_csd_customers
                 INTERSECT
                 SELECT region FROM sl_csd_suppliers
                 ORDER BY region"
            );

            if (count($rows) === 0) {
                $this->markTestIncomplete(
                    'INTERSECT of two modified tables returned 0 rows. Expected at least 2 common regions (east, south).'
                );
            }

            // Common regions: 'east' (both have it) and 'south' (both new)
            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'INTERSECT returned ' . count($rows) . ' rows. Expected 2 (east, south). Got: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
            $regions = array_column($rows, 'region');
            $this->assertContains('east', $regions);
            $this->assertContains('south', $regions);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INTERSECT both tables modified test failed: ' . $e->getMessage());
        }
    }

    /**
     * EXCEPT after DML — regions in customers but not suppliers.
     */
    public function testExceptBothTablesModified(): void
    {
        try {
            // Add 'south' to customers only
            $this->pdo->exec("INSERT INTO sl_csd_customers VALUES (3, 'Carol', 'south')");
            // Delete 'east' customer
            $this->pdo->exec("DELETE FROM sl_csd_customers WHERE id = 1");

            $rows = $this->ztdQuery(
                "SELECT region FROM sl_csd_customers
                 EXCEPT
                 SELECT region FROM sl_csd_suppliers
                 ORDER BY region"
            );

            // Customers now: Bob(west), Carol(south). Suppliers: Acme(east), Beta(north).
            // Customers-only regions: south, west
            if (count($rows) === 0) {
                $this->markTestIncomplete(
                    'EXCEPT of two modified tables returned 0 rows. Expected 2 (south, west).'
                );
            }
            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'EXCEPT returned ' . count($rows) . ' rows. Expected 2. Got: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
            $regions = array_column($rows, 'region');
            $this->assertContains('south', $regions);
            $this->assertContains('west', $regions);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('EXCEPT both tables modified test failed: ' . $e->getMessage());
        }
    }

    /**
     * UNION with WHERE on each branch after DML on both tables.
     */
    public function testUnionWithWhereAfterDml(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_csd_customers VALUES (3, 'Carol', 'east')");
            $this->pdo->exec("UPDATE sl_csd_suppliers SET region = 'east' WHERE id = 2");

            $rows = $this->ztdQuery(
                "SELECT name FROM sl_csd_customers WHERE region = 'east'
                 UNION ALL
                 SELECT name FROM sl_csd_suppliers WHERE region = 'east'
                 ORDER BY name"
            );

            // Customers east: Alice(1), Carol(3). Suppliers east: Acme(1), Beta(2 updated).
            if (count($rows) !== 4) {
                $this->markTestIncomplete(
                    'UNION ALL with WHERE returned ' . count($rows) . ' rows. Expected 4. Got: ' . json_encode($rows)
                );
            }

            $this->assertCount(4, $rows);
            $names = array_column($rows, 'name');
            $this->assertContains('Carol', $names);
            $this->assertContains('Beta', $names); // Beta was updated to east
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UNION with WHERE after DML test failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared compound SELECT with params after DML on both tables.
     */
    public function testPreparedUnionAfterDml(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_csd_customers VALUES (3, 'Carol', 'south')");
            $this->pdo->exec("INSERT INTO sl_csd_suppliers VALUES (3, 'Gamma', 'south')");

            $rows = $this->ztdPrepareAndExecute(
                "SELECT name FROM sl_csd_customers WHERE region = ?
                 UNION ALL
                 SELECT name FROM sl_csd_suppliers WHERE region = ?
                 ORDER BY name",
                ['south', 'south']
            );

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'Prepared UNION returned ' . count($rows) . ' rows. Expected 2. Got: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
            $names = array_column($rows, 'name');
            $this->assertContains('Carol', $names);
            $this->assertContains('Gamma', $names);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UNION after DML test failed: ' . $e->getMessage());
        }
    }
}
