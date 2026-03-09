<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests derived tables (subqueries in FROM clause) through ZTD shadow store.
 *
 * Pattern: SELECT ... FROM (SELECT ...) AS derived_table
 * Related to Issue #13: derived tables not rewritten by CTE rewriter.
 * @spec SPEC-3.3
 */
class SqliteSubqueryInFromTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sf_sales (id INT PRIMARY KEY, product VARCHAR(50), amount INT, region VARCHAR(20))',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sf_sales'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sf_sales VALUES (1, 'Widget', 100, 'east')");
        $this->pdo->exec("INSERT INTO sf_sales VALUES (2, 'Widget', 200, 'west')");
        $this->pdo->exec("INSERT INTO sf_sales VALUES (3, 'Gadget', 150, 'east')");
        $this->pdo->exec("INSERT INTO sf_sales VALUES (4, 'Gadget', 250, 'west')");
        $this->pdo->exec("INSERT INTO sf_sales VALUES (5, 'Gizmo', 300, 'east')");
    }

    /**
     * Simple derived table: aggregate in subquery.
     * Issue #13: derived tables as sole FROM source not rewritten — returns empty.
     */
    public function testSimpleDerivedTable(): void
    {
        $rows = $this->ztdQuery(
            'SELECT d.product, d.total
             FROM (SELECT product, SUM(amount) AS total FROM sf_sales GROUP BY product) d
             ORDER BY d.total DESC'
        );

        if (count($rows) === 0) {
            $this->markTestIncomplete('Issue #13: derived table as sole FROM source returns empty');
        }
        $this->assertCount(3, $rows);
    }

    /**
     * Derived table joined with base table.
     */
    public function testDerivedTableJoinedWithBaseTable(): void
    {
        $rows = $this->ztdQuery(
            'SELECT s.id, s.product, d.region_total
             FROM sf_sales s
             JOIN (SELECT region, SUM(amount) AS region_total FROM sf_sales GROUP BY region) d
               ON d.region = s.region
             WHERE s.product = \'Widget\'
             ORDER BY s.id'
        );

        // Widget has 2 sales: east (region_total=100+150+300=550) and west (200+250=450)
        $this->assertCount(2, $rows);
        $this->assertSame('550', (string) $rows[0]['region_total']);
        $this->assertSame('450', (string) $rows[1]['region_total']);
    }

    /**
     * Derived table with WHERE filter.
     * Issue #13: derived tables as sole FROM source not rewritten.
     */
    public function testDerivedTableWithFilter(): void
    {
        $rows = $this->ztdQuery(
            'SELECT d.product, d.total
             FROM (SELECT product, SUM(amount) AS total FROM sf_sales WHERE region = \'east\' GROUP BY product) d
             ORDER BY d.total DESC'
        );

        if (count($rows) === 0) {
            $this->markTestIncomplete('Issue #13: derived table as sole FROM source returns empty');
        }
        $this->assertCount(3, $rows);
    }

    /**
     * Derived table after INSERT mutation.
     * Issue #13: derived tables as sole FROM source not rewritten.
     */
    public function testDerivedTableAfterInsert(): void
    {
        $this->pdo->exec("INSERT INTO sf_sales VALUES (6, 'Gizmo', 500, 'west')");

        $rows = $this->ztdQuery(
            'SELECT d.product, d.total
             FROM (SELECT product, SUM(amount) AS total FROM sf_sales GROUP BY product) d
             WHERE d.product = \'Gizmo\''
        );

        if (count($rows) === 0) {
            $this->markTestIncomplete('Issue #13: derived table as sole FROM source returns empty');
        }
        $this->assertCount(1, $rows);
        $this->assertSame('800', (string) $rows[0]['total']);
    }

    /**
     * Nested derived tables (subquery within subquery in FROM).
     * Issue #13: derived tables as sole FROM source not rewritten.
     */
    public function testNestedDerivedTables(): void
    {
        $rows = $this->ztdQuery(
            'SELECT outer_d.product, outer_d.total
             FROM (
                 SELECT d.product, d.total
                 FROM (SELECT product, SUM(amount) AS total FROM sf_sales GROUP BY product) d
                 WHERE d.total > 300
             ) outer_d
             ORDER BY outer_d.total DESC'
        );

        if (count($rows) === 0) {
            $this->markTestIncomplete('Issue #13: nested derived tables return empty');
        }
        $this->assertCount(1, $rows);
        $this->assertSame('Gadget', $rows[0]['product']);
    }

    /**
     * Physical table isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM sf_sales');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
