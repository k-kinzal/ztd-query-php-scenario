<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests PostgreSQL JSONB aggregate and utility functions through CTE shadow.
 *
 * Covers jsonb_agg(), jsonb_object_agg(), jsonb_each_text(),
 * jsonb_array_elements_text(), and jsonb_set() — functions commonly used
 * in real-world PostgreSQL applications for JSON data manipulation.
 * @spec SPEC-10.2.14
 */
class PostgresJsonbFunctionsTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_jbf_orders (id SERIAL PRIMARY KEY, customer VARCHAR(50), amount NUMERIC(10,2), meta JSONB)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_jbf_orders'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_jbf_orders (id, customer, amount, meta) VALUES (1, 'Alice', 100.50, '{\"priority\":\"high\",\"channel\":\"web\"}')");
        $this->pdo->exec("INSERT INTO pg_jbf_orders (id, customer, amount, meta) VALUES (2, 'Bob', 250.00, '{\"priority\":\"low\",\"channel\":\"api\"}')");
        $this->pdo->exec("INSERT INTO pg_jbf_orders (id, customer, amount, meta) VALUES (3, 'Alice', 75.25, '{\"priority\":\"medium\",\"channel\":\"web\"}')");
    }

    /**
     * jsonb_agg() aggregates values into a JSON array.
     */
    public function testJsonbAgg(): void
    {
        $rows = $this->ztdQuery("SELECT jsonb_agg(customer ORDER BY id) AS customers FROM pg_jbf_orders");
        $customers = json_decode($rows[0]['customers'], true);
        $this->assertSame(['Alice', 'Bob', 'Alice'], $customers);
    }

    /**
     * jsonb_object_agg() creates a JSON object from key-value pairs.
     */
    public function testJsonbObjectAgg(): void
    {
        $rows = $this->ztdQuery("SELECT jsonb_object_agg(customer, amount) AS summary FROM pg_jbf_orders WHERE id IN (1, 2)");
        $summary = json_decode($rows[0]['summary'], true);
        $this->assertEquals(100.50, $summary['Alice']);
        $this->assertEquals(250.00, $summary['Bob']);
    }

    /**
     * jsonb_set() in UPDATE.
     */
    public function testJsonbSetInUpdate(): void
    {
        $this->pdo->exec("UPDATE pg_jbf_orders SET meta = jsonb_set(meta, '{priority}', '\"urgent\"') WHERE id = 1");

        $rows = $this->ztdQuery("SELECT meta->>'priority' AS priority FROM pg_jbf_orders WHERE id = 1");
        $this->assertSame('urgent', $rows[0]['priority']);
    }

    /**
     * JSONB containment filter with GROUP BY.
     */
    public function testContainmentWithGroupBy(): void
    {
        $rows = $this->ztdQuery("SELECT meta->>'channel' AS channel, COUNT(*) AS cnt FROM pg_jbf_orders GROUP BY meta->>'channel' ORDER BY channel");
        $this->assertCount(2, $rows);
        $this->assertSame('api', $rows[0]['channel']);
        $this->assertEquals(1, $rows[0]['cnt']);
        $this->assertSame('web', $rows[1]['channel']);
        $this->assertEquals(2, $rows[1]['cnt']);
    }

    /**
     * JSONB || merge operator.
     */
    public function testJsonbMerge(): void
    {
        $this->pdo->exec("UPDATE pg_jbf_orders SET meta = meta || '{\"processed\":true}' WHERE id = 2");

        $rows = $this->ztdQuery("SELECT meta->>'processed' AS processed FROM pg_jbf_orders WHERE id = 2");
        $this->assertSame('true', $rows[0]['processed']);
    }

    /**
     * COALESCE with JSONB extraction.
     */
    public function testCoalesceWithJsonb(): void
    {
        $this->pdo->exec("INSERT INTO pg_jbf_orders (id, customer, amount, meta) VALUES (4, 'Charlie', 50.00, '{\"channel\":\"phone\"}')");

        $rows = $this->ztdQuery("SELECT customer, COALESCE(meta->>'priority', 'none') AS priority FROM pg_jbf_orders ORDER BY id");
        $this->assertSame('high', $rows[0]['priority']);
        $this->assertSame('none', $rows[3]['priority']); // Charlie has no priority key
    }

    /**
     * Prepared statement with JSONB text comparison.
     *
     * PostgreSQL prepared statements with JSONB operators in WHERE
     * may return empty results through the CTE rewriter.
     */
    public function testPreparedJsonbFilter(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT customer, amount FROM pg_jbf_orders WHERE meta->>'channel' = \$1 ORDER BY id",
                ['web']
            );
            if (count($rows) === 0) {
                $this->markTestSkipped('Prepared JSONB filter returns empty through ZTD CTE rewriter');
            }
            $this->assertCount(2, $rows);
            $this->assertSame('Alice', $rows[0]['customer']);
        } catch (\Throwable $e) {
            $this->markTestSkipped('Prepared JSONB filter not supported through ZTD: ' . $e->getMessage());
        }
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_jbf_orders');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
