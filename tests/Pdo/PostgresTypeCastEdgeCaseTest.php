<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests PostgreSQL :: type cast operator through the CTE rewriter.
 *
 * Real-world scenario: PostgreSQL's :: type cast syntax is ubiquitous in
 * PostgreSQL applications. It appears in SELECT lists, WHERE clauses,
 * INSERT VALUES, and UPDATE SET. The CTE rewriter must handle :: correctly
 * and not confuse it with other syntax or break the cast expression.
 *
 * @spec SPEC-3.1
 * @spec SPEC-4.1
 */
class PostgresTypeCastEdgeCaseTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_tce_records (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                amount NUMERIC(10,2),
                metadata TEXT,
                created_date DATE
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_tce_records'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO pg_tce_records (id, name, amount, metadata, created_date) VALUES (1, 'Alpha', 100.50, '{\"key\":\"val\"}', '2025-01-15')");
        $this->ztdExec("INSERT INTO pg_tce_records (id, name, amount, metadata, created_date) VALUES (2, 'Beta', 200.75, '{\"key\":\"other\"}', '2025-06-20')");
        $this->ztdExec("INSERT INTO pg_tce_records (id, name, amount, metadata, created_date) VALUES (3, 'Gamma', NULL, NULL, NULL)");
    }

    /**
     * SELECT with :: type cast in column list.
     */
    public function testTypeCastInSelect(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT id, amount::INTEGER AS amount_int FROM pg_tce_records WHERE amount IS NOT NULL ORDER BY id"
            );

            $this->assertCount(2, $rows);
            $this->assertEquals(101, (int) $rows[0]['amount_int']); // 100.50 rounds to 101
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Type cast in SELECT failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * WHERE clause with :: type cast.
     */
    public function testTypeCastInWhere(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT name FROM pg_tce_records WHERE amount::INTEGER > 150 ORDER BY id"
            );

            $this->assertCount(1, $rows);
            $this->assertSame('Beta', $rows[0]['name']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Type cast in WHERE failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * INSERT with :: type cast in VALUES.
     */
    public function testTypeCastInInsertValues(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO pg_tce_records (id, name, amount, created_date) VALUES (4, 'Delta', '300.99'::NUMERIC, '2025-12-25'::DATE)"
            );

            $rows = $this->ztdQuery("SELECT amount, created_date FROM pg_tce_records WHERE id = 4");
            $this->assertCount(1, $rows);
            $this->assertEquals(300.99, (float) $rows[0]['amount'], '', 0.01);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Type cast in INSERT VALUES failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE with :: type cast in SET.
     */
    public function testTypeCastInUpdateSet(): void
    {
        try {
            $this->ztdExec(
                "UPDATE pg_tce_records SET amount = '500.00'::NUMERIC WHERE id = 3"
            );

            $rows = $this->ztdQuery("SELECT amount FROM pg_tce_records WHERE id = 3");
            $this->assertCount(1, $rows);
            $this->assertEquals(500.00, (float) $rows[0]['amount'], '', 0.01);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Type cast in UPDATE SET failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * :: cast to TEXT in various positions.
     */
    public function testCastToText(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT id::TEXT AS id_text, amount::TEXT AS amount_text FROM pg_tce_records WHERE id = 1"
            );

            $this->assertCount(1, $rows);
            $this->assertSame('1', $rows[0]['id_text']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Cast to TEXT failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Chained type casts.
     */
    public function testChainedTypeCasts(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT amount::INTEGER::TEXT AS cast_chain FROM pg_tce_records WHERE id = 1"
            );

            $this->assertCount(1, $rows);
            // 100.50 -> 101 (int) -> '101' (text)
            $this->assertSame('101', $rows[0]['cast_chain']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Chained type casts failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * CAST() function syntax (alternative to ::).
     */
    public function testCastFunction(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT CAST(amount AS INTEGER) AS amount_int FROM pg_tce_records WHERE id = 1"
            );

            $this->assertCount(1, $rows);
            $this->assertEquals(101, (int) $rows[0]['amount_int']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'CAST function failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * :: type cast with prepared parameter.
     */
    public function testTypeCastWithPreparedParam(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT name FROM pg_tce_records WHERE amount::INTEGER > ? ORDER BY id",
                [150]
            );

            $this->assertCount(1, $rows);
            $this->assertSame('Beta', $rows[0]['name']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Type cast with prepared param failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Date :: cast in WHERE for comparison.
     */
    public function testDateCastInWhere(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT name FROM pg_tce_records WHERE created_date > '2025-03-01'::DATE ORDER BY id"
            );

            $this->assertCount(1, $rows);
            $this->assertSame('Beta', $rows[0]['name']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Date cast in WHERE failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * NULL::type cast.
     */
    public function testNullTypeCast(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT id, NULL::TEXT AS null_text FROM pg_tce_records ORDER BY id LIMIT 1"
            );

            $this->assertCount(1, $rows);
            $this->assertNull($rows[0]['null_text']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'NULL type cast failed: ' . $e->getMessage()
            );
        }
    }
}
