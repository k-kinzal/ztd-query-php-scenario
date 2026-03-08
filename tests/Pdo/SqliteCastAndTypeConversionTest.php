<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests CAST and type conversion expressions through ZTD shadow store.
 * Real user pattern: data type coercion in queries, JSON extraction, numeric formatting.
 * @spec SPEC-10.2.48
 */
class SqliteCastAndTypeConversionTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_cast_items (id INTEGER PRIMARY KEY, name TEXT, price_text TEXT, quantity TEXT, weight REAL, created TEXT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_cast_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_cast_items VALUES (1, 'Widget', '19.99', '10', 2.5, '2024-01-15')");
        $this->pdo->exec("INSERT INTO sl_cast_items VALUES (2, 'Gadget', '49.50', '3', 0.8, '2024-02-20')");
        $this->pdo->exec("INSERT INTO sl_cast_items VALUES (3, 'Bolt', '0.75', '500', 0.01, '2024-03-10')");
        $this->pdo->exec("INSERT INTO sl_cast_items VALUES (4, 'Cable', '12.00', '25', NULL, '2024-04-01')");
    }

    /**
     * CAST text to integer in SELECT for arithmetic.
     */
    public function testCastTextToIntegerInSelect(): void
    {
        $rows = $this->ztdQuery("
            SELECT name, CAST(quantity AS INTEGER) AS qty_int
            FROM sl_cast_items
            ORDER BY qty_int DESC
        ");
        $this->assertCount(4, $rows);
        $this->assertSame('Bolt', $rows[0]['name']);
        $this->assertEquals(500, (int) $rows[0]['qty_int']);
    }

    /**
     * CAST text to real in WHERE clause for comparison.
     */
    public function testCastTextToRealInWhere(): void
    {
        $rows = $this->ztdQuery("
            SELECT name, price_text
            FROM sl_cast_items
            WHERE CAST(price_text AS REAL) > 15.0
            ORDER BY name
        ");
        $this->assertCount(2, $rows);
        $this->assertSame('Gadget', $rows[0]['name']);
        $this->assertSame('Widget', $rows[1]['name']);
    }

    /**
     * CAST in ORDER BY — sort text column numerically.
     */
    public function testCastInOrderBy(): void
    {
        $rows = $this->ztdQuery("
            SELECT name, price_text
            FROM sl_cast_items
            ORDER BY CAST(price_text AS REAL) ASC
        ");
        $this->assertSame('Bolt', $rows[0]['name']);     // 0.75
        $this->assertSame('Cable', $rows[1]['name']);     // 12.00
        $this->assertSame('Widget', $rows[2]['name']);    // 19.99
        $this->assertSame('Gadget', $rows[3]['name']);    // 49.50
    }

    /**
     * CAST in GROUP BY aggregate.
     */
    public function testCastInGroupByAggregate(): void
    {
        $rows = $this->ztdQuery("
            SELECT
                CASE WHEN CAST(price_text AS REAL) >= 10.0 THEN 'expensive' ELSE 'cheap' END AS tier,
                COUNT(*) AS cnt,
                SUM(CAST(price_text AS REAL)) AS total
            FROM sl_cast_items
            GROUP BY tier
            ORDER BY tier
        ");
        $this->assertCount(2, $rows);
        $this->assertSame('cheap', $rows[0]['tier']);
        $this->assertEquals(1, (int) $rows[0]['cnt']); // Bolt
        $this->assertSame('expensive', $rows[1]['tier']);
        $this->assertEquals(3, (int) $rows[1]['cnt']); // Widget, Gadget, Cable
    }

    /**
     * CAST with NULL values — CAST(NULL AS ...) yields NULL.
     */
    public function testCastNullValues(): void
    {
        $rows = $this->ztdQuery("
            SELECT name, CAST(weight AS TEXT) AS weight_text
            FROM sl_cast_items
            WHERE id = 4
        ");
        $this->assertCount(1, $rows);
        $this->assertNull($rows[0]['weight_text']);
    }

    /**
     * Computed column using multiple CASTs.
     */
    public function testComputedColumnWithMultipleCasts(): void
    {
        $rows = $this->ztdQuery("
            SELECT name,
                   CAST(price_text AS REAL) * CAST(quantity AS INTEGER) AS line_total
            FROM sl_cast_items
            ORDER BY line_total DESC
        ");
        $this->assertCount(4, $rows);
        $this->assertSame('Bolt', $rows[0]['name']); // 0.75 * 500 = 375
        $this->assertEqualsWithDelta(375.0, (float) $rows[0]['line_total'], 0.01);
    }

    /**
     * Prepared statement with CAST in WHERE.
     */
    public function testPreparedCastInWhere(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT name FROM sl_cast_items WHERE CAST(quantity AS INTEGER) > ? ORDER BY name",
            [20]
        );
        $this->assertCount(2, $rows);
        $this->assertSame('Bolt', $rows[0]['name']);
        $this->assertSame('Cable', $rows[1]['name']);
    }

    /**
     * CAST after INSERT — new data visible with type conversion.
     */
    public function testCastAfterInsert(): void
    {
        $this->pdo->exec("INSERT INTO sl_cast_items VALUES (5, 'Spring', '3.25', '1000', 0.005, '2024-05-01')");

        $rows = $this->ztdQuery("
            SELECT SUM(CAST(price_text AS REAL) * CAST(quantity AS INTEGER)) AS grand_total
            FROM sl_cast_items
        ");
        // 19.99*10 + 49.50*3 + 0.75*500 + 12.00*25 + 3.25*1000
        // = 199.9 + 148.5 + 375 + 300 + 3250 = 4273.4
        $this->assertEqualsWithDelta(4273.4, (float) $rows[0]['grand_total'], 0.1);
    }

    /**
     * typeof() function — SQLite-specific type inspection.
     */
    public function testTypeofFunction(): void
    {
        $rows = $this->ztdQuery("
            SELECT name, typeof(weight) AS weight_type, typeof(id) AS id_type
            FROM sl_cast_items
            ORDER BY id
        ");
        $this->assertSame('real', $rows[0]['weight_type']);
        $this->assertSame('integer', $rows[0]['id_type']);
        // NULL weight row
        $this->assertSame('null', $rows[3]['weight_type']);
    }
}
