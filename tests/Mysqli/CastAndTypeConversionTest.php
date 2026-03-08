<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests CAST and type conversion expressions through ZTD shadow store.
 * Real user pattern: data type coercion in queries, numeric formatting.
 * @spec SPEC-10.2.48
 */
class CastAndTypeConversionTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_cast_items (id INT PRIMARY KEY, name VARCHAR(100), price_text VARCHAR(20), quantity VARCHAR(20), weight DOUBLE, created DATE)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_cast_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_cast_items VALUES (1, 'Widget', '19.99', '10', 2.5, '2024-01-15')");
        $this->mysqli->query("INSERT INTO mi_cast_items VALUES (2, 'Gadget', '49.50', '3', 0.8, '2024-02-20')");
        $this->mysqli->query("INSERT INTO mi_cast_items VALUES (3, 'Bolt', '0.75', '500', 0.01, '2024-03-10')");
        $this->mysqli->query("INSERT INTO mi_cast_items VALUES (4, 'Cable', '12.00', '25', NULL, '2024-04-01')");
    }

    public function testCastTextToSignedInSelect(): void
    {
        $rows = $this->ztdQuery("
            SELECT name, CAST(quantity AS SIGNED) AS qty_int
            FROM mi_cast_items
            ORDER BY qty_int DESC
        ");
        $this->assertCount(4, $rows);
        $this->assertSame('Bolt', $rows[0]['name']);
        $this->assertEquals(500, (int) $rows[0]['qty_int']);
    }

    public function testCastTextToDecimalInWhere(): void
    {
        $rows = $this->ztdQuery("
            SELECT name, price_text
            FROM mi_cast_items
            WHERE CAST(price_text AS DECIMAL(10,2)) > 15.0
            ORDER BY name
        ");
        $this->assertCount(2, $rows);
        $this->assertSame('Gadget', $rows[0]['name']);
        $this->assertSame('Widget', $rows[1]['name']);
    }

    public function testCastInOrderBy(): void
    {
        $rows = $this->ztdQuery("
            SELECT name, price_text
            FROM mi_cast_items
            ORDER BY CAST(price_text AS DECIMAL(10,2)) ASC
        ");
        $this->assertSame('Bolt', $rows[0]['name']);
        $this->assertSame('Cable', $rows[1]['name']);
        $this->assertSame('Widget', $rows[2]['name']);
        $this->assertSame('Gadget', $rows[3]['name']);
    }

    public function testCastInGroupByAggregate(): void
    {
        $rows = $this->ztdQuery("
            SELECT
                CASE WHEN CAST(price_text AS DECIMAL(10,2)) >= 10.0 THEN 'expensive' ELSE 'cheap' END AS tier,
                COUNT(*) AS cnt
            FROM mi_cast_items
            GROUP BY tier
            ORDER BY tier
        ");
        $this->assertCount(2, $rows);
        $this->assertSame('cheap', $rows[0]['tier']);
        $this->assertEquals(1, (int) $rows[0]['cnt']);
        $this->assertSame('expensive', $rows[1]['tier']);
        $this->assertEquals(3, (int) $rows[1]['cnt']);
    }

    public function testCastNullValues(): void
    {
        $rows = $this->ztdQuery("
            SELECT name, CAST(weight AS CHAR) AS weight_text
            FROM mi_cast_items
            WHERE id = 4
        ");
        $this->assertCount(1, $rows);
        $this->assertNull($rows[0]['weight_text']);
    }

    public function testComputedColumnWithMultipleCasts(): void
    {
        $rows = $this->ztdQuery("
            SELECT name,
                   CAST(price_text AS DECIMAL(10,2)) * CAST(quantity AS SIGNED) AS line_total
            FROM mi_cast_items
            ORDER BY line_total DESC
        ");
        $this->assertCount(4, $rows);
        $this->assertSame('Bolt', $rows[0]['name']);
        $this->assertEqualsWithDelta(375.0, (float) $rows[0]['line_total'], 0.01);
    }

    public function testPreparedCastInWhere(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT name FROM mi_cast_items WHERE CAST(quantity AS SIGNED) > ? ORDER BY name",
            [20]
        );
        $this->assertCount(2, $rows);
        $this->assertSame('Bolt', $rows[0]['name']);
        $this->assertSame('Cable', $rows[1]['name']);
    }

    public function testConvertFunction(): void
    {
        $rows = $this->ztdQuery("
            SELECT name, CONVERT(price_text, DECIMAL(10,2)) AS price_num
            FROM mi_cast_items
            ORDER BY price_num DESC
        ");
        $this->assertSame('Gadget', $rows[0]['name']);
        $this->assertEqualsWithDelta(49.50, (float) $rows[0]['price_num'], 0.01);
    }

    public function testCastAfterInsert(): void
    {
        $this->mysqli->query("INSERT INTO mi_cast_items VALUES (5, 'Spring', '3.25', '1000', 0.005, '2024-05-01')");

        $rows = $this->ztdQuery("
            SELECT SUM(CAST(price_text AS DECIMAL(10,2)) * CAST(quantity AS SIGNED)) AS grand_total
            FROM mi_cast_items
        ");
        $this->assertEqualsWithDelta(4273.4, (float) $rows[0]['grand_total'], 0.1);
    }
}
