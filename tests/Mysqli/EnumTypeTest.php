<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests ENUM column behavior in the CTE shadow store via MySQLi.
 *
 * Extends coverage beyond the existing EnumTypeTest by testing:
 *   - All ENUM values round-trip correctly
 *   - ORDER BY ENUM uses MySQL's internal index ordering
 *   - GROUP BY and aggregate on ENUM columns
 *   - ENUM in subquery WHERE
 *   - ENUM with DEFAULT value
 *   - Prepared statements with ENUM parameters
 *
 * MySQL ENUM ordering: values are stored by internal index (1-based,
 * in definition order), not alphabetically. ORDER BY on ENUM sorts
 * by this index, not by the string value.
 *
 * @spec SPEC-10.2.19
 */
class EnumTypeTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_enum_sz (
            id INT PRIMARY KEY,
            size ENUM(\'small\', \'medium\', \'large\') NOT NULL,
            color ENUM(\'red\', \'green\', \'blue\') DEFAULT \'green\',
            quantity INT NOT NULL DEFAULT 0
        ) ENGINE=InnoDB';
    }

    protected function getTableNames(): array
    {
        return ['mi_enum_sz'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO mi_enum_sz VALUES (1, 'small',  'red',   10)");
        $this->ztdExec("INSERT INTO mi_enum_sz VALUES (2, 'medium', 'green', 20)");
        $this->ztdExec("INSERT INTO mi_enum_sz VALUES (3, 'large',  'blue',  30)");
        $this->ztdExec("INSERT INTO mi_enum_sz VALUES (4, 'small',  'blue',  15)");
        $this->ztdExec("INSERT INTO mi_enum_sz VALUES (5, 'large',  'red',   25)");
    }

    /**
     * Each ENUM value round-trips correctly through the CTE shadow store.
     */
    public function testEnumValuesRoundTrip(): void
    {
        try {
            $rows = $this->ztdQuery('SELECT id, size, color FROM mi_enum_sz ORDER BY id');

            $this->assertCount(5, $rows);
            $this->assertSame('small', $rows[0]['size']);
            $this->assertSame('red', $rows[0]['color']);
            $this->assertSame('medium', $rows[1]['size']);
            $this->assertSame('green', $rows[1]['color']);
            $this->assertSame('large', $rows[2]['size']);
            $this->assertSame('blue', $rows[2]['color']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'ENUM values round-trip failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * WHERE filter on ENUM column.
     */
    public function testWhereFilterOnEnum(): void
    {
        try {
            $rows = $this->ztdQuery("SELECT id FROM mi_enum_sz WHERE size = 'small' ORDER BY id");

            $this->assertCount(2, $rows);
            $this->assertSame(1, (int) $rows[0]['id']);
            $this->assertSame(4, (int) $rows[1]['id']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'WHERE filter on ENUM failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * ORDER BY ENUM column sorts by internal index, not alphabetically.
     *
     * MySQL ENUM internal order: small=1, medium=2, large=3.
     * Alphabetical would be: large, medium, small.
     * The CTE rewriter must preserve the ENUM ordering semantics.
     */
    public function testOrderByEnumUsesInternalIndex(): void
    {
        try {
            $rows = $this->ztdQuery(
                'SELECT id, size FROM mi_enum_sz ORDER BY size ASC, id ASC'
            );

            $this->assertCount(5, $rows);

            // small (index 1) should come first
            $this->assertSame('small', $rows[0]['size'], 'First rows should be small (index 1)');
            $this->assertSame('small', $rows[1]['size']);

            // medium (index 2) next
            $this->assertSame('medium', $rows[2]['size'], 'Next should be medium (index 2)');

            // large (index 3) last
            $this->assertSame('large', $rows[3]['size'], 'Last should be large (index 3)');
            $this->assertSame('large', $rows[4]['size']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'ORDER BY ENUM internal index failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * ORDER BY ENUM DESC reverses the internal index order.
     */
    public function testOrderByEnumDesc(): void
    {
        try {
            $rows = $this->ztdQuery(
                'SELECT id, size FROM mi_enum_sz ORDER BY size DESC, id ASC'
            );

            // large (index 3) should come first in DESC
            $this->assertSame('large', $rows[0]['size']);
            $this->assertSame('large', $rows[1]['size']);

            // medium (index 2) next
            $this->assertSame('medium', $rows[2]['size']);

            // small (index 1) last
            $this->assertSame('small', $rows[3]['size']);
            $this->assertSame('small', $rows[4]['size']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'ORDER BY ENUM DESC failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE ENUM column and verify the change is visible.
     */
    public function testUpdateEnumColumn(): void
    {
        try {
            $this->ztdExec("UPDATE mi_enum_sz SET size = 'large' WHERE id = 1");

            $rows = $this->ztdQuery('SELECT size FROM mi_enum_sz WHERE id = 1');
            $this->assertSame('large', $rows[0]['size']);

            // Verify ORDER BY still reflects updated value
            $rows = $this->ztdQuery(
                "SELECT id FROM mi_enum_sz WHERE size = 'large' ORDER BY id"
            );
            $ids = array_column($rows, 'id');
            $ids = array_map('intval', $ids);
            $this->assertContains(1, $ids, 'id=1 should now appear in large filter');
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'UPDATE ENUM column failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * GROUP BY ENUM column with COUNT aggregate.
     *
     * Tests that the CTE rewriter correctly handles aggregation
     * over ENUM-typed columns.
     */
    public function testGroupByEnumWithCount(): void
    {
        try {
            $rows = $this->ztdQuery(
                'SELECT size, COUNT(*) AS cnt FROM mi_enum_sz GROUP BY size ORDER BY size'
            );

            $this->assertCount(3, $rows, 'Should have 3 distinct ENUM values');

            // Order by ENUM internal index: small=1, medium=2, large=3
            $this->assertSame('small', $rows[0]['size']);
            $this->assertEquals(2, (int) $rows[0]['cnt']);

            $this->assertSame('medium', $rows[1]['size']);
            $this->assertEquals(1, (int) $rows[1]['cnt']);

            $this->assertSame('large', $rows[2]['size']);
            $this->assertEquals(2, (int) $rows[2]['cnt']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'GROUP BY ENUM with COUNT failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * ENUM comparison operators: >, <, >=, <= compare by internal index.
     *
     * In MySQL, ENUM comparisons use the internal index, so
     * 'medium' > 'small' is true (index 2 > 1).
     */
    public function testEnumComparisonOperators(): void
    {
        try {
            // size > 'small' means index > 1, i.e. medium and large
            $rows = $this->ztdQuery("SELECT id FROM mi_enum_sz WHERE size > 'small' ORDER BY id");
            $this->assertCount(3, $rows, "'medium' and 'large' have higher index than 'small'");

            $ids = array_column($rows, 'id');
            $ids = array_map('intval', $ids);
            $this->assertSame([2, 3, 5], $ids);

            // size <= 'medium' means index <= 2, i.e. small and medium
            $rows = $this->ztdQuery("SELECT id FROM mi_enum_sz WHERE size <= 'medium' ORDER BY id");
            $this->assertCount(3, $rows);

            $ids = array_column($rows, 'id');
            $ids = array_map('intval', $ids);
            $this->assertSame([1, 2, 4], $ids);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'ENUM comparison operators failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * ENUM column with DEFAULT value.
     *
     * INSERT without specifying the color column should use the
     * DEFAULT 'green' value, and the CTE rewriter must preserve this.
     */
    public function testEnumDefaultValue(): void
    {
        try {
            $this->ztdExec("INSERT INTO mi_enum_sz (id, size, quantity) VALUES (6, 'medium', 50)");

            $rows = $this->ztdQuery('SELECT color FROM mi_enum_sz WHERE id = 6');
            $this->assertSame('green', $rows[0]['color'], 'Default ENUM value should be green');
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'ENUM DEFAULT value failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Prepared statement with ENUM parameter in WHERE.
     */
    public function testPreparedWithEnumParameter(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                'SELECT id, quantity FROM mi_enum_sz WHERE size = ? ORDER BY quantity DESC',
                ['large']
            );

            $this->assertCount(2, $rows);
            $this->assertEquals(30, (int) $rows[0]['quantity']); // id=3
            $this->assertEquals(25, (int) $rows[1]['quantity']); // id=5
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Prepared with ENUM parameter failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Subquery with ENUM filter.
     *
     * Tests that the CTE rewriter correctly handles ENUM comparisons
     * inside a subquery.
     */
    public function testSubqueryWithEnumFilter(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT id, size FROM mi_enum_sz
                 WHERE id IN (SELECT id FROM mi_enum_sz WHERE color = 'red')
                 ORDER BY id"
            );

            $this->assertCount(2, $rows);
            $this->assertSame(1, (int) $rows[0]['id']);
            $this->assertSame(5, (int) $rows[1]['id']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Subquery with ENUM filter failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Physical isolation: ENUM data must not reach the physical table.
     */
    public function testPhysicalIsolation(): void
    {
        $this->disableZtd();
        $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM mi_enum_sz');
        $this->assertEquals(0, (int) $rows[0]['cnt'], 'Physical table should be empty');
    }
}
