<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests numeric precision and type coercion edge cases through the CTE shadow store.
 *
 * Verifies that the CTE rewriter correctly preserves numeric precision during
 * value round-tripping: large integers, IEEE 754 floats, very small values,
 * NULL, and arithmetic expressions must survive the shadow store intact.
 *
 * @spec SPEC-4.1
 */
class SqliteNumericPrecisionTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_num_measurements (
            id INTEGER PRIMARY KEY,
            label TEXT NOT NULL,
            int_val INTEGER,
            float_val REAL,
            decimal_val TEXT,
            small_val INTEGER
        )';
    }

    protected function getTableNames(): array
    {
        return ['sl_num_measurements'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Row 1: max INT32
        $this->pdo->exec("INSERT INTO sl_num_measurements VALUES (1, 'max_int32', 2147483647, 1.0, '1000000.000001', 100)");
        // Row 2: negative integer
        $this->pdo->exec("INSERT INTO sl_num_measurements VALUES (2, 'negative', -999999, -273.15, '-50000.500000', -1)");
        // Row 3: pi with full double precision
        $this->pdo->exec("INSERT INTO sl_num_measurements VALUES (3, 'pi', 3, 3.141592653589793, '3.141593', 0)");
        // Row 4: very small float
        $this->pdo->exec("INSERT INTO sl_num_measurements VALUES (4, 'tiny', 0, 0.000001, '0.000001', 1)");
        // Row 5: zero
        $this->pdo->exec("INSERT INTO sl_num_measurements VALUES (5, 'zero', 0, 0.0, '0.000000', 0)");
        // Row 6: NULL numeric values
        $this->pdo->exec("INSERT INTO sl_num_measurements VALUES (6, 'null_row', NULL, NULL, NULL, NULL)");
    }

    /**
     * Large integer round-trips correctly through the shadow store.
     */
    public function testIntegerPrecisionPreservation(): void
    {
        $rows = $this->ztdQuery(
            "SELECT int_val FROM sl_num_measurements WHERE label = 'max_int32'"
        );

        $this->assertCount(1, $rows);
        $this->assertEquals(2147483647, (int) $rows[0]['int_val']);

        // Negative integer
        $rows = $this->ztdQuery(
            "SELECT int_val FROM sl_num_measurements WHERE label = 'negative'"
        );
        $this->assertCount(1, $rows);
        $this->assertEquals(-999999, (int) $rows[0]['int_val']);
    }

    /**
     * IEEE 754 double (pi) preserved through shadow store.
     */
    public function testFloatPrecisionPreservation(): void
    {
        $rows = $this->ztdQuery(
            "SELECT float_val FROM sl_num_measurements WHERE label = 'pi'"
        );

        $this->assertCount(1, $rows);
        // PHP's double can represent pi to ~15 significant digits
        $this->assertEqualsWithDelta(
            3.141592653589793,
            (float) $rows[0]['float_val'],
            1e-12,
            'Pi value should survive CTE shadow store round-trip with at least 12 digits of precision'
        );

        // Very small float
        $rows = $this->ztdQuery(
            "SELECT float_val FROM sl_num_measurements WHERE label = 'tiny'"
        );
        $this->assertCount(1, $rows);
        $this->assertEqualsWithDelta(0.000001, (float) $rows[0]['float_val'], 1e-10);
    }

    /**
     * Arithmetic expressions in SELECT preserve precision through the CTE rewriter.
     */
    public function testArithmeticInSelect(): void
    {
        $rows = $this->ztdQuery(
            "SELECT label,
                    int_val + small_val AS int_sum,
                    float_val * 2.0 AS float_doubled
             FROM sl_num_measurements
             WHERE label IN ('max_int32', 'pi')
             ORDER BY id"
        );

        $this->assertCount(2, $rows);

        // max_int32: 2147483647 + 100 = 2147483747
        $this->assertSame('max_int32', $rows[0]['label']);
        $this->assertEquals(2147483747, (int) $rows[0]['int_sum']);

        // pi: 3.141592653589793 * 2 = 6.283185307179586
        $this->assertSame('pi', $rows[1]['label']);
        $this->assertEqualsWithDelta(
            6.283185307179586,
            (float) $rows[1]['float_doubled'],
            1e-12
        );
    }

    /**
     * SUM and AVG aggregates preserve numeric precision.
     */
    public function testSumAvgAggregatePrecision(): void
    {
        $rows = $this->ztdQuery(
            "SELECT SUM(float_val) AS float_sum,
                    AVG(float_val) AS float_avg,
                    SUM(int_val) AS int_sum
             FROM sl_num_measurements
             WHERE float_val IS NOT NULL"
        );

        $this->assertCount(1, $rows);

        // SUM of float_val: 1.0 + (-273.15) + 3.141592653589793 + 0.000001 + 0.0
        $expectedSum = 1.0 + (-273.15) + 3.141592653589793 + 0.000001 + 0.0;
        $this->assertEqualsWithDelta($expectedSum, (float) $rows[0]['float_sum'], 1e-6);

        // AVG = SUM / 5
        $expectedAvg = $expectedSum / 5.0;
        $this->assertEqualsWithDelta($expectedAvg, (float) $rows[0]['float_avg'], 1e-6);

        // SUM of int_val: 2147483647 + (-999999) + 3 + 0 + 0 = 2146483651
        $this->assertEquals(2146483651, (int) $rows[0]['int_sum']);
    }

    /**
     * Comparison operators with precise float values in WHERE clause.
     */
    public function testComparisonWithPreciseValues(): void
    {
        // Should match pi (3.14159...) but not 1.0 or -273.15
        $rows = $this->ztdQuery(
            "SELECT label, float_val FROM sl_num_measurements
             WHERE float_val > 3.14159 AND float_val < 3.14160
             ORDER BY id"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('pi', $rows[0]['label']);

        // Boundary: >= 0.000001 should include 'tiny' but not 'zero'
        $rows = $this->ztdQuery(
            "SELECT label FROM sl_num_measurements
             WHERE float_val >= 0.000001 AND float_val <= 0.000001"
        );
        $this->assertCount(1, $rows);
        $this->assertSame('tiny', $rows[0]['label']);
    }

    /**
     * Division behavior: integer division vs float division.
     */
    public function testDivisionPrecision(): void
    {
        // Integer division in SQLite: 7 / 2 = 3 (integer division)
        $rows = $this->ztdQuery(
            "SELECT 7 / 2 AS int_div, 7.0 / 2.0 AS float_div"
        );
        $this->assertCount(1, $rows);
        $this->assertEquals(3, (int) $rows[0]['int_div']);
        $this->assertEqualsWithDelta(3.5, (float) $rows[0]['float_div'], 1e-10);

        // Division using stored values
        $rows = $this->ztdQuery(
            "SELECT label,
                    int_val / 3 AS int_divided,
                    float_val / 3.0 AS float_divided
             FROM sl_num_measurements
             WHERE label = 'pi'"
        );
        $this->assertCount(1, $rows);
        // 3 / 3 = 1
        $this->assertEquals(1, (int) $rows[0]['int_divided']);
        // 3.141592653589793 / 3.0 = 1.0471975511965976
        $this->assertEqualsWithDelta(
            1.0471975511965976,
            (float) $rows[0]['float_divided'],
            1e-12
        );
    }

    /**
     * UPDATE with arithmetic: precision preserved after mutation.
     */
    public function testUpdateWithArithmeticPrecision(): void
    {
        // Multiply the pi float_val by 1.1
        $this->pdo->exec(
            "UPDATE sl_num_measurements SET float_val = float_val * 1.1 WHERE label = 'pi'"
        );

        $rows = $this->ztdQuery(
            "SELECT float_val FROM sl_num_measurements WHERE label = 'pi'"
        );

        $this->assertCount(1, $rows);
        // 3.141592653589793 * 1.1 = 3.4557519189487724
        $this->assertEqualsWithDelta(
            3.4557519189487724,
            (float) $rows[0]['float_val'],
            1e-10,
            'Float precision must hold after UPDATE SET val = val * 1.1'
        );

        // Also verify integer arithmetic in UPDATE
        $this->pdo->exec(
            "UPDATE sl_num_measurements SET int_val = int_val + 1 WHERE label = 'max_int32'"
        );

        $rows = $this->ztdQuery(
            "SELECT int_val FROM sl_num_measurements WHERE label = 'max_int32'"
        );
        $this->assertCount(1, $rows);
        // 2147483647 + 1 = 2147483648 (overflows INT32, but SQLite uses 64-bit integers)
        $this->assertEquals(2147483648, (int) $rows[0]['int_val']);
    }

    /**
     * Physical isolation: shadow store data must not appear in the physical table.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();

        $rows = $this->pdo->query(
            "SELECT COUNT(*) AS cnt FROM sl_num_measurements"
        )->fetchAll(PDO::FETCH_ASSOC);

        $this->assertEquals(0, (int) $rows[0]['cnt'], 'Physical table must be empty; all rows live in shadow store');
    }
}
