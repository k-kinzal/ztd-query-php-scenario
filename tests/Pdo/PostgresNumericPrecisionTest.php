<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests numeric precision and type coercion edge cases through the CTE shadow store.
 *
 * Verifies that the CTE rewriter correctly preserves numeric precision during
 * value round-tripping on PostgreSQL: large integers, double precision floats,
 * NUMERIC(15,6) fixed-point, very small values, NULL, and arithmetic expressions
 * must survive the shadow store intact.
 *
 * @spec SPEC-4.1
 */
class PostgresNumericPrecisionTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pg_num_measurements (
            id INT PRIMARY KEY,
            label TEXT NOT NULL,
            int_val INTEGER,
            float_val DOUBLE PRECISION,
            decimal_val NUMERIC(15,6),
            small_val INTEGER
        )';
    }

    protected function getTableNames(): array
    {
        return ['pg_num_measurements'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Row 1: max INT32
        $this->pdo->exec("INSERT INTO pg_num_measurements VALUES (1, 'max_int32', 2147483647, 1.0, 1000000.000001, 100)");
        // Row 2: negative integer
        $this->pdo->exec("INSERT INTO pg_num_measurements VALUES (2, 'negative', -999999, -273.15, -50000.500000, -1)");
        // Row 3: pi with full double precision
        $this->pdo->exec("INSERT INTO pg_num_measurements VALUES (3, 'pi', 3, 3.141592653589793, 3.141593, 0)");
        // Row 4: very small float
        $this->pdo->exec("INSERT INTO pg_num_measurements VALUES (4, 'tiny', 0, 0.000001, 0.000001, 1)");
        // Row 5: zero
        $this->pdo->exec("INSERT INTO pg_num_measurements VALUES (5, 'zero', 0, 0.0, 0.000000, 0)");
        // Row 6: NULL numeric values
        $this->pdo->exec("INSERT INTO pg_num_measurements VALUES (6, 'null_row', NULL, NULL, NULL, NULL)");
    }

    /**
     * Large integer round-trips correctly through the shadow store.
     */
    public function testIntegerPrecisionPreservation(): void
    {
        $rows = $this->ztdQuery(
            "SELECT int_val FROM pg_num_measurements WHERE label = 'max_int32'"
        );

        $this->assertCount(1, $rows);
        $this->assertEquals(2147483647, (int) $rows[0]['int_val']);

        // Negative integer
        $rows = $this->ztdQuery(
            "SELECT int_val FROM pg_num_measurements WHERE label = 'negative'"
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
            "SELECT float_val FROM pg_num_measurements WHERE label = 'pi'"
        );

        $this->assertCount(1, $rows);
        $this->assertEqualsWithDelta(
            3.141592653589793,
            (float) $rows[0]['float_val'],
            1e-12,
            'Pi value should survive CTE shadow store round-trip with at least 12 digits of precision'
        );

        // Very small float
        $rows = $this->ztdQuery(
            "SELECT float_val FROM pg_num_measurements WHERE label = 'tiny'"
        );
        $this->assertCount(1, $rows);
        $this->assertEqualsWithDelta(0.000001, (float) $rows[0]['float_val'], 1e-10);

        // NUMERIC(15,6) fixed-point: decimal_val for pi row
        $rows = $this->ztdQuery(
            "SELECT decimal_val FROM pg_num_measurements WHERE label = 'pi'"
        );
        $this->assertCount(1, $rows);
        $this->assertEqualsWithDelta(3.141593, (float) $rows[0]['decimal_val'], 1e-6);
    }

    /**
     * Arithmetic expressions in SELECT preserve precision through the CTE rewriter.
     */
    public function testArithmeticInSelect(): void
    {
        $rows = $this->ztdQuery(
            "SELECT label,
                    int_val::BIGINT + small_val::BIGINT AS int_sum,
                    float_val * 2.0 AS float_doubled,
                    decimal_val + decimal_val AS decimal_doubled
             FROM pg_num_measurements
             WHERE label IN ('max_int32', 'pi')
             ORDER BY id"
        );

        $this->assertCount(2, $rows);

        // max_int32: 2147483647 + 100 = 2147483747 (cast to BIGINT to avoid INT32 overflow)
        $this->assertSame('max_int32', $rows[0]['label']);
        $this->assertEquals(2147483747, (int) $rows[0]['int_sum']);
        // decimal: 1000000.000001 + 1000000.000001 = 2000000.000002
        $this->assertEqualsWithDelta(2000000.000002, (float) $rows[0]['decimal_doubled'], 1e-6);

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
                    SUM(int_val) AS int_sum,
                    SUM(decimal_val) AS decimal_sum
             FROM pg_num_measurements
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

        // SUM of decimal_val: 1000000.000001 + (-50000.500000) + 3.141593 + 0.000001 + 0.000000
        $expectedDecimalSum = 1000000.000001 + (-50000.500000) + 3.141593 + 0.000001 + 0.000000;
        $this->assertEqualsWithDelta($expectedDecimalSum, (float) $rows[0]['decimal_sum'], 1e-4);
    }

    /**
     * Comparison operators with precise float values in WHERE clause.
     */
    public function testComparisonWithPreciseValues(): void
    {
        // Should match pi (3.14159...) but not 1.0 or -273.15
        $rows = $this->ztdQuery(
            "SELECT label, float_val FROM pg_num_measurements
             WHERE float_val > 3.14159 AND float_val < 3.14160
             ORDER BY id"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('pi', $rows[0]['label']);

        // NUMERIC comparison: exact match on decimal_val
        $rows = $this->ztdQuery(
            "SELECT label FROM pg_num_measurements
             WHERE decimal_val = 0.000001"
        );
        $this->assertCount(1, $rows);
        $this->assertSame('tiny', $rows[0]['label']);
    }

    /**
     * Division behavior: integer division vs float division in PostgreSQL.
     */
    public function testDivisionPrecision(): void
    {
        // PostgreSQL integer division: 7 / 2 = 3 (truncates toward zero)
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
                    float_val / 3.0 AS float_divided,
                    decimal_val / 3.0 AS decimal_divided
             FROM pg_num_measurements
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
        // 3.141593 / 3.0 = 1.047197666...
        $this->assertEqualsWithDelta(
            1.047197666667,
            (float) $rows[0]['decimal_divided'],
            1e-4
        );
    }

    /**
     * UPDATE with arithmetic: precision preserved after mutation.
     */
    public function testUpdateWithArithmeticPrecision(): void
    {
        // Multiply the pi float_val by 1.1
        $this->pdo->exec(
            "UPDATE pg_num_measurements SET float_val = float_val * 1.1 WHERE label = 'pi'"
        );

        $rows = $this->ztdQuery(
            "SELECT float_val FROM pg_num_measurements WHERE label = 'pi'"
        );

        $this->assertCount(1, $rows);
        // 3.141592653589793 * 1.1 = 3.4557519189487724
        $this->assertEqualsWithDelta(
            3.4557519189487724,
            (float) $rows[0]['float_val'],
            1e-10,
            'Float precision must hold after UPDATE SET val = val * 1.1'
        );

        // Verify NUMERIC column arithmetic in UPDATE
        $this->pdo->exec(
            "UPDATE pg_num_measurements SET decimal_val = decimal_val * 1.1 WHERE label = 'max_int32'"
        );

        $rows = $this->ztdQuery(
            "SELECT decimal_val FROM pg_num_measurements WHERE label = 'max_int32'"
        );
        $this->assertCount(1, $rows);
        // 1000000.000001 * 1.1 = 1100000.000001 (NUMERIC preserves scale)
        $this->assertEqualsWithDelta(
            1100000.000001,
            (float) $rows[0]['decimal_val'],
            1e-4
        );
    }

    /**
     * Physical isolation: shadow store data must not appear in the physical table.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();

        $rows = $this->pdo->query(
            "SELECT COUNT(*) AS cnt FROM pg_num_measurements"
        )->fetchAll(PDO::FETCH_ASSOC);

        $this->assertEquals(0, (int) $rows[0]['cnt'], 'Physical table must be empty; all rows live in shadow store');
    }
}
