<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Type preservation through CTE shadow store on MySQL via MySQLi.
 * Verifies integers, floats, strings, NULLs, empty strings, and edge values.
 * @spec SPEC-3.1, SPEC-4.1
 */
class TypeRoundtripTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_tr_data (id INT PRIMARY KEY, int_val INT, bigint_val BIGINT, float_val DOUBLE, dec_val DECIMAL(12,4), str_val VARCHAR(255), text_val TEXT, bool_val TINYINT(1), nullable_val VARCHAR(50) NULL)';
    }

    protected function getTableNames(): array
    {
        return ['mi_tr_data'];
    }

    public function testIntegerValues(): void
    {
        $this->mysqli->query("INSERT INTO mi_tr_data (id, int_val) VALUES (1, 0)");
        $this->mysqli->query("INSERT INTO mi_tr_data (id, int_val) VALUES (2, 1)");
        $this->mysqli->query("INSERT INTO mi_tr_data (id, int_val) VALUES (3, -1)");
        $this->mysqli->query("INSERT INTO mi_tr_data (id, int_val) VALUES (4, 2147483647)");
        $this->mysqli->query("INSERT INTO mi_tr_data (id, int_val) VALUES (5, -2147483648)");

        $rows = $this->ztdQuery('SELECT id, int_val FROM mi_tr_data ORDER BY id');
        $this->assertSame(0, (int) $rows[0]['int_val']);
        $this->assertSame(1, (int) $rows[1]['int_val']);
        $this->assertSame(-1, (int) $rows[2]['int_val']);
        $this->assertSame(2147483647, (int) $rows[3]['int_val']);
        $this->assertSame(-2147483648, (int) $rows[4]['int_val']);
    }

    public function testFloatValues(): void
    {
        $this->mysqli->query("INSERT INTO mi_tr_data (id, float_val) VALUES (1, 0.0)");
        $this->mysqli->query("INSERT INTO mi_tr_data (id, float_val) VALUES (2, 3.14159)");
        $this->mysqli->query("INSERT INTO mi_tr_data (id, float_val) VALUES (3, -0.001)");

        $rows = $this->ztdQuery('SELECT float_val FROM mi_tr_data ORDER BY id');
        $this->assertEqualsWithDelta(0.0, (float) $rows[0]['float_val'], 0.0001);
        $this->assertEqualsWithDelta(3.14159, (float) $rows[1]['float_val'], 0.0001);
        $this->assertEqualsWithDelta(-0.001, (float) $rows[2]['float_val'], 0.0001);
    }

    public function testDecimalPrecision(): void
    {
        $this->mysqli->query("INSERT INTO mi_tr_data (id, dec_val) VALUES (1, 12345.6789)");
        $this->mysqli->query("INSERT INTO mi_tr_data (id, dec_val) VALUES (2, 0.0001)");
        $this->mysqli->query("INSERT INTO mi_tr_data (id, dec_val) VALUES (3, -99999.9999)");

        $rows = $this->ztdQuery('SELECT dec_val FROM mi_tr_data ORDER BY id');
        $this->assertSame('12345.6789', $rows[0]['dec_val']);
        $this->assertSame('0.0001', $rows[1]['dec_val']);
        $this->assertSame('-99999.9999', $rows[2]['dec_val']);
    }

    public function testStringValues(): void
    {
        $this->mysqli->query("INSERT INTO mi_tr_data (id, str_val) VALUES (1, 'hello')");
        $this->mysqli->query("INSERT INTO mi_tr_data (id, str_val) VALUES (2, '')");
        $this->mysqli->query("INSERT INTO mi_tr_data (id, str_val) VALUES (3, 'It''s a test')");
        $this->mysqli->query("INSERT INTO mi_tr_data (id, str_val) VALUES (4, '  spaces  ')");

        $rows = $this->ztdQuery('SELECT str_val FROM mi_tr_data ORDER BY id');
        $this->assertSame('hello', $rows[0]['str_val']);
        $this->assertSame('', $rows[1]['str_val']);
        $this->assertSame("It's a test", $rows[2]['str_val']);
        $this->assertSame('  spaces  ', $rows[3]['str_val']);
    }

    public function testNullValues(): void
    {
        $this->mysqli->query("INSERT INTO mi_tr_data (id, nullable_val) VALUES (1, NULL)");
        $this->mysqli->query("INSERT INTO mi_tr_data (id, nullable_val) VALUES (2, 'not null')");

        $rows = $this->ztdQuery('SELECT nullable_val FROM mi_tr_data ORDER BY id');
        $this->assertNull($rows[0]['nullable_val']);
        $this->assertSame('not null', $rows[1]['nullable_val']);
    }

    public function testNullFilteringIsNull(): void
    {
        $this->mysqli->query("INSERT INTO mi_tr_data (id, nullable_val) VALUES (1, NULL)");
        $this->mysqli->query("INSERT INTO mi_tr_data (id, nullable_val) VALUES (2, 'val')");

        $rows = $this->ztdQuery('SELECT id FROM mi_tr_data WHERE nullable_val IS NULL');
        $this->assertCount(1, $rows);
        $this->assertSame(1, (int) $rows[0]['id']);
    }

    public function testNullFilteringIsNotNull(): void
    {
        $this->mysqli->query("INSERT INTO mi_tr_data (id, nullable_val) VALUES (1, NULL)");
        $this->mysqli->query("INSERT INTO mi_tr_data (id, nullable_val) VALUES (2, 'val')");

        $rows = $this->ztdQuery('SELECT id FROM mi_tr_data WHERE nullable_val IS NOT NULL');
        $this->assertCount(1, $rows);
        $this->assertSame(2, (int) $rows[0]['id']);
    }

    public function testBooleanValues(): void
    {
        $this->mysqli->query("INSERT INTO mi_tr_data (id, bool_val) VALUES (1, 1)");
        $this->mysqli->query("INSERT INTO mi_tr_data (id, bool_val) VALUES (2, 0)");

        $rows = $this->ztdQuery('SELECT bool_val FROM mi_tr_data ORDER BY id');
        $this->assertSame(1, (int) $rows[0]['bool_val']);
        $this->assertSame(0, (int) $rows[1]['bool_val']);
    }

    public function testLargeTextValue(): void
    {
        $longText = str_repeat('abcde', 200);
        $escaped = $this->mysqli->real_escape_string($longText);
        $this->mysqli->query("INSERT INTO mi_tr_data (id, text_val) VALUES (1, '{$escaped}')");

        $rows = $this->ztdQuery('SELECT text_val FROM mi_tr_data WHERE id = 1');
        $this->assertSame($longText, $rows[0]['text_val']);
    }

    public function testUpdatePreservesType(): void
    {
        $this->mysqli->query("INSERT INTO mi_tr_data (id, int_val, str_val) VALUES (1, 10, 'original')");
        $this->mysqli->query("UPDATE mi_tr_data SET int_val = 20, str_val = 'updated' WHERE id = 1");

        $rows = $this->ztdQuery('SELECT int_val, str_val FROM mi_tr_data WHERE id = 1');
        $this->assertSame(20, (int) $rows[0]['int_val']);
        $this->assertSame('updated', $rows[0]['str_val']);
    }

    public function testPreparedTypeRoundtrip(): void
    {
        $stmt = $this->mysqli->prepare("INSERT INTO mi_tr_data (id, int_val, float_val, str_val) VALUES (?, ?, ?, ?)");
        $id = 1;
        $intVal = 42;
        $floatVal = 3.14;
        $strVal = 'prepared';
        $stmt->bind_param('iids', $id, $intVal, $floatVal, $strVal);
        $stmt->execute();

        $rows = $this->ztdQuery('SELECT int_val, float_val, str_val FROM mi_tr_data WHERE id = 1');
        $this->assertSame(42, (int) $rows[0]['int_val']);
        $this->assertEqualsWithDelta(3.14, (float) $rows[0]['float_val'], 0.001);
        $this->assertSame('prepared', $rows[0]['str_val']);
    }
}
