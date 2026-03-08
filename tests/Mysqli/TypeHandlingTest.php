<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests type handling in the shadow store on MySQLi.
 * @spec pending
 */
class TypeHandlingTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_type_test (id INT PRIMARY KEY, float_val DOUBLE, bool_val TINYINT, date_val DATE, long_text TEXT)';
    }

    protected function getTableNames(): array
    {
        return ['mi_type_test'];
    }


    public function testFloatPrecision(): void
    {
        $this->mysqli->query("INSERT INTO mi_type_test (id, float_val) VALUES (1, 3.14159265358979)");

        $result = $this->mysqli->query('SELECT float_val FROM mi_type_test WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertEqualsWithDelta(3.14159265358979, (float) $row['float_val'], 0.0001);
    }

    public function testDateStringStorage(): void
    {
        $this->mysqli->query("INSERT INTO mi_type_test (id, date_val) VALUES (1, '2026-03-07')");

        $result = $this->mysqli->query('SELECT date_val FROM mi_type_test WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertSame('2026-03-07', $row['date_val']);
    }

    public function testLongTextStorage(): void
    {
        $longText = str_repeat('abcdefghij', 500);
        $stmt = $this->mysqli->prepare('INSERT INTO mi_type_test (id, long_text) VALUES (?, ?)');
        $id = 1;
        $stmt->bind_param('is', $id, $longText);
        $stmt->execute();

        $result = $this->mysqli->query('SELECT long_text FROM mi_type_test WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertSame($longText, $row['long_text']);
    }

    public function testUnicodeStrings(): void
    {
        $text = '日本語テスト 🎉 émojis café';
        $stmt = $this->mysqli->prepare('INSERT INTO mi_type_test (id, long_text) VALUES (?, ?)');
        $id = 1;
        $stmt->bind_param('is', $id, $text);
        $stmt->execute();

        $result = $this->mysqli->query('SELECT long_text FROM mi_type_test WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertSame($text, $row['long_text']);
    }

    public function testMultiRowInsert(): void
    {
        $this->mysqli->query("INSERT INTO mi_type_test (id, float_val) VALUES (1, 1.1), (2, 2.2), (3, 3.3)");

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_type_test');
        $row = $result->fetch_assoc();
        $this->assertSame(3, (int) $row['cnt']);
    }

    public function testNegativeNumbers(): void
    {
        $this->mysqli->query("INSERT INTO mi_type_test (id, float_val) VALUES (1, -42.5)");

        $result = $this->mysqli->query('SELECT float_val FROM mi_type_test WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertEqualsWithDelta(-42.5, (float) $row['float_val'], 0.01);
    }
}
