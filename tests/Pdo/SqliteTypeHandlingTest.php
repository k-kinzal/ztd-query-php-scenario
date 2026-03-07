<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests type handling in the shadow store on SQLite PDO.
 * Verifies FLOAT, BOOLEAN, DATE, and long TEXT values through CTE rewriting.
 */
class SqliteTypeHandlingTest extends TestCase
{
    private ZtdPdo $pdo;

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);

        $this->pdo->exec('CREATE TABLE type_test (id INT PRIMARY KEY, float_val REAL, bool_val INT, date_val TEXT, long_text TEXT)');
    }

    public function testFloatPrecision(): void
    {
        $this->pdo->exec("INSERT INTO type_test (id, float_val) VALUES (1, 3.14159265358979)");

        $stmt = $this->pdo->query('SELECT float_val FROM type_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertEqualsWithDelta(3.14159265358979, (float) $row['float_val'], 0.0001);
    }

    public function testFloatWithPreparedParam(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO type_test (id, float_val) VALUES (?, ?)');
        $stmt->execute([1, 99.999]);

        $select = $this->pdo->query('SELECT float_val FROM type_test WHERE id = 1');
        $row = $select->fetch(PDO::FETCH_ASSOC);
        $this->assertEqualsWithDelta(99.999, (float) $row['float_val'], 0.001);
    }

    public function testBooleanAsInteger(): void
    {
        $this->pdo->exec("INSERT INTO type_test (id, bool_val) VALUES (1, 1)");
        $this->pdo->exec("INSERT INTO type_test (id, bool_val) VALUES (2, 0)");

        $stmt = $this->pdo->query('SELECT bool_val FROM type_test WHERE id = 1');
        $this->assertSame(1, (int) $stmt->fetch(PDO::FETCH_ASSOC)['bool_val']);

        $stmt2 = $this->pdo->query('SELECT bool_val FROM type_test WHERE id = 2');
        $this->assertSame(0, (int) $stmt2->fetch(PDO::FETCH_ASSOC)['bool_val']);
    }

    public function testBooleanFilter(): void
    {
        $this->pdo->exec("INSERT INTO type_test (id, bool_val) VALUES (1, 1)");
        $this->pdo->exec("INSERT INTO type_test (id, bool_val) VALUES (2, 0)");
        $this->pdo->exec("INSERT INTO type_test (id, bool_val) VALUES (3, 1)");

        $stmt = $this->pdo->query('SELECT COUNT(*) AS cnt FROM type_test WHERE bool_val = 1');
        $this->assertSame(2, (int) $stmt->fetch(PDO::FETCH_ASSOC)['cnt']);
    }

    public function testDateStringStorage(): void
    {
        $this->pdo->exec("INSERT INTO type_test (id, date_val) VALUES (1, '2026-03-07')");

        $stmt = $this->pdo->query('SELECT date_val FROM type_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('2026-03-07', $row['date_val']);
    }

    public function testDateComparison(): void
    {
        $this->pdo->exec("INSERT INTO type_test (id, date_val) VALUES (1, '2026-01-01')");
        $this->pdo->exec("INSERT INTO type_test (id, date_val) VALUES (2, '2026-06-15')");
        $this->pdo->exec("INSERT INTO type_test (id, date_val) VALUES (3, '2026-12-31')");

        $stmt = $this->pdo->prepare('SELECT id FROM type_test WHERE date_val > ? ORDER BY id');
        $stmt->execute(['2026-03-01']);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame(2, (int) $rows[0]['id']);
        $this->assertSame(3, (int) $rows[1]['id']);
    }

    public function testLongTextStorage(): void
    {
        $longText = str_repeat('abcdefghij', 500); // 5000 chars
        $stmt = $this->pdo->prepare('INSERT INTO type_test (id, long_text) VALUES (?, ?)');
        $stmt->execute([1, $longText]);

        $select = $this->pdo->query('SELECT long_text FROM type_test WHERE id = 1');
        $row = $select->fetch(PDO::FETCH_ASSOC);
        $this->assertSame($longText, $row['long_text']);
    }

    public function testEmptyStringDistinctFromNull(): void
    {
        $this->pdo->exec("INSERT INTO type_test (id, long_text) VALUES (1, '')");
        $this->pdo->exec("INSERT INTO type_test (id, long_text) VALUES (2, NULL)");

        $stmt = $this->pdo->query("SELECT id FROM type_test WHERE long_text = ''");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame(1, (int) $rows[0]['id']);

        $stmt2 = $this->pdo->query("SELECT id FROM type_test WHERE long_text IS NULL");
        $rows2 = $stmt2->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows2);
        $this->assertSame(2, (int) $rows2[0]['id']);
    }

    public function testNegativeNumbers(): void
    {
        $this->pdo->exec("INSERT INTO type_test (id, float_val) VALUES (1, -42.5)");

        $stmt = $this->pdo->query('SELECT float_val FROM type_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertEqualsWithDelta(-42.5, (float) $row['float_val'], 0.01);
    }

    public function testZeroValues(): void
    {
        $this->pdo->exec("INSERT INTO type_test (id, float_val, bool_val) VALUES (1, 0.0, 0)");

        $stmt = $this->pdo->query('SELECT float_val, bool_val FROM type_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertEqualsWithDelta(0.0, (float) $row['float_val'], 0.001);
        $this->assertSame(0, (int) $row['bool_val']);
    }

    public function testSpecialCharactersInStrings(): void
    {
        $this->pdo->exec("INSERT INTO type_test (id, long_text) VALUES (1, 'It''s a \"test\" with <html> & special chars')");

        $stmt = $this->pdo->query('SELECT long_text FROM type_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('It\'s a "test" with <html> & special chars', $row['long_text']);
    }

    public function testUnicodeStrings(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO type_test (id, long_text) VALUES (?, ?)');
        $stmt->execute([1, '日本語テスト 🎉 émojis café']);

        $select = $this->pdo->query('SELECT long_text FROM type_test WHERE id = 1');
        $row = $select->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('日本語テスト 🎉 émojis café', $row['long_text']);
    }

    public function testMultiRowInsert(): void
    {
        $this->pdo->exec("INSERT INTO type_test (id, float_val) VALUES (1, 1.1), (2, 2.2), (3, 3.3)");

        $stmt = $this->pdo->query('SELECT COUNT(*) AS cnt FROM type_test');
        $this->assertSame(3, (int) $stmt->fetch(PDO::FETCH_ASSOC)['cnt']);

        $stmt2 = $this->pdo->query('SELECT float_val FROM type_test WHERE id = 2');
        $this->assertEqualsWithDelta(2.2, (float) $stmt2->fetch(PDO::FETCH_ASSOC)['float_val'], 0.01);
    }
}
