<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests type handling in the shadow store on MySQL PDO.
 * @spec SPEC-3.4
 */
class MysqlTypeHandlingTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE type_test (id INT PRIMARY KEY, float_val DOUBLE, bool_val TINYINT, date_val DATE, long_text TEXT)';
    }

    protected function getTableNames(): array
    {
        return ['type_test'];
    }


    public function testFloatPrecision(): void
    {
        $this->pdo->exec("INSERT INTO type_test (id, float_val) VALUES (1, 3.14159265358979)");

        $stmt = $this->pdo->query('SELECT float_val FROM type_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertEqualsWithDelta(3.14159265358979, (float) $row['float_val'], 0.0001);
    }

    public function testDateStringStorage(): void
    {
        $this->pdo->exec("INSERT INTO type_test (id, date_val) VALUES (1, '2026-03-07')");

        $stmt = $this->pdo->query('SELECT date_val FROM type_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('2026-03-07', $row['date_val']);
    }

    public function testLongTextStorage(): void
    {
        $longText = str_repeat('abcdefghij', 500);
        $stmt = $this->pdo->prepare('INSERT INTO type_test (id, long_text) VALUES (?, ?)');
        $stmt->execute([1, $longText]);

        $select = $this->pdo->query('SELECT long_text FROM type_test WHERE id = 1');
        $row = $select->fetch(PDO::FETCH_ASSOC);
        $this->assertSame($longText, $row['long_text']);
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
    }

    public function testEmptyStringDistinctFromNull(): void
    {
        $this->pdo->exec("INSERT INTO type_test (id, long_text) VALUES (1, '')");
        $this->pdo->exec("INSERT INTO type_test (id, long_text) VALUES (2, NULL)");

        $stmt = $this->pdo->query("SELECT id FROM type_test WHERE long_text = ''");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);

        $stmt2 = $this->pdo->query("SELECT id FROM type_test WHERE long_text IS NULL");
        $rows2 = $stmt2->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows2);
    }
}
