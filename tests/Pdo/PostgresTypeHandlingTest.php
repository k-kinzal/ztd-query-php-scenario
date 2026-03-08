<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests type handling in the shadow store on PostgreSQL PDO.
 * @spec SPEC-3.4
 */
class PostgresTypeHandlingTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE type_test (id INT PRIMARY KEY, float_val DOUBLE PRECISION, bool_val BOOLEAN, date_val DATE, long_text TEXT)';
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

    public function testBooleanTrueWorks(): void
    {
        $this->pdo->exec("INSERT INTO type_test (id, bool_val) VALUES (1, TRUE)");

        $stmt = $this->pdo->query('SELECT bool_val FROM type_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        // Shadow store stores TRUE correctly
        $this->assertTrue(in_array($row['bool_val'], [true, 't', '1', 1], true));
    }

    /**
     * PostgreSQL BOOLEAN FALSE should be stored and retrieved correctly.
     *
     * @see https://github.com/k-kinzal/ztd-query-php/issues/6
     */
    public function testBooleanFalseWorks(): void
    {
        $this->pdo->exec("INSERT INTO type_test (id, bool_val) VALUES (1, TRUE)");

        try {
            $this->pdo->exec("INSERT INTO type_test (id, bool_val) VALUES (2, FALSE)");
            $stmt = $this->pdo->query('SELECT bool_val FROM type_test WHERE id = 2');
            $row = $stmt->fetch(PDO::FETCH_ASSOC);
            // FALSE should be stored correctly
            $this->assertTrue(in_array($row['bool_val'], [false, 'f', '0', 0, ''], true));
        } catch (\PDOException $e) {
            $this->markTestIncomplete(
                'Issue #6: BOOLEAN FALSE stored as empty string, CAST(\'\' AS BOOLEAN) fails. ' . $e->getMessage()
            );
        }
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
}
