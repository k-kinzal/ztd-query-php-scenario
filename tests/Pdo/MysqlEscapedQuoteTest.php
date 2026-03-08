<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests that doubled-quote escaping ('') works correctly in MySQL PDO ZTD.
 *
 * MySQL supports both '' and \' for escaping single quotes.
 * This test verifies the MySQL parser handles these correctly.
 * @spec SPEC-4.11
 */
class MysqlEscapedQuoteTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE my_eq_test (id INT PRIMARY KEY, body TEXT, notes TEXT)';
    }

    protected function getTableNames(): array
    {
        return ['my_eq_test'];
    }


    public function testInsertWithDoubledQuotes(): void
    {
        $this->pdo->exec("INSERT INTO my_eq_test (id, body, notes) VALUES (1, 'It''s a test', 'note')");

        $stmt = $this->pdo->query('SELECT body FROM my_eq_test WHERE id = 1');
        $this->assertSame("It's a test", $stmt->fetchColumn());
    }

    public function testUpdateWithDoubledQuotesInSetValue(): void
    {
        $this->pdo->exec("INSERT INTO my_eq_test (id, body, notes) VALUES (2, 'original', 'note')");
        $this->pdo->exec("UPDATE my_eq_test SET body = 'it''s updated' WHERE id = 2");

        $stmt = $this->pdo->query('SELECT body FROM my_eq_test WHERE id = 2');
        $this->assertSame("it's updated", $stmt->fetchColumn());
    }

    public function testDeleteWithDoubledQuotesInWhere(): void
    {
        $this->pdo->exec("INSERT INTO my_eq_test (id, body, notes) VALUES (3, 'Bob''s item', 'x')");
        $this->pdo->exec("INSERT INTO my_eq_test (id, body, notes) VALUES (4, 'plain', 'y')");

        $this->pdo->exec("DELETE FROM my_eq_test WHERE body = 'Bob''s item'");

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM my_eq_test');
        $this->assertSame(1, (int) $stmt->fetchColumn());
    }

    public function testMultipleDoubledQuotes(): void
    {
        $this->pdo->exec("INSERT INTO my_eq_test (id, body, notes) VALUES (5, 'it''s', 'she''s')");

        $stmt = $this->pdo->query('SELECT body, notes FROM my_eq_test WHERE id = 5');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame("it's", $row['body']);
        $this->assertSame("she's", $row['notes']);
    }
}
