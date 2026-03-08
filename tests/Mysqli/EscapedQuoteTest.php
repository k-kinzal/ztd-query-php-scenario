<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests that doubled-quote escaping ('') works correctly in MySQLi ZTD.
 *
 * MySQL supports both '' and \' for escaping single quotes.
 * This test verifies the MySQL parser handles these correctly via MySQLi adapter.
 * @spec pending
 */
class EscapedQuoteTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_eq_quote (id INT PRIMARY KEY, body TEXT, notes TEXT)';
    }

    protected function getTableNames(): array
    {
        return ['mi_eq_quote'];
    }


    public function testInsertWithDoubledQuotes(): void
    {
        $this->mysqli->query("INSERT INTO mi_eq_quote (id, body, notes) VALUES (1, 'It''s a test', 'note')");

        $result = $this->mysqli->query('SELECT body FROM mi_eq_quote WHERE id = 1');
        $this->assertSame("It's a test", $result->fetch_assoc()['body']);
    }

    public function testUpdateWithDoubledQuotesInSetValue(): void
    {
        $this->mysqli->query("INSERT INTO mi_eq_quote (id, body, notes) VALUES (2, 'original', 'note')");
        $this->mysqli->query("UPDATE mi_eq_quote SET body = 'it''s updated' WHERE id = 2");

        $result = $this->mysqli->query('SELECT body FROM mi_eq_quote WHERE id = 2');
        $this->assertSame("it's updated", $result->fetch_assoc()['body']);
    }

    public function testDeleteWithDoubledQuotesInWhere(): void
    {
        $this->mysqli->query("INSERT INTO mi_eq_quote (id, body, notes) VALUES (3, 'Bob''s item', 'x')");
        $this->mysqli->query("INSERT INTO mi_eq_quote (id, body, notes) VALUES (4, 'plain', 'y')");

        $this->mysqli->query("DELETE FROM mi_eq_quote WHERE body = 'Bob''s item'");

        $result = $this->mysqli->query('SELECT COUNT(*) as cnt FROM mi_eq_quote');
        $this->assertSame(1, (int) $result->fetch_assoc()['cnt']);
    }

    public function testMultipleDoubledQuotes(): void
    {
        $this->mysqli->query("INSERT INTO mi_eq_quote (id, body, notes) VALUES (5, 'it''s', 'she''s')");

        $result = $this->mysqli->query('SELECT body, notes FROM mi_eq_quote WHERE id = 5');
        $row = $result->fetch_assoc();
        $this->assertSame("it's", $row['body']);
        $this->assertSame("she's", $row['notes']);
    }
}
