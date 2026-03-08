<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests that doubled-quote escaping ('') works correctly in SQLite ZTD.
 *
 * SQLite uses '' for escaping single quotes in string literals.
 * This test verifies that the SQLite parser handles this correctly,
 * unlike the PostgreSQL parser which has a bug (issue #25).
 * @spec SPEC-4.11
 */
class SqliteEscapedQuoteTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE eq_test (id INTEGER PRIMARY KEY, body TEXT, notes TEXT)';
    }

    protected function getTableNames(): array
    {
        return ['eq_test'];
    }


    public function testInsertWithEscapedQuotes(): void
    {
        $this->pdo->exec("INSERT INTO eq_test (id, body, notes) VALUES (1, 'It''s a test', 'note')");

        $stmt = $this->pdo->query('SELECT body FROM eq_test WHERE id = 1');
        $this->assertSame("It's a test", $stmt->fetchColumn());
    }

    public function testUpdateWithEscapedQuotesInSetValue(): void
    {
        $this->pdo->exec("INSERT INTO eq_test (id, body, notes) VALUES (2, 'original', 'note')");
        $this->pdo->exec("UPDATE eq_test SET body = 'it''s updated' WHERE id = 2");

        $stmt = $this->pdo->query('SELECT body FROM eq_test WHERE id = 2');
        $this->assertSame("it's updated", $stmt->fetchColumn());
    }

    public function testDeleteWithEscapedQuotesInWhere(): void
    {
        $this->pdo->exec("INSERT INTO eq_test (id, body, notes) VALUES (3, 'Bob''s item', 'x')");
        $this->pdo->exec("INSERT INTO eq_test (id, body, notes) VALUES (4, 'plain', 'y')");

        $this->pdo->exec("DELETE FROM eq_test WHERE body = 'Bob''s item'");

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM eq_test');
        $this->assertSame(1, (int) $stmt->fetchColumn());
    }

    public function testWhereClauseWithEscapedQuotes(): void
    {
        $this->pdo->exec("INSERT INTO eq_test (id, body, notes) VALUES (5, 'She said ''hello''', 'quoted')");

        $stmt = $this->pdo->query("SELECT notes FROM eq_test WHERE body = 'She said ''hello'''");
        $this->assertSame('quoted', $stmt->fetchColumn());
    }

    public function testMultipleEscapedQuotesInOneStatement(): void
    {
        $this->pdo->exec("INSERT INTO eq_test (id, body, notes) VALUES (6, 'it''s', 'she''s')");

        $stmt = $this->pdo->query('SELECT body, notes FROM eq_test WHERE id = 6');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame("it's", $row['body']);
        $this->assertSame("she's", $row['notes']);
    }
}
