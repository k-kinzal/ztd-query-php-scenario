<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests shadow store behavior with wide tables (many columns) on SQLite.
 * CTE rewriting must handle all columns correctly.
 * @spec SPEC-3.1
 */
class SqliteWideTableTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE wide20 (',
            'CREATE TABLE wide20u (',
            'CREATE TABLE wide10 (',
            'CREATE TABLE wide5 (id INT PRIMARY KEY, a VARCHAR(50), b VARCHAR(50), c VARCHAR(50), d VARCHAR(50), e VARCHAR(50))',
        ];
    }

    protected function getTableNames(): array
    {
        return ['wide20', 'wide20u', 'wide10', 'wide5'];
    }


    public function testTableWith20Columns(): void
    {
        $cols = ['id INT PRIMARY KEY'];
        for ($i = 1; $i <= 19; $i++) {
            $cols[] = "col$i VARCHAR(50)";
        }
        $this->pdo->exec('CREATE TABLE wide20 (' . implode(', ', $cols) . ')');

        // Insert a row with all columns filled
        $values = ['1'];
        $colNames = ['id'];
        for ($i = 1; $i <= 19; $i++) {
            $colNames[] = "col$i";
            $values[] = "'val$i'";
        }
        $this->pdo->exec('INSERT INTO wide20 (' . implode(', ', $colNames) . ') VALUES (' . implode(', ', $values) . ')');

        $stmt = $this->pdo->query('SELECT * FROM wide20 WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('val1', $row['col1']);
        $this->assertSame('val10', $row['col10']);
        $this->assertSame('val19', $row['col19']);
    }

    public function testTableWith20ColumnsUpdate(): void
    {
        $cols = ['id INT PRIMARY KEY'];
        for ($i = 1; $i <= 19; $i++) {
            $cols[] = "col$i VARCHAR(50)";
        }
        $this->pdo->exec('CREATE TABLE wide20u (' . implode(', ', $cols) . ')');

        $values = ['1'];
        $colNames = ['id'];
        for ($i = 1; $i <= 19; $i++) {
            $colNames[] = "col$i";
            $values[] = "'original'";
        }
        $this->pdo->exec('INSERT INTO wide20u (' . implode(', ', $colNames) . ') VALUES (' . implode(', ', $values) . ')');

        $this->pdo->exec("UPDATE wide20u SET col5 = 'modified', col15 = 'changed' WHERE id = 1");

        $stmt = $this->pdo->query('SELECT col5, col10, col15 FROM wide20u WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('modified', $row['col5']);
        $this->assertSame('original', $row['col10']);
        $this->assertSame('changed', $row['col15']);
    }

    public function testPreparedInsertWideTable(): void
    {
        $cols = ['id INT PRIMARY KEY'];
        $placeholders = ['?'];
        $colNames = ['id'];
        for ($i = 1; $i <= 10; $i++) {
            $cols[] = "col$i INT";
            $colNames[] = "col$i";
            $placeholders[] = '?';
        }
        $this->pdo->exec('CREATE TABLE wide10 (' . implode(', ', $cols) . ')');

        $stmt = $this->pdo->prepare('INSERT INTO wide10 (' . implode(', ', $colNames) . ') VALUES (' . implode(', ', $placeholders) . ')');

        $params = [1];
        for ($i = 1; $i <= 10; $i++) {
            $params[] = $i * 100;
        }
        $stmt->execute($params);

        $select = $this->pdo->query('SELECT col5, col10 FROM wide10 WHERE id = 1');
        $row = $select->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(500, (int) $row['col5']);
        $this->assertSame(1000, (int) $row['col10']);
    }

    public function testMultipleRowsWideTable(): void
    {
        $this->pdo->exec('CREATE TABLE wide5 (id INT PRIMARY KEY, a VARCHAR(50), b VARCHAR(50), c VARCHAR(50), d VARCHAR(50), e VARCHAR(50))');

        for ($i = 1; $i <= 20; $i++) {
            $this->pdo->exec("INSERT INTO wide5 VALUES ($i, 'a$i', 'b$i', 'c$i', 'd$i', 'e$i')");
        }

        $stmt = $this->pdo->query('SELECT COUNT(*) AS cnt FROM wide5');
        $this->assertSame(20, (int) $stmt->fetch(PDO::FETCH_ASSOC)['cnt']);

        $stmt2 = $this->pdo->query('SELECT a, e FROM wide5 WHERE id = 15');
        $row = $stmt2->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('a15', $row['a']);
        $this->assertSame('e15', $row['e']);
    }
}
