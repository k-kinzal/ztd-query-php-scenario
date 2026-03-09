<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests type edge cases in the shadow store on SQLite.
 *
 * Exercises boundary conditions for data types: NULL vs empty string,
 * negative values, zero, large numbers, and special string values
 * that could confuse the CTE rewriter.
 * @spec SPEC-3.1
 */
class SqliteTypeEdgeCasesTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE te_data (id INT PRIMARY KEY, str_val VARCHAR(200), int_val INT, real_val REAL)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['te_data'];
    }

    /**
     * NULL vs empty string: should be distinguished.
     */
    public function testNullVsEmptyString(): void
    {
        $this->pdo->exec("INSERT INTO te_data VALUES (1, NULL, NULL, NULL)");
        $this->pdo->exec("INSERT INTO te_data VALUES (2, '', 0, 0.0)");

        $rows = $this->ztdQuery('SELECT * FROM te_data ORDER BY id');
        $this->assertCount(2, $rows);

        // id=1: all NULL
        $this->assertNull($rows[0]['str_val']);
        $this->assertNull($rows[0]['int_val']);
        $this->assertNull($rows[0]['real_val']);

        // id=2: empty string and zero (not NULL)
        $this->assertSame('', $rows[1]['str_val']);
        $this->assertNotNull($rows[1]['int_val']);
        $this->assertNotNull($rows[1]['real_val']);
    }

    /**
     * IS NULL filtering on shadow data.
     */
    public function testIsNullFiltering(): void
    {
        $this->pdo->exec("INSERT INTO te_data VALUES (1, NULL, NULL, NULL)");
        $this->pdo->exec("INSERT INTO te_data VALUES (2, 'test', 1, 1.0)");

        $rows = $this->ztdQuery('SELECT id FROM te_data WHERE str_val IS NULL');
        $this->assertCount(1, $rows);
        $this->assertSame('1', (string) $rows[0]['id']);

        $rows = $this->ztdQuery('SELECT id FROM te_data WHERE str_val IS NOT NULL');
        $this->assertCount(1, $rows);
        $this->assertSame('2', (string) $rows[0]['id']);
    }

    /**
     * Negative numbers.
     */
    public function testNegativeNumbers(): void
    {
        $this->pdo->exec("INSERT INTO te_data VALUES (1, 'neg', -100, -3.14)");

        $rows = $this->ztdQuery('SELECT * FROM te_data WHERE id = 1');
        $this->assertSame('-100', (string) $rows[0]['int_val']);
        $this->assertEquals(-3.14, (float) $rows[0]['real_val'], '', 0.001);
    }

    /**
     * Zero values.
     */
    public function testZeroValues(): void
    {
        $this->pdo->exec("INSERT INTO te_data VALUES (1, '0', 0, 0.0)");

        $rows = $this->ztdQuery('SELECT * FROM te_data WHERE id = 1');
        $this->assertSame('0', $rows[0]['str_val']);
        $this->assertSame('0', (string) $rows[0]['int_val']);
    }

    /**
     * String containing SQL keywords.
     */
    public function testStringsWithSqlKeywords(): void
    {
        $this->pdo->exec("INSERT INTO te_data VALUES (1, 'SELECT * FROM table', 0, 0)");
        $this->pdo->exec("INSERT INTO te_data VALUES (2, 'DROP TABLE; --', 0, 0)");
        $this->pdo->exec("INSERT INTO te_data VALUES (3, 'WHERE 1=1 OR true', 0, 0)");

        $rows = $this->ztdQuery('SELECT str_val FROM te_data ORDER BY id');
        $this->assertCount(3, $rows);
        $this->assertSame('SELECT * FROM table', $rows[0]['str_val']);
        $this->assertSame('DROP TABLE; --', $rows[1]['str_val']);
        $this->assertSame('WHERE 1=1 OR true', $rows[2]['str_val']);
    }

    /**
     * String containing single quotes (escaped).
     */
    public function testStringsWithSingleQuotes(): void
    {
        $this->pdo->exec("INSERT INTO te_data VALUES (1, 'it''s a test', 0, 0)");
        $this->pdo->exec("INSERT INTO te_data VALUES (2, 'O''Brien', 0, 0)");

        $rows = $this->ztdQuery('SELECT str_val FROM te_data ORDER BY id');
        $this->assertCount(2, $rows);
        $this->assertSame("it's a test", $rows[0]['str_val']);
        $this->assertSame("O'Brien", $rows[1]['str_val']);
    }

    /**
     * Prepared statement with NULL parameter.
     */
    public function testPreparedWithNullParam(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO te_data VALUES (?, ?, ?, ?)');
        $stmt->execute([1, null, null, null]);

        $rows = $this->ztdQuery('SELECT * FROM te_data WHERE id = 1');
        $this->assertCount(1, $rows);
        $this->assertNull($rows[0]['str_val']);
    }

    /**
     * UPDATE NULL to value and value to NULL.
     */
    public function testUpdateNullToValueAndBack(): void
    {
        $this->pdo->exec("INSERT INTO te_data VALUES (1, NULL, NULL, NULL)");

        // Set to value
        $this->pdo->exec("UPDATE te_data SET str_val = 'hello', int_val = 42 WHERE id = 1");
        $rows = $this->ztdQuery('SELECT * FROM te_data WHERE id = 1');
        $this->assertSame('hello', $rows[0]['str_val']);
        $this->assertSame('42', (string) $rows[0]['int_val']);

        // Set back to NULL
        $this->pdo->exec('UPDATE te_data SET str_val = NULL, int_val = NULL WHERE id = 1');
        $rows = $this->ztdQuery('SELECT * FROM te_data WHERE id = 1');
        $this->assertNull($rows[0]['str_val']);
        $this->assertNull($rows[0]['int_val']);
    }

    /**
     * COALESCE on shadow data with NULLs.
     */
    public function testCoalesceOnShadowData(): void
    {
        $this->pdo->exec("INSERT INTO te_data VALUES (1, NULL, NULL, NULL)");
        $this->pdo->exec("INSERT INTO te_data VALUES (2, 'hello', 42, 3.14)");

        $rows = $this->ztdQuery(
            "SELECT id, COALESCE(str_val, 'default') AS str, COALESCE(int_val, -1) AS num
             FROM te_data ORDER BY id"
        );

        $this->assertSame('default', $rows[0]['str']);
        $this->assertSame('-1', (string) $rows[0]['num']);
        $this->assertSame('hello', $rows[1]['str']);
        $this->assertSame('42', (string) $rows[1]['num']);
    }

    /**
     * DISTINCT on shadow data.
     */
    public function testDistinctOnShadowData(): void
    {
        $this->pdo->exec("INSERT INTO te_data VALUES (1, 'a', 1, 0)");
        $this->pdo->exec("INSERT INTO te_data VALUES (2, 'b', 1, 0)");
        $this->pdo->exec("INSERT INTO te_data VALUES (3, 'c', 2, 0)");
        $this->pdo->exec("INSERT INTO te_data VALUES (4, 'd', 2, 0)");

        $rows = $this->ztdQuery('SELECT DISTINCT int_val FROM te_data ORDER BY int_val');
        $this->assertCount(2, $rows);
        $this->assertSame('1', (string) $rows[0]['int_val']);
        $this->assertSame('2', (string) $rows[1]['int_val']);
    }

    /**
     * Physical table isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO te_data VALUES (1, 'test', 1, 1.0)");
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM te_data');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
