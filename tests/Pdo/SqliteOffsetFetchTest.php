<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests LIMIT/OFFSET syntax variations on SQLite ZTD.
 *
 * SQLite supports:
 *   LIMIT x OFFSET y (traditional)
 *   LIMIT x, y (MySQL-compatible, x is offset, y is limit)
 * SQLite 3.35+ does NOT support OFFSET...FETCH (SQL:2012) syntax.
 * @spec SPEC-3.1
 */
class SqliteOffsetFetchTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE of_test (id INTEGER PRIMARY KEY, name TEXT, score INT)';
    }

    protected function getTableNames(): array
    {
        return ['of_test'];
    }



    protected function setUp(): void
    {
        parent::setUp();

        for ($i = 1; $i <= 10; $i++) {
            $this->pdo->exec("INSERT INTO of_test (id, name, score) VALUES ($i, 'User_$i', " . ($i * 10) . ")");
        }
    }
    /**
     * Traditional LIMIT/OFFSET.
     */
    public function testLimitOffset(): void
    {
        $stmt = $this->pdo->query('SELECT id FROM of_test ORDER BY id LIMIT 3 OFFSET 2');
        $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertSame([3, 4, 5], array_map('intval', $rows));
    }

    /**
     * LIMIT only (no offset).
     */
    public function testLimitOnly(): void
    {
        $stmt = $this->pdo->query('SELECT id FROM of_test ORDER BY id LIMIT 3');
        $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertSame([1, 2, 3], array_map('intval', $rows));
    }

    /**
     * OFFSET only (SQLite supports this as LIMIT -1 OFFSET y).
     */
    public function testOffsetOnly(): void
    {
        $stmt = $this->pdo->query('SELECT id FROM of_test ORDER BY id LIMIT -1 OFFSET 7');
        $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertSame([8, 9, 10], array_map('intval', $rows));
    }

    /**
     * LIMIT with WHERE clause.
     */
    public function testLimitWithWhere(): void
    {
        $stmt = $this->pdo->query('SELECT id FROM of_test WHERE score >= 50 ORDER BY id LIMIT 2 OFFSET 1');
        $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertSame([6, 7], array_map('intval', $rows));
    }

    /**
     * LIMIT 0 returns no rows.
     */
    public function testLimitZero(): void
    {
        $stmt = $this->pdo->query('SELECT id FROM of_test ORDER BY id LIMIT 0');
        $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertSame([], $rows);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM of_test');
        $this->assertSame(10, (int) $stmt->fetchColumn());

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM of_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
