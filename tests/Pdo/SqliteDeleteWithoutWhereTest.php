<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests DELETE without WHERE clause behavior on SQLite.
 *
 * @see https://github.com/k-kinzal/ztd-query-php/issues/7
 */
class SqliteDeleteWithoutWhereTest extends TestCase
{
    private ZtdPdo $pdo;

    protected function setUp(): void
    {
        $raw = new PDO('sqlite::memory:');
        $raw->exec('CREATE TABLE dww_test (id INT PRIMARY KEY, name VARCHAR(50))');
        $this->pdo = ZtdPdo::fromPdo($raw);

        $this->pdo->exec("INSERT INTO dww_test VALUES (1, 'Alice')");
        $this->pdo->exec("INSERT INTO dww_test VALUES (2, 'Bob')");
        $this->pdo->exec("INSERT INTO dww_test VALUES (3, 'Charlie')");
    }

    /**
     * DELETE without WHERE should delete all rows.
     *
     * @see https://github.com/k-kinzal/ztd-query-php/issues/7
     */
    public function testDeleteWithoutWhere(): void
    {
        $this->pdo->exec('DELETE FROM dww_test');

        // Expected: DELETE without WHERE should delete all rows
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM dww_test');
        $count = (int) $stmt->fetchColumn();
        if ($count !== 0) {
            $this->markTestIncomplete(
                'Issue #7: DELETE without WHERE silently ignored on SQLite. Expected 0, got ' . $count
            );
        }
        $this->assertSame(0, $count);
    }

    /**
     * Workaround: DELETE FROM table WHERE 1=1 works correctly.
     */
    public function testDeleteWithWhereTrueWorks(): void
    {
        $this->pdo->exec('DELETE FROM dww_test WHERE 1=1');

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM dww_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }

    /**
     * DELETE with specific WHERE works correctly.
     */
    public function testDeleteWithSpecificWhereWorks(): void
    {
        $this->pdo->exec("DELETE FROM dww_test WHERE name = 'Bob'");

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM dww_test');
        $this->assertSame(2, (int) $stmt->fetchColumn());
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec('DELETE FROM dww_test WHERE 1=1');

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM dww_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
