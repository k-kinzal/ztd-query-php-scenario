<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests PDOStatement::nextRowset() behavior with ZTD.
 *
 * Discovery: nextRowset() delegates to the underlying PDO driver.
 * - SQLite: throws PDOException "Driver does not support this function"
 * - MySQL/PostgreSQL: returns false (no additional result sets from CTE queries)
 */
class SqliteNextRowsetTest extends TestCase
{
    private ZtdPdo $pdo;

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);

        $this->pdo->exec('CREATE TABLE nr_test (id INT PRIMARY KEY, name VARCHAR(50))');
        $this->pdo->exec("INSERT INTO nr_test VALUES (1, 'Alice')");
        $this->pdo->exec("INSERT INTO nr_test VALUES (2, 'Bob')");
    }

    /**
     * SQLite does not support multiple rowsets — nextRowset() throws.
     */
    public function testNextRowsetThrowsOnSqlite(): void
    {
        $stmt = $this->pdo->query('SELECT * FROM nr_test ORDER BY id');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);

        $this->expectException(\PDOException::class);
        $this->expectExceptionMessage('does not support');
        $stmt->nextRowset();
    }

    /**
     * Prepared statement also throws on nextRowset().
     */
    public function testNextRowsetThrowsOnPrepared(): void
    {
        $stmt = $this->pdo->prepare('SELECT * FROM nr_test WHERE id = ?');
        $stmt->execute([1]);
        $stmt->fetch(PDO::FETCH_ASSOC);

        $this->expectException(\PDOException::class);
        $this->expectExceptionMessage('does not support');
        $stmt->nextRowset();
    }
}
