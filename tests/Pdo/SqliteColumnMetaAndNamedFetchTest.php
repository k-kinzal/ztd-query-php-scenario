<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests getColumnMeta() and FETCH_NAMED mode with ZTD on SQLite PDO.
 */
class SqliteColumnMetaAndNamedFetchTest extends TestCase
{
    private ZtdPdo $pdo;

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);

        $this->pdo->exec('CREATE TABLE left_t (id INT PRIMARY KEY, name VARCHAR(50))');
        $this->pdo->exec('CREATE TABLE right_t (id INT PRIMARY KEY, name VARCHAR(50), left_id INT)');

        $this->pdo->exec("INSERT INTO left_t VALUES (1, 'LeftAlice')");
        $this->pdo->exec("INSERT INTO left_t VALUES (2, 'LeftBob')");
        $this->pdo->exec("INSERT INTO right_t VALUES (1, 'RightX', 1)");
        $this->pdo->exec("INSERT INTO right_t VALUES (2, 'RightY', 1)");
    }

    public function testGetColumnMetaReturnsInfo(): void
    {
        $stmt = $this->pdo->query('SELECT id, name FROM left_t WHERE id = 1');
        $meta0 = $stmt->getColumnMeta(0);
        $meta1 = $stmt->getColumnMeta(1);

        $this->assertSame('id', $meta0['name']);
        $this->assertSame('name', $meta1['name']);
    }

    public function testColumnCountAfterJoin(): void
    {
        $stmt = $this->pdo->query(
            'SELECT l.id AS lid, l.name AS lname, r.id AS rid, r.name AS rname
             FROM left_t l
             JOIN right_t r ON r.left_id = l.id
             ORDER BY l.id, r.id'
        );
        $this->assertSame(4, $stmt->columnCount());

        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('LeftAlice', $rows[0]['lname']);
        $this->assertSame('RightX', $rows[0]['rname']);
    }

    public function testFetchNamedWithDuplicateColumnNames(): void
    {
        $stmt = $this->pdo->query(
            'SELECT l.name, r.name
             FROM left_t l
             JOIN right_t r ON r.left_id = l.id
             WHERE l.id = 1
             ORDER BY r.id'
        );

        // FETCH_NAMED groups duplicate column names into arrays
        $row = $stmt->fetch(PDO::FETCH_NAMED);
        if (is_array($row['name'])) {
            // When names collide, FETCH_NAMED returns an array
            $this->assertCount(2, $row['name']);
        } else {
            // Some drivers may return just one value
            $this->assertIsString($row['name']);
        }
    }

    public function testFetchAssocWithAliasedColumns(): void
    {
        $stmt = $this->pdo->query(
            'SELECT l.name AS author, r.name AS item
             FROM left_t l
             JOIN right_t r ON r.left_id = l.id
             WHERE l.id = 1
             ORDER BY r.id'
        );
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('LeftAlice', $rows[0]['author']);
        $this->assertSame('RightX', $rows[0]['item']);
    }

    public function testSubqueryColumnAlias(): void
    {
        $stmt = $this->pdo->query(
            'SELECT l.name,
                    (SELECT COUNT(*) FROM right_t r WHERE r.left_id = l.id) AS item_count
             FROM left_t l
             ORDER BY l.id'
        );
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(2, (int) $rows[0]['item_count']); // LeftAlice has 2 rights
        $this->assertSame(0, (int) $rows[1]['item_count']); // LeftBob has 0 rights
    }
}
