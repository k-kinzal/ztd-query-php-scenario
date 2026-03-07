<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\PostgreSQLContainer;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests getColumnMeta() and FETCH_NAMED mode on PostgreSQL ZTD PDO.
 */
class PostgresColumnMetaAndNamedFetchTest extends TestCase
{
    private ZtdPdo $pdo;

    public static function setUpBeforeClass(): void
    {
        $container = (new PostgreSQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS cm_right_pg');
        $raw->exec('DROP TABLE IF EXISTS cm_left_pg');
        $raw->exec('CREATE TABLE cm_left_pg (id INT PRIMARY KEY, name VARCHAR(50))');
        $raw->exec('CREATE TABLE cm_right_pg (id INT PRIMARY KEY, name VARCHAR(50), left_id INT)');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );

        $this->pdo->exec("INSERT INTO cm_left_pg VALUES (1, 'LeftAlice')");
        $this->pdo->exec("INSERT INTO cm_right_pg VALUES (1, 'RightX', 1)");
        $this->pdo->exec("INSERT INTO cm_right_pg VALUES (2, 'RightY', 1)");
    }

    public function testGetColumnMetaReturnsInfo(): void
    {
        $stmt = $this->pdo->query('SELECT id, name FROM cm_left_pg WHERE id = 1');
        $meta0 = $stmt->getColumnMeta(0);
        $meta1 = $stmt->getColumnMeta(1);

        $this->assertSame('id', $meta0['name']);
        $this->assertSame('name', $meta1['name']);
    }

    public function testColumnCountAfterJoin(): void
    {
        $stmt = $this->pdo->query(
            'SELECT l.id AS lid, l.name AS lname, r.id AS rid, r.name AS rname
             FROM cm_left_pg l
             JOIN cm_right_pg r ON r.left_id = l.id
             ORDER BY l.id, r.id'
        );
        $this->assertSame(4, $stmt->columnCount());

        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('LeftAlice', $rows[0]['lname']);
        $this->assertSame('RightX', $rows[0]['rname']);
    }

    public function testFetchAssocWithAliasedColumns(): void
    {
        $stmt = $this->pdo->query(
            'SELECT l.name AS author, r.name AS item
             FROM cm_left_pg l
             JOIN cm_right_pg r ON r.left_id = l.id
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
                    (SELECT COUNT(*) FROM cm_right_pg r WHERE r.left_id = l.id) AS item_count
             FROM cm_left_pg l
             ORDER BY l.id'
        );
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(2, (int) $rows[0]['item_count']);
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS cm_right_pg');
        $raw->exec('DROP TABLE IF EXISTS cm_left_pg');
    }
}
