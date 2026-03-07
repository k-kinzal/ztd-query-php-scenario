<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests getColumnMeta() and FETCH_NAMED mode on MySQL ZTD PDO.
 */
class MysqlColumnMetaAndNamedFetchTest extends TestCase
{
    private ZtdPdo $pdo;

    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS cm_right_m');
        $raw->exec('DROP TABLE IF EXISTS cm_left_m');
        $raw->exec('CREATE TABLE cm_left_m (id INT PRIMARY KEY, name VARCHAR(50))');
        $raw->exec('CREATE TABLE cm_right_m (id INT PRIMARY KEY, name VARCHAR(50), left_id INT)');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );

        $this->pdo->exec("INSERT INTO cm_left_m VALUES (1, 'LeftAlice')");
        $this->pdo->exec("INSERT INTO cm_right_m VALUES (1, 'RightX', 1)");
        $this->pdo->exec("INSERT INTO cm_right_m VALUES (2, 'RightY', 1)");
    }

    public function testGetColumnMetaReturnsInfo(): void
    {
        $stmt = $this->pdo->query('SELECT id, name FROM cm_left_m WHERE id = 1');
        $meta0 = $stmt->getColumnMeta(0);
        $meta1 = $stmt->getColumnMeta(1);

        $this->assertSame('id', $meta0['name']);
        $this->assertSame('name', $meta1['name']);
    }

    public function testColumnCountAfterJoin(): void
    {
        $stmt = $this->pdo->query(
            'SELECT l.id AS lid, l.name AS lname, r.id AS rid, r.name AS rname
             FROM cm_left_m l
             JOIN cm_right_m r ON r.left_id = l.id
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
             FROM cm_left_m l
             JOIN cm_right_m r ON r.left_id = l.id
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
                    (SELECT COUNT(*) FROM cm_right_m r WHERE r.left_id = l.id) AS item_count
             FROM cm_left_m l
             ORDER BY l.id'
        );
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(2, (int) $rows[0]['item_count']);
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS cm_right_m');
        $raw->exec('DROP TABLE IF EXISTS cm_left_m');
    }
}
