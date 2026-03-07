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
 * Tests DDL operations mid-session on MySQL ZTD PDO.
 */
class MysqlDdlMidSessionTest extends TestCase
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
        $raw->exec('DROP TABLE IF EXISTS ddl_ms_mysql');
        $raw->exec('CREATE TABLE ddl_ms_mysql (id INT PRIMARY KEY, name VARCHAR(50), score INT)');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
    }

    public function testDropTableClearsShadowAndFallsToPhysical(): void
    {
        $this->pdo->exec("INSERT INTO ddl_ms_mysql VALUES (1, 'Alice', 100)");

        $this->pdo->exec('DROP TABLE ddl_ms_mysql');

        // On MySQL, the physical table still exists, so after DROP the query falls through
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM ddl_ms_mysql');
        $count = (int) $stmt->fetchColumn();
        // Physical table has 0 rows (shadow INSERT didn't reach it)
        $this->assertSame(0, $count);
    }

    public function testDropAndRecreateTableInShadow(): void
    {
        $this->pdo->exec("INSERT INTO ddl_ms_mysql VALUES (1, 'Alice', 100)");

        $this->pdo->exec('DROP TABLE ddl_ms_mysql');

        // On MySQL, DROP clears the shadow knowledge of the table,
        // and CREATE TABLE succeeds (creates a new shadow table even though physical table exists)
        $this->pdo->exec('CREATE TABLE ddl_ms_mysql (id INT PRIMARY KEY, name VARCHAR(50))');
        $this->pdo->exec("INSERT INTO ddl_ms_mysql VALUES (1, 'NewAlice')");

        $stmt = $this->pdo->query('SELECT name FROM ddl_ms_mysql WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('NewAlice', $row['name']);
    }

    public function testShadowDataPersistenceAcrossTableDrop(): void
    {
        // Create a shadow-only table
        $this->pdo->exec('CREATE TABLE ddl_other_m (id INT PRIMARY KEY, tag VARCHAR(20))');
        $this->pdo->exec("INSERT INTO ddl_other_m VALUES (1, 'important')");

        // Insert into physical-reflected table
        $this->pdo->exec("INSERT INTO ddl_ms_mysql VALUES (1, 'Alice', 100)");

        // Drop the physical table from shadow
        $this->pdo->exec('DROP TABLE ddl_ms_mysql');

        // Shadow-only table data should be unaffected
        $stmt = $this->pdo->query('SELECT tag FROM ddl_other_m WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('important', $row['tag']);
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS ddl_ms_mysql');
    }
}
