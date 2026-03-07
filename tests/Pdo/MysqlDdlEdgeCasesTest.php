<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Pdo\ZtdPdo;
use ZtdQuery\Adapter\Pdo\ZtdPdoException;

/**
 * Tests DDL edge cases on MySQL via PDO adapter:
 * CREATE TABLE IF NOT EXISTS, DROP TABLE IF EXISTS, TRUNCATE isolation.
 */
class MysqlDdlEdgeCasesTest extends TestCase
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
        $raw->exec('DROP TABLE IF EXISTS mysql_pdo_ddl_edge');
        $raw->exec('CREATE TABLE mysql_pdo_ddl_edge (id INT PRIMARY KEY, val VARCHAR(255))');
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

    public function testCreateTableIfNotExistsOnExistingTable(): void
    {
        // Should NOT throw because IF NOT EXISTS is specified
        $this->pdo->exec('CREATE TABLE IF NOT EXISTS mysql_pdo_ddl_edge (id INT PRIMARY KEY, val VARCHAR(255))');

        $this->pdo->exec("INSERT INTO mysql_pdo_ddl_edge (id, val) VALUES (1, 'test')");
        $stmt = $this->pdo->query('SELECT * FROM mysql_pdo_ddl_edge WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
    }

    public function testCreateTableWithoutIfNotExistsThrows(): void
    {
        $this->expectException(ZtdPdoException::class);
        $this->expectExceptionMessageMatches('/already exists/i');
        $this->pdo->exec('CREATE TABLE mysql_pdo_ddl_edge (id INT PRIMARY KEY)');
    }

    public function testDropTableIfExistsOnNonExistent(): void
    {
        // Should NOT throw because IF EXISTS is specified
        $this->pdo->exec('DROP TABLE IF EXISTS mysql_pdo_nonexistent_ddl_table');
        $this->assertTrue(true);
    }

    public function testTruncateIsolation(): void
    {
        // Insert into shadow
        $this->pdo->exec("INSERT INTO mysql_pdo_ddl_edge (id, val) VALUES (1, 'shadow_data')");

        // Truncate clears shadow data
        $this->pdo->exec('TRUNCATE TABLE mysql_pdo_ddl_edge');

        $stmt = $this->pdo->query('SELECT * FROM mysql_pdo_ddl_edge');
        $this->assertCount(0, $stmt->fetchAll(PDO::FETCH_ASSOC));

        // Physical table should still exist and be empty (it was already empty)
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT * FROM mysql_pdo_ddl_edge');
        $this->assertCount(0, $stmt->fetchAll(PDO::FETCH_ASSOC));
    }

    public function testTruncateThenInsert(): void
    {
        $this->pdo->exec("INSERT INTO mysql_pdo_ddl_edge (id, val) VALUES (1, 'before')");
        $this->pdo->exec('TRUNCATE TABLE mysql_pdo_ddl_edge');
        $this->pdo->exec("INSERT INTO mysql_pdo_ddl_edge (id, val) VALUES (2, 'after')");

        $stmt = $this->pdo->query('SELECT * FROM mysql_pdo_ddl_edge');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('after', $rows[0]['val']);
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS mysql_pdo_ddl_edge');
    }
}
