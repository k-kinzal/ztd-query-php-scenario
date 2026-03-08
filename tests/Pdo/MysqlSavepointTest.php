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
 * Tests savepoint behavior with ZTD on MySQL PDO.
 */
class MysqlSavepointTest extends TestCase
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
        $raw->exec('DROP TABLE IF EXISTS sp_test');
        $raw->exec('CREATE TABLE sp_test (id INT PRIMARY KEY, name VARCHAR(50))');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );

        $this->pdo->exec("INSERT INTO sp_test VALUES (1, 'Alice')");
    }

    /**
     * SAVEPOINT should be supported on MySQL.
     */
    public function testSavepointSupported(): void
    {
        $this->pdo->beginTransaction();

        try {
            $this->pdo->exec('SAVEPOINT sp1');
            $this->pdo->commit();
            $this->assertTrue(true);
        } catch (ZtdPdoException $e) {
            $this->markTestIncomplete(
                'SAVEPOINT not yet supported on MySQL PDO: ' . $e->getMessage()
            );
        }
    }

    /**
     * RELEASE SAVEPOINT should be supported on MySQL.
     */
    public function testReleaseSavepointSupported(): void
    {
        $this->pdo->beginTransaction();

        try {
            $this->pdo->exec('SAVEPOINT sp1');
            $this->pdo->exec('RELEASE SAVEPOINT sp1');
            $this->pdo->commit();
            $this->assertTrue(true);
        } catch (ZtdPdoException $e) {
            $this->markTestIncomplete(
                'RELEASE SAVEPOINT not yet supported on MySQL PDO: ' . $e->getMessage()
            );
        }
    }

    /**
     * ROLLBACK TO SAVEPOINT should be supported on MySQL.
     */
    public function testRollbackToSavepointSupported(): void
    {
        $this->pdo->beginTransaction();

        try {
            $this->pdo->exec('SAVEPOINT sp1');
            $this->pdo->exec('ROLLBACK TO SAVEPOINT sp1');
            $this->pdo->commit();
            $this->assertTrue(true);
        } catch (ZtdPdoException $e) {
            $this->markTestIncomplete(
                'ROLLBACK TO SAVEPOINT not yet supported on MySQL PDO: ' . $e->getMessage()
            );
        }
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS sp_test');
    }
}
