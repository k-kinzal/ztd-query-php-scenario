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
 *
 * MySQL rewriter treats SAVEPOINT/RELEASE as unparseable SQL and throws.
 * ROLLBACK TO SAVEPOINT throws "Statement type not supported."
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

    public function testSavepointThrowsUnparseableException(): void
    {
        $this->pdo->beginTransaction();

        $this->expectException(ZtdPdoException::class);
        $this->expectExceptionMessage('Empty or unparseable SQL');
        $this->pdo->exec('SAVEPOINT sp1');
    }

    public function testReleaseSavepointThrowsUnparseableException(): void
    {
        $this->expectException(ZtdPdoException::class);
        $this->expectExceptionMessage('Empty or unparseable SQL');
        $this->pdo->exec('RELEASE SAVEPOINT sp1');
    }

    public function testRollbackToSavepointThrowsUnsupportedSqlException(): void
    {
        $this->expectException(ZtdPdoException::class);
        $this->expectExceptionMessage('Statement type not supported');
        $this->pdo->exec('ROLLBACK TO SAVEPOINT sp1');
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
