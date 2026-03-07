<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Pdo\ZtdPdo;
use ZtdQuery\Exception\ColumnAlreadyExistsException;
use ZtdQuery\Exception\ColumnNotFoundException;

/**
 * Tests ALTER TABLE error scenarios on MySQL ZTD:
 * duplicate column add, drop nonexistent column, modify nonexistent column.
 */
class MysqlAlterTableErrorTest extends TestCase
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
        $raw->exec('DROP TABLE IF EXISTS alt_err_m');
        $raw->exec('CREATE TABLE alt_err_m (id INT PRIMARY KEY, name VARCHAR(50), score INT)');
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

    public function testAddDuplicateColumnThrows(): void
    {
        // 'name' already exists in the table schema
        $this->expectException(ColumnAlreadyExistsException::class);
        $this->pdo->exec('ALTER TABLE alt_err_m ADD COLUMN name VARCHAR(100)');
    }

    public function testDropNonexistentColumnThrows(): void
    {
        $this->expectException(ColumnNotFoundException::class);
        $this->pdo->exec('ALTER TABLE alt_err_m DROP COLUMN nonexistent_col');
    }

    public function testModifyNonexistentColumnThrows(): void
    {
        $this->expectException(ColumnNotFoundException::class);
        $this->pdo->exec('ALTER TABLE alt_err_m MODIFY COLUMN nonexistent_col INT');
    }

    public function testChangeNonexistentColumnThrows(): void
    {
        $this->expectException(ColumnNotFoundException::class);
        $this->pdo->exec('ALTER TABLE alt_err_m CHANGE COLUMN nonexistent_col new_col INT');
    }

    public function testRenameNonexistentColumnThrows(): void
    {
        $this->expectException(ColumnNotFoundException::class);
        $this->pdo->exec('ALTER TABLE alt_err_m RENAME COLUMN nonexistent_col TO new_col');
    }

    public function testShadowStoreIntactAfterAlterError(): void
    {
        $this->pdo->exec("INSERT INTO alt_err_m VALUES (1, 'Alice', 90)");

        try {
            $this->pdo->exec('ALTER TABLE alt_err_m ADD COLUMN name VARCHAR(100)');
        } catch (ColumnAlreadyExistsException $e) {
            // Expected
        }

        // Shadow store should still have the data
        $stmt = $this->pdo->query('SELECT name FROM alt_err_m WHERE id = 1');
        $this->assertSame('Alice', $stmt->fetchColumn());
    }

    public function testSuccessfulAlterThenErrorLeavesSchemaConsistent(): void
    {
        // First, add a column successfully
        $this->pdo->exec('ALTER TABLE alt_err_m ADD COLUMN email VARCHAR(100)');

        // Then try to add it again — should error
        try {
            $this->pdo->exec('ALTER TABLE alt_err_m ADD COLUMN email VARCHAR(100)');
            $this->fail('Expected ColumnAlreadyExistsException');
        } catch (ColumnAlreadyExistsException $e) {
            // Expected
        }

        // After ALTER TABLE ADD COLUMN, schema now has 4 columns (id, name, score, email)
        // Must include the new column in INSERT
        $this->pdo->exec("INSERT INTO alt_err_m VALUES (1, 'Alice', 90, 'alice@test.com')");
        $stmt = $this->pdo->query('SELECT name, email FROM alt_err_m WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Alice', $row['name']);
        $this->assertSame('alice@test.com', $row['email']);
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS alt_err_m');
    }
}
