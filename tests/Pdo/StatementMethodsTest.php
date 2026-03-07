<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

class StatementMethodsTest extends TestCase
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
        $raw->exec('DROP TABLE IF EXISTS stmt_methods_test');
        $raw->exec('CREATE TABLE stmt_methods_test (id INT PRIMARY KEY, name VARCHAR(255), amount DECIMAL(10,2))');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );

        $this->pdo->exec("INSERT INTO stmt_methods_test (id, name, amount) VALUES (1, 'Alice', 100.50)");
        $this->pdo->exec("INSERT INTO stmt_methods_test (id, name, amount) VALUES (2, 'Bob', 200.75)");
    }

    public function testSetFetchModeOnStatement(): void
    {
        $stmt = $this->pdo->prepare('SELECT * FROM stmt_methods_test WHERE id = :id');
        $stmt->setFetchMode(PDO::FETCH_ASSOC);
        $stmt->bindValue(':id', 1);
        $stmt->execute();

        $row = $stmt->fetch();
        $this->assertIsArray($row);
        $this->assertArrayHasKey('name', $row);
        $this->assertArrayNotHasKey(0, $row);
    }

    public function testCloseCursorAllowsReExecution(): void
    {
        $stmt = $this->pdo->prepare('SELECT * FROM stmt_methods_test WHERE id = :id');

        $stmt->execute([':id' => 1]);
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Alice', $row['name']);
        $stmt->closeCursor();

        // Re-execute after closeCursor
        $stmt->execute([':id' => 2]);
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Bob', $row['name']);
    }

    public function testBindColumnBindsResultToVariable(): void
    {
        $stmt = $this->pdo->prepare('SELECT id, name FROM stmt_methods_test WHERE id = ?');
        $stmt->execute([1]);

        $id = null;
        $name = null;
        $stmt->bindColumn(1, $id);
        $stmt->bindColumn(2, $name);
        $stmt->fetch(PDO::FETCH_BOUND);

        $this->assertSame(1, (int) $id);
        $this->assertSame('Alice', $name);
    }

    public function testGetColumnMetaReturnsColumnInfo(): void
    {
        $stmt = $this->pdo->query('SELECT id, name, amount FROM stmt_methods_test WHERE id = 1');
        $meta = $stmt->getColumnMeta(0);

        $this->assertIsArray($meta);
        $this->assertSame('id', $meta['name']);
    }

    public function testColumnCountReturnsCorrectCount(): void
    {
        $stmt = $this->pdo->query('SELECT id, name, amount FROM stmt_methods_test WHERE id = 1');
        $this->assertSame(3, $stmt->columnCount());
    }

    public function testErrorCodeAndErrorInfo(): void
    {
        $stmt = $this->pdo->prepare('SELECT * FROM stmt_methods_test WHERE id = ?');
        $stmt->execute([1]);

        // After successful execution, error code should be '00000'
        $this->assertSame('00000', $stmt->errorCode());

        $info = $stmt->errorInfo();
        $this->assertIsArray($info);
        $this->assertSame('00000', $info[0]);
    }

    public function testQueryWithFetchMode(): void
    {
        // ZtdPdo::query() supports $fetchMode parameter
        $stmt = $this->pdo->query('SELECT id, name FROM stmt_methods_test ORDER BY id', PDO::FETCH_NUM);
        $row = $stmt->fetch();
        $this->assertIsArray($row);
        // FETCH_NUM returns numeric keys
        $this->assertArrayHasKey(0, $row);
        $this->assertArrayHasKey(1, $row);
    }

    public function testGetAttributeAndSetAttribute(): void
    {
        $this->pdo->setAttribute(PDO::ATTR_DEFAULT_FETCH_MODE, PDO::FETCH_ASSOC);
        $mode = $this->pdo->getAttribute(PDO::ATTR_DEFAULT_FETCH_MODE);
        $this->assertSame(PDO::FETCH_ASSOC, $mode);
    }

    public function testErrorCodeAndErrorInfoOnConnection(): void
    {
        $code = $this->pdo->errorCode();
        // Initial error code may be null or '00000'
        $this->assertTrue($code === null || $code === '00000' || $code === '');

        $info = $this->pdo->errorInfo();
        $this->assertIsArray($info);
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS stmt_methods_test');
    }
}
