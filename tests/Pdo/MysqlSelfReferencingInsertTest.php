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
 * Tests INSERT INTO ... SELECT FROM the same table on MySQL.
 *
 * Self-referencing INSERT copies rows from a table back into itself.
 * MySQL requires explicit column lists for INSERT...SELECT (no SELECT *).
 */
class MysqlSelfReferencingInsertTest extends TestCase
{
    private ZtdPdo $pdo;

    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new PDO(MySQLContainer::getDsn(), 'root', 'root');
        $raw->exec('DROP TABLE IF EXISTS sri_test');
        $raw->exec('CREATE TABLE sri_test (id INT PRIMARY KEY, name VARCHAR(50), score INT, category VARCHAR(20))');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(MySQLContainer::getDsn(), 'root', 'root');
    }

    /**
     * Self-referencing INSERT with new IDs.
     */
    public function testSelfReferencingInsertWithNewIds(): void
    {
        $this->pdo->exec("INSERT INTO sri_test (id, name, score, category) VALUES (1, 'Alice', 90, 'A')");
        $this->pdo->exec("INSERT INTO sri_test (id, name, score, category) VALUES (2, 'Bob', 80, 'B')");

        $affected = $this->pdo->exec(
            'INSERT INTO sri_test (id, name, score, category) SELECT id + 100, name, score, category FROM sri_test'
        );

        $this->assertSame(2, $affected);

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM sri_test');
        $this->assertSame(4, (int) $stmt->fetchColumn());
    }

    /**
     * Self-referencing INSERT with WHERE filter.
     */
    public function testSelfReferencingInsertWithFilter(): void
    {
        $this->pdo->exec("INSERT INTO sri_test (id, name, score, category) VALUES (1, 'Alice', 90, 'A')");
        $this->pdo->exec("INSERT INTO sri_test (id, name, score, category) VALUES (2, 'Bob', 80, 'B')");
        $this->pdo->exec("INSERT INTO sri_test (id, name, score, category) VALUES (3, 'Charlie', 70, 'A')");

        $affected = $this->pdo->exec(
            "INSERT INTO sri_test (id, name, score, category) SELECT id + 100, name, score, 'A-copy' FROM sri_test WHERE category = 'A'"
        );

        $this->assertSame(2, $affected);
    }

    /**
     * Self-referencing INSERT doesn't cause infinite loop.
     */
    public function testSelfReferencingInsertDoesNotLoop(): void
    {
        $this->pdo->exec("INSERT INTO sri_test (id, name, score, category) VALUES (1, 'Alice', 90, 'A')");

        $affected = $this->pdo->exec(
            'INSERT INTO sri_test (id, name, score, category) SELECT id + 10, name, score, category FROM sri_test'
        );

        $this->assertSame(1, $affected);

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM sri_test');
        $this->assertSame(2, (int) $stmt->fetchColumn());
    }

    /**
     * Self-referencing INSERT after mutations.
     */
    public function testSelfReferencingInsertAfterMutations(): void
    {
        $this->pdo->exec("INSERT INTO sri_test (id, name, score, category) VALUES (1, 'Alice', 90, 'A')");
        $this->pdo->exec("INSERT INTO sri_test (id, name, score, category) VALUES (2, 'Bob', 80, 'B')");
        $this->pdo->exec("UPDATE sri_test SET score = 100 WHERE id = 1");
        $this->pdo->exec("DELETE FROM sri_test WHERE id = 2");

        $affected = $this->pdo->exec(
            'INSERT INTO sri_test (id, name, score, category) SELECT id + 100, name, score, category FROM sri_test'
        );

        $this->assertSame(1, $affected);

        $stmt = $this->pdo->query('SELECT score FROM sri_test WHERE id = 101');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(100, (int) $row['score']);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO sri_test (id, name, score, category) VALUES (1, 'Alice', 90, 'A')");
        $this->pdo->exec(
            'INSERT INTO sri_test (id, name, score, category) SELECT id + 100, name, score, category FROM sri_test'
        );

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM sri_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }

    public static function tearDownAfterClass(): void
    {
        try {
            $raw = new PDO(MySQLContainer::getDsn(), 'root', 'root');
            $raw->exec('DROP TABLE IF EXISTS sri_test');
        } catch (\Exception $e) {
        }
    }
}
