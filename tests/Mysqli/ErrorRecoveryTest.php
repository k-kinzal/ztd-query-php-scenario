<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Tests error recovery in ZTD mode via MySQLi: ensures shadow store consistency
 * after SQL errors, and verifies proper exception propagation.
 */
class ErrorRecoveryTest extends TestCase
{
    private ZtdMysqli $mysqli;

    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new \mysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
        $raw->query('DROP TABLE IF EXISTS recovery_test');
        $raw->query('CREATE TABLE recovery_test (id INT PRIMARY KEY, name VARCHAR(255), score INT)');
        $raw->close();
    }

    protected function setUp(): void
    {
        $this->mysqli = new ZtdMysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
    }

    public function testShadowStoreConsistentAfterSyntaxError(): void
    {
        $this->mysqli->query("INSERT INTO recovery_test (id, name, score) VALUES (1, 'Alice', 100)");

        // Attempt invalid SQL — should throw
        try {
            $this->mysqli->query('INSERT INTO recovery_test VALUES INVALID SYNTAX');
        } catch (\RuntimeException $e) {
            // Expected
        }

        // Shadow store should still contain the first insert
        $result = $this->mysqli->query('SELECT * FROM recovery_test ORDER BY id');
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
    }

    public function testDuplicatePkDoesNotThrowInShadowStore(): void
    {
        // Shadow store does NOT enforce primary key constraints.
        // Duplicate PK inserts succeed silently, resulting in multiple rows.
        $this->mysqli->query("INSERT INTO recovery_test (id, name, score) VALUES (1, 'Alice', 100)");
        $this->mysqli->query("INSERT INTO recovery_test (id, name, score) VALUES (1, 'Duplicate', 50)");

        $result = $this->mysqli->query('SELECT * FROM recovery_test ORDER BY name');
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(2, $rows);
    }

    public function testSubsequentOperationsWorkAfterError(): void
    {
        $this->mysqli->query("INSERT INTO recovery_test (id, name, score) VALUES (1, 'Alice', 100)");

        // Cause an error
        try {
            $this->mysqli->query('SELECT * FROM nonexistent_table');
        } catch (\RuntimeException $e) {
            // Expected
        }

        // Subsequent operations should work fine
        $this->mysqli->query("INSERT INTO recovery_test (id, name, score) VALUES (2, 'Bob', 85)");

        $result = $this->mysqli->query('SELECT * FROM recovery_test ORDER BY id');
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Bob', $rows[1]['name']);
    }

    public function testUpdateAfterFailedUpdate(): void
    {
        $this->mysqli->query("INSERT INTO recovery_test (id, name, score) VALUES (1, 'Alice', 100)");

        // Attempt invalid update
        try {
            $this->mysqli->query("UPDATE recovery_test SET nonexistent_column = 'x' WHERE id = 1");
        } catch (\RuntimeException $e) {
            // Expected
        }

        // Valid update should still work
        $this->mysqli->query("UPDATE recovery_test SET name = 'Alice Updated' WHERE id = 1");

        $result = $this->mysqli->query('SELECT name FROM recovery_test WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertSame('Alice Updated', $row['name']);
    }

    public function testDeleteAfterFailedQuery(): void
    {
        $this->mysqli->query("INSERT INTO recovery_test (id, name, score) VALUES (1, 'Alice', 100)");
        $this->mysqli->query("INSERT INTO recovery_test (id, name, score) VALUES (2, 'Bob', 85)");

        // Cause an error
        try {
            $this->mysqli->query('DELETE FROM nonexistent_table WHERE id = 1');
        } catch (\RuntimeException $e) {
            // Expected
        }

        // Valid delete should still work
        $this->mysqli->query('DELETE FROM recovery_test WHERE id = 1');

        $result = $this->mysqli->query('SELECT * FROM recovery_test ORDER BY id');
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('Bob', $rows[0]['name']);
    }

    public function testPreparedStatementErrorRecovery(): void
    {
        $this->mysqli->query("INSERT INTO recovery_test (id, name, score) VALUES (1, 'Alice', 100)");

        // Attempt to prepare invalid SQL
        try {
            $stmt = $this->mysqli->prepare('SELECT * FROM nonexistent_table WHERE id = ?');
        } catch (\RuntimeException $e) {
            // Expected
        }

        // Valid prepared statement should work
        $stmt = $this->mysqli->prepare('SELECT * FROM recovery_test WHERE id = ?');
        $id = 1;
        $stmt->bind_param('i', $id);
        $stmt->execute();
        $result = $stmt->get_result();
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
    }

    protected function tearDown(): void
    {
        $this->mysqli->close();
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new \mysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
        $raw->query('DROP TABLE IF EXISTS recovery_test');
        $raw->close();
    }
}
