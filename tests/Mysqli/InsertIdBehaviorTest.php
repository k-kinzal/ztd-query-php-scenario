<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Tests insert_id property behavior with ZTD on MySQLi.
 *
 * ZtdMysqli does not call the parent mysqli constructor, so the C extension
 * handler for built-in properties like insert_id takes precedence over __get,
 * throwing Error("Property access is not allowed yet").
 *
 * This is a significant user pitfall: there is NO way to retrieve the
 * insert_id from ZtdMysqli. Even if it were accessible, it would return 0
 * because ZTD rewrites INSERT to CTE-based SELECT (no physical INSERT).
 *
 * Unlike ZtdPdo which has lastInsertId(), ZtdMysqli has no equivalent
 * method, making auto-increment workflows impossible with ZTD.
 */
class InsertIdBehaviorTest extends TestCase
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
        $raw->query('DROP TABLE IF EXISTS mi_iid_test');
        $raw->query('CREATE TABLE mi_iid_test (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(50), score INT)');
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

    /**
     * insert_id property throws Error after shadow INSERT.
     *
     * The C extension handler throws because ZtdMysqli doesn't call
     * the parent constructor, even though __get is defined.
     */
    public function testInsertIdThrowsErrorAfterShadowInsert(): void
    {
        $this->mysqli->query("INSERT INTO mi_iid_test (name, score) VALUES ('Alice', 90)");

        $this->expectException(\Error::class);
        $this->expectExceptionMessage('Property access is not allowed yet');
        $_ = $this->mysqli->insert_id;
    }

    /**
     * insert_id throws Error even without any prior INSERT.
     */
    public function testInsertIdThrowsErrorWithoutInsert(): void
    {
        $this->expectException(\Error::class);
        $this->expectExceptionMessage('Property access is not allowed yet');
        $_ = $this->mysqli->insert_id;
    }

    /**
     * Shadow data IS accessible despite insert_id being inaccessible.
     */
    public function testShadowDataAccessibleDespiteInsertIdError(): void
    {
        $this->mysqli->query("INSERT INTO mi_iid_test (name, score) VALUES ('Alice', 90)");

        // Shadow data IS queryable
        $result = $this->mysqli->query("SELECT name FROM mi_iid_test WHERE name = 'Alice'");
        $row = $result->fetch_assoc();
        $this->assertSame('Alice', $row['name']);
    }

    /**
     * Explicit ID works in shadow INSERT; the row is queryable.
     */
    public function testExplicitIdWorksInShadow(): void
    {
        $this->mysqli->query("INSERT INTO mi_iid_test (id, name, score) VALUES (999, 'Alice', 90)");

        $result = $this->mysqli->query('SELECT id, name FROM mi_iid_test WHERE id = 999');
        $row = $result->fetch_assoc();
        $this->assertSame(999, (int) $row['id']);
    }

    /**
     * insert_id works correctly on raw mysqli (ZTD disabled).
     */
    public function testInsertIdWorksOnRawMysqli(): void
    {
        // Use a raw connection to verify insert_id works without ZTD
        $raw = new \mysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
        $raw->query("INSERT INTO mi_iid_test (name, score) VALUES ('Physical', 100)");

        $this->assertGreaterThan(0, $raw->insert_id);

        $raw->query("DELETE FROM mi_iid_test WHERE id = {$raw->insert_id}");
        $raw->close();
    }

    /**
     * Prepared statement insert_id on ZtdMysqliStatement.
     *
     * ZtdMysqliStatement delegates insert_id via __get to the inner stmt.
     * Since ZTD rewrites to CTE, the physical insert_id is 0.
     */
    public function testPreparedStatementInsertIdIsZero(): void
    {
        $stmt = $this->mysqli->prepare('INSERT INTO mi_iid_test (name, score) VALUES (?, ?)');
        $name = 'Alice';
        $score = 90;
        $stmt->bind_param('si', $name, $score);
        $stmt->execute();

        // ZtdMysqliStatement delegates insert_id to the inner stmt via __get.
        // After execute(), the inner statement may be closed by ZTD processing.
        // Access insert_id — it may throw if the statement is already closed.
        try {
            $insertId = $stmt->insert_id;
            $this->assertSame(0, $insertId);
        } catch (\Error $e) {
            // Statement already closed by ZTD — insert_id is inaccessible
            $this->assertStringContainsString('closed', $e->getMessage());
        }
    }

    /**
     * Auto-increment column gets NULL in shadow when not specified.
     *
     * Since ZTD doesn't actually INSERT into the physical table,
     * the AUTO_INCREMENT value is never generated. The shadow store
     * will have NULL for the auto-increment column unless explicitly set.
     */
    public function testAutoIncrementColumnIsNullInShadow(): void
    {
        $this->mysqli->query("INSERT INTO mi_iid_test (name, score) VALUES ('Alice', 90)");

        $result = $this->mysqli->query('SELECT id FROM mi_iid_test LIMIT 1');
        $row = $result->fetch_assoc();
        // The id column will be NULL because no physical INSERT generated it
        $this->assertNull($row['id']);
    }

    /**
     * Physical isolation verification.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->query("INSERT INTO mi_iid_test (name, score) VALUES ('Alice', 90)");

        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_iid_test');
        $this->assertSame(0, (int) $result->fetch_assoc()['cnt']);
    }

    protected function tearDown(): void
    {
        if (isset($this->mysqli)) {
            $this->mysqli->close();
        }
    }

    public static function tearDownAfterClass(): void
    {
        try {
            $raw = new \mysqli(
                MySQLContainer::getHost(),
                'root',
                'root',
                'test',
                MySQLContainer::getPort(),
            );
            $raw->query('DROP TABLE IF EXISTS mi_iid_test');
            $raw->close();
        } catch (\Exception $e) {
        }
    }
}
