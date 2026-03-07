<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Tests INSERT IGNORE behavior on MySQL ZTD via MySQLi adapter:
 * - Duplicate PK silently skipped
 * - Non-duplicate rows inserted
 * - Batch INSERT IGNORE with mixed duplicates
 * - Prepared INSERT IGNORE
 * - Physical isolation
 */
class InsertIgnoreTest extends TestCase
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
        $raw->query('DROP TABLE IF EXISTS mi_ins_ign');
        $raw->query('CREATE TABLE mi_ins_ign (id INT PRIMARY KEY, name VARCHAR(50), score INT)');
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

        $this->mysqli->query("INSERT INTO mi_ins_ign VALUES (1, 'Alice', 90)");
        $this->mysqli->query("INSERT INTO mi_ins_ign VALUES (2, 'Bob', 80)");
    }

    public function testInsertIgnoreDuplicateKeySkipped(): void
    {
        $this->mysqli->query("INSERT IGNORE INTO mi_ins_ign VALUES (1, 'AliceV2', 99)");

        $result = $this->mysqli->query('SELECT name, score FROM mi_ins_ign WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertSame('Alice', $row['name']);
        $this->assertEquals(90, $row['score']);
    }

    public function testInsertIgnoreNonDuplicateInserted(): void
    {
        $this->mysqli->query("INSERT IGNORE INTO mi_ins_ign VALUES (3, 'Charlie', 70)");

        $result = $this->mysqli->query('SELECT COUNT(*) as cnt FROM mi_ins_ign');
        $row = $result->fetch_assoc();
        $this->assertEquals(3, $row['cnt']);
    }

    public function testInsertIgnoreBatchMixedDuplicates(): void
    {
        $this->mysqli->query("INSERT IGNORE INTO mi_ins_ign VALUES (1, 'DupAlice', 99), (3, 'Charlie', 70)");

        $result = $this->mysqli->query('SELECT COUNT(*) as cnt FROM mi_ins_ign');
        $this->assertEquals(3, $result->fetch_assoc()['cnt']);

        // Duplicate row unchanged
        $result = $this->mysqli->query('SELECT name FROM mi_ins_ign WHERE id = 1');
        $this->assertSame('Alice', $result->fetch_assoc()['name']);

        // New row inserted
        $result = $this->mysqli->query('SELECT name FROM mi_ins_ign WHERE id = 3');
        $this->assertSame('Charlie', $result->fetch_assoc()['name']);
    }

    public function testInsertIgnoreAllDuplicates(): void
    {
        $this->mysqli->query("INSERT IGNORE INTO mi_ins_ign VALUES (1, 'DupAlice', 99), (2, 'DupBob', 99)");

        $result = $this->mysqli->query('SELECT COUNT(*) as cnt FROM mi_ins_ign');
        $this->assertEquals(2, $result->fetch_assoc()['cnt']);
    }

    public function testPreparedInsertIgnore(): void
    {
        $stmt = $this->mysqli->prepare('INSERT IGNORE INTO mi_ins_ign VALUES (?, ?, ?)');
        $id = 1;
        $name = 'DupAlice';
        $score = 99;
        $stmt->bind_param('isi', $id, $name, $score);
        $stmt->execute();

        // Original preserved
        $result = $this->mysqli->query('SELECT name FROM mi_ins_ign WHERE id = 1');
        $this->assertSame('Alice', $result->fetch_assoc()['name']);

        // Insert non-duplicate
        $id = 3;
        $name = 'Charlie';
        $score = 70;
        $stmt->execute();

        $result = $this->mysqli->query('SELECT name FROM mi_ins_ign WHERE id = 3');
        $this->assertSame('Charlie', $result->fetch_assoc()['name']);
    }

    public function testInsertIgnorePhysicalIsolation(): void
    {
        $this->mysqli->query("INSERT IGNORE INTO mi_ins_ign VALUES (1, 'DupAlice', 99)");
        $this->mysqli->query("INSERT IGNORE INTO mi_ins_ign VALUES (3, 'Charlie', 70)");

        // Physical table should be empty
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) as cnt FROM mi_ins_ign');
        $this->assertEquals(0, $result->fetch_assoc()['cnt']);
    }

    protected function tearDown(): void
    {
        $this->mysqli->close();
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
            $raw->query('DROP TABLE IF EXISTS mi_ins_ign');
            $raw->close();
        } catch (\Exception $e) {
            // Container may be unavailable during cleanup
        }
    }
}
