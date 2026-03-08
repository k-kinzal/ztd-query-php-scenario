<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Tests MySQL-specific INSERT ... SET syntax on MySQLi ZTD.
 *
 * MySQL supports an alternative INSERT syntax:
 *   INSERT INTO table SET col1 = val1, col2 = val2
 * which is equivalent to:
 *   INSERT INTO table (col1, col2) VALUES (val1, val2)
 *
 * The InsertTransformer handles this via buildInsertSetSelect() which converts
 * SET operations into a SELECT expression.
 */
class InsertSetSyntaxTest extends TestCase
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
        $raw->query('DROP TABLE IF EXISTS mi_ins_set');
        $raw->query('CREATE TABLE mi_ins_set (id INT PRIMARY KEY, name VARCHAR(50), score INT)');
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
     * Basic INSERT ... SET syntax.
     */
    public function testInsertSetBasic(): void
    {
        $this->mysqli->query("INSERT INTO mi_ins_set SET id = 1, name = 'Alice', score = 90");

        $result = $this->mysqli->query('SELECT * FROM mi_ins_set WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertSame('Alice', $row['name']);
        $this->assertSame(90, (int) $row['score']);
    }

    /**
     * Multiple INSERT ... SET statements.
     */
    public function testInsertSetMultipleRows(): void
    {
        $this->mysqli->query("INSERT INTO mi_ins_set SET id = 1, name = 'Alice', score = 90");
        $this->mysqli->query("INSERT INTO mi_ins_set SET id = 2, name = 'Bob', score = 80");
        $this->mysqli->query("INSERT INTO mi_ins_set SET id = 3, name = 'Charlie', score = 70");

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_ins_set');
        $this->assertSame(3, (int) $result->fetch_assoc()['cnt']);
    }

    /**
     * INSERT ... SET with expression values.
     */
    public function testInsertSetWithExpression(): void
    {
        $this->mysqli->query("INSERT INTO mi_ins_set SET id = 1, name = CONCAT('A', 'lice'), score = 45 * 2");

        $result = $this->mysqli->query('SELECT * FROM mi_ins_set WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertSame('Alice', $row['name']);
        $this->assertSame(90, (int) $row['score']);
    }

    /**
     * INSERT ... SET then update — verifies shadow store accepts SET-inserted data.
     */
    public function testInsertSetThenUpdate(): void
    {
        $this->mysqli->query("INSERT INTO mi_ins_set SET id = 1, name = 'Alice', score = 90");
        $this->mysqli->query("UPDATE mi_ins_set SET score = 100 WHERE id = 1");

        $result = $this->mysqli->query('SELECT score FROM mi_ins_set WHERE id = 1');
        $this->assertSame(100, (int) $result->fetch_assoc()['score']);
    }

    /**
     * INSERT ... SET ... ON DUPLICATE KEY UPDATE.
     */
    public function testInsertSetOnDuplicateKeyUpdate(): void
    {
        $this->mysqli->query("INSERT INTO mi_ins_set SET id = 1, name = 'Alice', score = 90");
        $this->mysqli->query("INSERT INTO mi_ins_set SET id = 1, name = 'Alice V2', score = 95 ON DUPLICATE KEY UPDATE name = VALUES(name), score = VALUES(score)");

        $result = $this->mysqli->query('SELECT * FROM mi_ins_set WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertSame('Alice V2', $row['name']);
        $this->assertSame(95, (int) $row['score']);
    }

    /**
     * Physical isolation: INSERT ... SET stays in shadow.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->query("INSERT INTO mi_ins_set SET id = 1, name = 'Alice', score = 90");

        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_ins_set');
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
            $raw->query('DROP TABLE IF EXISTS mi_ins_set');
            $raw->close();
        } catch (\Exception $e) {
        }
    }
}
