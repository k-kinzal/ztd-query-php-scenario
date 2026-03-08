<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Confirms prepared INSERT + UPDATE bug on MySQLi (issue #23).
 *
 * Cross-platform parity with MysqlPreparedInsertUpdateBugTest (PDO).
 */
class PreparedInsertUpdateBugTest extends TestCase
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
        $raw->query('DROP TABLE IF EXISTS mi_prep_ins_bug');
        $raw->query('CREATE TABLE mi_prep_ins_bug (id INT PRIMARY KEY, name VARCHAR(50), score INT)');
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

    public function testExecInsertThenUpdateWorks(): void
    {
        $this->mysqli->query("INSERT INTO mi_prep_ins_bug VALUES (1, 'Alice', 100)");
        $this->mysqli->query('UPDATE mi_prep_ins_bug SET score = 200 WHERE id = 1');

        $result = $this->mysqli->query('SELECT score FROM mi_prep_ins_bug WHERE id = 1');
        $this->assertSame(200, (int) $result->fetch_assoc()['score']);
    }

    /**
     * Prepared INSERT + query UPDATE works correctly on MySQLi.
     *
     * Unlike PDO where this is a bug (issue #23, update doesn't take effect),
     * MySQLi correctly applies the UPDATE after a prepared INSERT.
     */
    public function testPreparedInsertThenUpdateWorks(): void
    {
        $stmt = $this->mysqli->prepare('INSERT INTO mi_prep_ins_bug (id, name, score) VALUES (?, ?, ?)');
        $id = 1;
        $name = 'Alice';
        $score = 100;
        $stmt->bind_param('isi', $id, $name, $score);
        $stmt->execute();

        $this->mysqli->query('UPDATE mi_prep_ins_bug SET score = 200 WHERE id = 1');

        $result = $this->mysqli->query('SELECT score FROM mi_prep_ins_bug WHERE id = 1');
        $this->assertSame(200, (int) $result->fetch_assoc()['score']);
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
            $raw->query('DROP TABLE IF EXISTS mi_prep_ins_bug');
            $raw->close();
        } catch (\Exception $e) {
        }
    }
}
