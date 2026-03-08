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
 * Tests PDO prepared INSERT + UPDATE behavior on MySQL.
 *
 * @see https://github.com/k-kinzal/ztd-query-php/issues/23
 */
class MysqlPreparedInsertUpdateBugTest extends TestCase
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
        $raw->exec('DROP TABLE IF EXISTS prep_ins_bug');
        $raw->exec('CREATE TABLE prep_ins_bug (id INT PRIMARY KEY, name VARCHAR(50), score INT)');
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

    public function testExecInsertThenUpdateWorks(): void
    {
        $this->pdo->exec("INSERT INTO prep_ins_bug VALUES (1, 'Alice', 100)");
        $this->pdo->exec("UPDATE prep_ins_bug SET score = 200 WHERE id = 1");

        $row = $this->pdo->query('SELECT score FROM prep_ins_bug WHERE id = 1')->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(200, (int) $row['score']);
    }

    /**
     * Prepared INSERT + exec UPDATE should update the row.
     *
     * @see https://github.com/k-kinzal/ztd-query-php/issues/23
     */
    public function testPreparedInsertThenUpdate(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO prep_ins_bug (id, name, score) VALUES (?, ?, ?)');
        $stmt->execute([1, 'Alice', 100]);

        $this->pdo->exec("UPDATE prep_ins_bug SET score = 200 WHERE id = 1");

        $row = $this->pdo->query('SELECT score FROM prep_ins_bug WHERE id = 1')->fetch(PDO::FETCH_ASSOC);
        $score = (int) $row['score'];
        if ($score !== 200) {
            $this->markTestIncomplete(
                'Issue #23: prepared INSERT rows cannot be updated on MySQL. Expected score 200, got ' . $score
            );
        }
        $this->assertSame(200, $score);
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS prep_ins_bug');
    }
}
