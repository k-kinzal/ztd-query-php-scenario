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
 * Tests HAVING without GROUP BY and REPLACE INTO edge cases on MySQL PDO.
 */
class MysqlHavingAndReplaceTest extends TestCase
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
        $raw->exec('DROP TABLE IF EXISTS mysql_hr_items');
        $raw->exec('CREATE TABLE mysql_hr_items (id INT PRIMARY KEY, name VARCHAR(255), qty INT, price DECIMAL(10,2))');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );

        $this->pdo->exec("INSERT INTO mysql_hr_items VALUES (1, 'Widget', 10, 9.99)");
        $this->pdo->exec("INSERT INTO mysql_hr_items VALUES (2, 'Gadget', 5, 29.99)");
        $this->pdo->exec("INSERT INTO mysql_hr_items VALUES (3, 'Gizmo', 20, 19.99)");
    }

    public function testHavingWithoutGroupBy(): void
    {
        $stmt = $this->pdo->query("SELECT COUNT(*) AS cnt FROM mysql_hr_items HAVING COUNT(*) > 2");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame(3, (int) $rows[0]['cnt']);
    }

    public function testHavingWithoutGroupByNoMatch(): void
    {
        $stmt = $this->pdo->query("SELECT COUNT(*) AS cnt FROM mysql_hr_items HAVING COUNT(*) > 10");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(0, $rows);
    }

    public function testReplaceIntoExistingRow(): void
    {
        $this->pdo->exec("REPLACE INTO mysql_hr_items VALUES (1, 'Widget V2', 100, 12.99)");

        $stmt = $this->pdo->query("SELECT name, qty, price FROM mysql_hr_items WHERE id = 1");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Widget V2', $row['name']);
        $this->assertSame(100, (int) $row['qty']);
    }

    public function testReplaceIntoNewRow(): void
    {
        $this->pdo->exec("REPLACE INTO mysql_hr_items VALUES (4, 'NewItem', 50, 5.99)");

        $stmt = $this->pdo->query("SELECT COUNT(*) AS cnt FROM mysql_hr_items");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(4, (int) $row['cnt']);
    }

    /**
     * REPLACE INTO via prepared statement does not replace the existing row in shadow store.
     * The old row is retained — REPLACE behavior is not applied to prepared statements.
     */
    public function testReplaceIntoWithPreparedDoesNotReplace(): void
    {
        $stmt = $this->pdo->prepare("REPLACE INTO mysql_hr_items (id, name, qty, price) VALUES (?, ?, ?, ?)");
        $stmt->execute([2, 'Gadget Pro', 200, 49.99]);

        $select = $this->pdo->query("SELECT name, qty FROM mysql_hr_items WHERE id = 2");
        $row = $select->fetch(PDO::FETCH_ASSOC);
        // Expected: 'Gadget Pro' (replaced), Actual: 'Gadget' (old row retained)
        $this->assertSame('Gadget', $row['name']);
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS mysql_hr_items');
    }
}
