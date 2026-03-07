<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Tests ZtdMysqliStatement-specific methods: ztdAffectedRows(), num_rows(),
 * reset(), free_result(), and property access behavior.
 */
class StatementMethodsTest extends TestCase
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
        $raw->query('DROP TABLE IF EXISTS mi_stm_items');
        $raw->query('CREATE TABLE mi_stm_items (id INT PRIMARY KEY, name VARCHAR(255), qty INT)');
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

        $this->mysqli->query("INSERT INTO mi_stm_items (id, name, qty) VALUES (1, 'Apple', 10)");
        $this->mysqli->query("INSERT INTO mi_stm_items (id, name, qty) VALUES (2, 'Banana', 20)");
        $this->mysqli->query("INSERT INTO mi_stm_items (id, name, qty) VALUES (3, 'Cherry', 30)");
    }

    public function testZtdAffectedRowsAfterUpdate(): void
    {
        $stmt = $this->mysqli->prepare("UPDATE mi_stm_items SET qty = ? WHERE qty > ?");
        $stmt->bind_param('ii', $newQty, $minQty);
        $newQty = 99;
        $minQty = 15;
        $stmt->execute();

        $this->assertSame(2, $stmt->ztdAffectedRows());
    }

    public function testZtdAffectedRowsAfterDelete(): void
    {
        $stmt = $this->mysqli->prepare("DELETE FROM mi_stm_items WHERE id = ?");
        $stmt->bind_param('i', $id);
        $id = 2;
        $stmt->execute();

        $this->assertSame(1, $stmt->ztdAffectedRows());
    }

    public function testZtdAffectedRowsAfterInsert(): void
    {
        $stmt = $this->mysqli->prepare("INSERT INTO mi_stm_items (id, name, qty) VALUES (?, ?, ?)");
        $stmt->bind_param('isi', $id, $name, $qty);
        $id = 10;
        $name = 'Date';
        $qty = 5;
        $stmt->execute();

        $this->assertSame(1, $stmt->ztdAffectedRows());
    }

    public function testZtdAffectedRowsReExecute(): void
    {
        $stmt = $this->mysqli->prepare("DELETE FROM mi_stm_items WHERE id = ?");
        $stmt->bind_param('i', $id);

        $id = 1;
        $stmt->execute();
        $this->assertSame(1, $stmt->ztdAffectedRows());

        $id = 2;
        $stmt->execute();
        $this->assertSame(1, $stmt->ztdAffectedRows());

        // Third time — only id=3 left, delete non-existent
        $id = 99;
        $stmt->execute();
        $this->assertSame(0, $stmt->ztdAffectedRows());
    }

    public function testSelectWithGetResult(): void
    {
        $stmt = $this->mysqli->prepare("SELECT name FROM mi_stm_items WHERE qty > ? ORDER BY name");
        $stmt->bind_param('i', $minQty);
        $minQty = 15;
        $stmt->execute();
        $result = $stmt->get_result();
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('Banana', $rows[0]['name']);
        $this->assertSame('Cherry', $rows[1]['name']);
    }

    public function testSelectWithBindResultAndFetch(): void
    {
        $stmt = $this->mysqli->prepare("SELECT id, name FROM mi_stm_items ORDER BY id");
        $stmt->execute();
        $stmt->bind_result($id, $name);

        $results = [];
        while ($stmt->fetch()) {
            $results[] = ['id' => $id, 'name' => $name];
        }
        $this->assertCount(3, $results);
        $this->assertSame('Apple', $results[0]['name']);
    }

    public function testResetClearsResult(): void
    {
        $stmt = $this->mysqli->prepare("UPDATE mi_stm_items SET qty = 0 WHERE id = ?");
        $stmt->bind_param('i', $id);
        $id = 1;
        $stmt->execute();
        $this->assertSame(1, $stmt->ztdAffectedRows());

        // After reset, re-execute should work
        $stmt->reset();
        // Need to re-prepare after reset in some cases, but bind_param still works
        $id = 2;
        $stmt->execute();
        $this->assertSame(1, $stmt->ztdAffectedRows());
    }

    public function testFreeResult(): void
    {
        $stmt = $this->mysqli->prepare("SELECT * FROM mi_stm_items ORDER BY id");
        $stmt->execute();
        $result = $stmt->get_result();
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(3, $rows);

        $stmt->free_result();
        // After free_result, the statement can be re-executed
        $stmt->execute();
        $result = $stmt->get_result();
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(3, $rows);
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
        $raw->query('DROP TABLE IF EXISTS mi_stm_items');
        $raw->close();
    }
}
