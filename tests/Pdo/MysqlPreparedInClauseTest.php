<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests prepared statements with IN and NOT IN clauses on MySQL PDO.
 * @spec pending
 */
class MysqlPreparedInClauseTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pic_items (id INT PRIMARY KEY, name VARCHAR(50), category VARCHAR(10), price DECIMAL(10,2))';
    }

    protected function getTableNames(): array
    {
        return ['pic_items'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pic_items (id, name, category, price) VALUES (1, 'Widget', 'A', 10.0)");
        $this->pdo->exec("INSERT INTO pic_items (id, name, category, price) VALUES (2, 'Gadget', 'B', 25.0)");
        $this->pdo->exec("INSERT INTO pic_items (id, name, category, price) VALUES (3, 'Doohickey', 'A', 5.0)");
        $this->pdo->exec("INSERT INTO pic_items (id, name, category, price) VALUES (4, 'Thingamajig', 'C', 50.0)");
    }

    public function testInClauseWithParams(): void
    {
        $stmt = $this->pdo->prepare('SELECT name FROM pic_items WHERE id IN (?, ?) ORDER BY name');
        $stmt->execute([1, 3]);
        $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertSame(['Doohickey', 'Widget'], $rows);
    }

    public function testNotInClauseWithParams(): void
    {
        $stmt = $this->pdo->prepare('SELECT name FROM pic_items WHERE id NOT IN (?, ?) ORDER BY name');
        $stmt->execute([1, 2]);
        $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertSame(['Doohickey', 'Thingamajig'], $rows);
    }

    public function testInClauseWithSubquery(): void
    {
        $stmt = $this->pdo->prepare('SELECT name FROM pic_items WHERE category IN (SELECT category FROM pic_items WHERE price > ?) ORDER BY name');
        $stmt->execute([20.0]);
        $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertSame(['Gadget', 'Thingamajig'], $rows);
    }

    public function testCaseWhenWithParam(): void
    {
        $stmt = $this->pdo->prepare(
            'SELECT name, CASE WHEN price > ? THEN \'expensive\' ELSE \'cheap\' END AS tier '
            . 'FROM pic_items ORDER BY name'
        );
        $stmt->execute([20.0]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame('cheap', $rows[0]['tier']); // Doohickey
        $this->assertSame('expensive', $rows[1]['tier']); // Gadget
    }
}
