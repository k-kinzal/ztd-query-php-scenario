<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests prepared statements with IN clause, NOT IN clause, and
 * CASE WHEN with parameters — common user patterns for dynamic filtering.
 * @spec pending
 */
class SqlitePreparedInClauseTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pic_items (id INTEGER PRIMARY KEY, name TEXT, category TEXT, price REAL)';
    }

    protected function getTableNames(): array
    {
        return ['pic_items'];
    }


    public function testInClauseWithTwoParams(): void
    {
        $stmt = $this->pdo->prepare('SELECT name FROM pic_items WHERE id IN (?, ?) ORDER BY name');
        $stmt->execute([1, 3]);
        $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertSame(['Doohickey', 'Widget'], $rows);
    }

    public function testInClauseWithThreeParams(): void
    {
        $stmt = $this->pdo->prepare('SELECT name FROM pic_items WHERE category IN (?, ?, ?) ORDER BY name');
        $stmt->execute(['A', 'B', 'C']);
        $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertCount(5, $rows);
    }

    public function testNotInClauseWithParams(): void
    {
        $stmt = $this->pdo->prepare('SELECT name FROM pic_items WHERE id NOT IN (?, ?) ORDER BY name');
        $stmt->execute([1, 2]);
        $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertSame(['Doohickey', 'Thingamajig', 'Whatchamacallit'], $rows);
    }

    public function testInClauseWithNamedParams(): void
    {
        $stmt = $this->pdo->prepare('SELECT name FROM pic_items WHERE category IN (:cat1, :cat2) ORDER BY name');
        $stmt->execute([':cat1' => 'A', ':cat2' => 'C']);
        $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertSame(['Doohickey', 'Thingamajig', 'Widget'], $rows);
    }

    public function testInClauseAfterMutation(): void
    {
        $this->pdo->exec("DELETE FROM pic_items WHERE id = 1");
        $this->pdo->exec("INSERT INTO pic_items (id, name, category, price) VALUES (6, 'NewItem', 'A', 99.0)");

        $stmt = $this->pdo->prepare('SELECT name FROM pic_items WHERE category IN (?, ?) ORDER BY name');
        $stmt->execute(['A', 'C']);
        $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertSame(['Doohickey', 'NewItem', 'Thingamajig'], $rows);
    }

    public function testCaseWhenWithParam(): void
    {
        $stmt = $this->pdo->prepare(
            'SELECT name, CASE WHEN price > ? THEN \'expensive\' ELSE \'cheap\' END AS tier '
            . 'FROM pic_items ORDER BY name'
        );
        $stmt->execute([20.0]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame('cheap', $rows[0]['tier']); // Doohickey (5.0)
        $this->assertSame('expensive', $rows[1]['tier']); // Gadget (25.0)
    }

    public function testInClauseWithSubquery(): void
    {
        $stmt = $this->pdo->prepare('SELECT name FROM pic_items WHERE category IN (SELECT category FROM pic_items WHERE price > ?) ORDER BY name');
        $stmt->execute([20.0]);
        $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);
        // Categories with items > 20.0: B (Gadget=25), C (Thingamajig=50)
        $this->assertSame(['Gadget', 'Thingamajig', 'Whatchamacallit'], $rows);
    }

    public function testInClauseReExecuteWithDifferentParams(): void
    {
        $stmt = $this->pdo->prepare('SELECT name FROM pic_items WHERE id IN (?, ?) ORDER BY name');

        $stmt->execute([1, 2]);
        $rows1 = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertSame(['Gadget', 'Widget'], $rows1);

        $stmt->execute([4, 5]);
        $rows2 = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertSame(['Thingamajig', 'Whatchamacallit'], $rows2);
    }
}
