<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests prepared statement edge cases on SQLite PDO.
 *
 * Covers edge cases like no-parameter prepared statements,
 * multiple concurrent prepared statements, parameter binding errors,
 * and prepared statement behavior with empty result sets.
 */
class SqlitePreparedStatementEdgeCasesTest extends TestCase
{
    private ZtdPdo $pdo;

    protected function setUp(): void
    {
        $raw = new PDO('sqlite::memory:');
        $raw->exec('CREATE TABLE pse_items (id INT PRIMARY KEY, name VARCHAR(50), category VARCHAR(20))');
        $this->pdo = ZtdPdo::fromPdo($raw);

        $this->pdo->exec("INSERT INTO pse_items VALUES (1, 'Widget', 'tools')");
        $this->pdo->exec("INSERT INTO pse_items VALUES (2, 'Gadget', 'electronics')");
        $this->pdo->exec("INSERT INTO pse_items VALUES (3, 'Doohickey', 'tools')");
    }

    /**
     * Prepared statement with no parameters (literal SQL).
     */
    public function testPreparedWithNoParameters(): void
    {
        $stmt = $this->pdo->prepare('SELECT * FROM pse_items WHERE id = 1');
        $stmt->execute();
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Widget', $row['name']);
    }

    /**
     * Prepared statement returning empty result set.
     */
    public function testPreparedEmptyResult(): void
    {
        $stmt = $this->pdo->prepare('SELECT * FROM pse_items WHERE id = ?');
        $stmt->execute([999]);
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertFalse($row);
    }

    /**
     * Multiple concurrent prepared statements.
     */
    public function testMultipleConcurrentPreparedStatements(): void
    {
        $stmt1 = $this->pdo->prepare('SELECT name FROM pse_items WHERE id = ?');
        $stmt2 = $this->pdo->prepare('SELECT category FROM pse_items WHERE id = ?');

        $stmt1->execute([1]);
        $stmt2->execute([2]);

        $this->assertSame('Widget', $stmt1->fetchColumn());
        $this->assertSame('electronics', $stmt2->fetchColumn());
    }

    /**
     * Re-executing prepared statement with different parameters.
     */
    public function testReExecuteWithDifferentParams(): void
    {
        $stmt = $this->pdo->prepare('SELECT name FROM pse_items WHERE id = ?');

        $stmt->execute([1]);
        $this->assertSame('Widget', $stmt->fetchColumn());

        $stmt->execute([2]);
        $this->assertSame('Gadget', $stmt->fetchColumn());
    }

    /**
     * Prepared INSERT followed by prepared SELECT.
     */
    public function testPreparedInsertThenPreparedSelect(): void
    {
        $insert = $this->pdo->prepare('INSERT INTO pse_items VALUES (?, ?, ?)');
        $insert->execute([4, 'Thingamajig', 'misc']);

        // New prepare after insert sees the new data
        $select = $this->pdo->prepare('SELECT name FROM pse_items WHERE id = ?');
        $select->execute([4]);
        $this->assertSame('Thingamajig', $select->fetchColumn());
    }

    /**
     * Named parameters with bindValue.
     */
    public function testNamedParametersWithBindValue(): void
    {
        $stmt = $this->pdo->prepare('SELECT name FROM pse_items WHERE category = :cat AND id > :min_id');
        $stmt->bindValue(':cat', 'tools');
        $stmt->bindValue(':min_id', 1, PDO::PARAM_INT);
        $stmt->execute();

        $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertCount(1, $rows);
        $this->assertSame('Doohickey', $rows[0]);
    }

    /**
     * Prepared DELETE with affected row count.
     */
    public function testPreparedDeleteRowCount(): void
    {
        $stmt = $this->pdo->prepare('DELETE FROM pse_items WHERE category = ?');
        $stmt->execute(['tools']);

        $this->assertEquals(2, $stmt->rowCount());

        // Verify deletion
        $select = $this->pdo->prepare('SELECT COUNT(*) FROM pse_items');
        $select->execute();
        $this->assertSame(1, (int) $select->fetchColumn());
    }

    /**
     * Prepared UPDATE with affected row count.
     */
    public function testPreparedUpdateRowCount(): void
    {
        $stmt = $this->pdo->prepare("UPDATE pse_items SET category = ? WHERE category = ?");
        $stmt->execute(['gadgets', 'tools']);

        $this->assertEquals(2, $stmt->rowCount());
    }

    /**
     * columnCount() on prepared statement.
     */
    public function testColumnCount(): void
    {
        $stmt = $this->pdo->prepare('SELECT id, name, category FROM pse_items WHERE id = ?');
        $stmt->execute([1]);

        $this->assertSame(3, $stmt->columnCount());
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pse_items');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
