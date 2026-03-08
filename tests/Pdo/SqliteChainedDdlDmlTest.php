<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests chained DDL → DML operation sequences on SQLite.
 *
 * Verifies that complex sequences of CREATE, DROP, INSERT, UPDATE, DELETE,
 * and SELECT maintain shadow store consistency.
 * @spec pending
 */
class SqliteChainedDdlDmlTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE chain_t1 (id INT PRIMARY KEY, name VARCHAR(50))',
            'CREATE TABLE chain_t2 (id INT PRIMARY KEY, ref_id INT, value INT)',
            'CREATE TABLE → INSERT → SELECT.
     */
    public function testDropCreateInsert(): void
    {
        $this->pdo->exec("INSERT INTO chain_t1 VALUES (1,',
            'CREATE TABLE chain_t1 (id INT PRIMARY KEY, label VARCHAR(50))',
            'CREATE TABLE → INSERT → DROP → CREATE same name → INSERT different schema.
     */
    public function testRecreateWithDifferentSchema(): void
    {
        $this->pdo->exec("INSERT INTO chain_t1 VALUES (1,',
            'CREATE TABLE chain_t1 (id INT PRIMARY KEY, score INT, grade VARCHAR(2))',
        ];
    }

    protected function getTableNames(): array
    {
        return ['chain_t1', 'chain_t2'];
    }


    /**
     * INSERT → SELECT → UPDATE → SELECT cycle.
     */
    public function testInsertUpdateCycle(): void
    {
        $this->pdo->exec("INSERT INTO chain_t1 VALUES (1, 'Alice')");
        $this->pdo->exec("INSERT INTO chain_t1 VALUES (2, 'Bob')");

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM chain_t1');
        $this->assertSame(2, (int) $stmt->fetchColumn());

        $this->pdo->exec("UPDATE chain_t1 SET name = 'Updated' WHERE id = 1");

        $stmt = $this->pdo->query('SELECT name FROM chain_t1 WHERE id = 1');
        $this->assertSame('Updated', $stmt->fetchColumn());
    }

    /**
     * INSERT → DELETE → INSERT same ID.
     */
    public function testDeleteReinsertSameId(): void
    {
        $this->pdo->exec("INSERT INTO chain_t1 VALUES (1, 'First')");
        $this->pdo->exec("DELETE FROM chain_t1 WHERE id = 1");
        $this->pdo->exec("INSERT INTO chain_t1 VALUES (1, 'Second')");

        $stmt = $this->pdo->query('SELECT name FROM chain_t1 WHERE id = 1');
        $this->assertSame('Second', $stmt->fetchColumn());
    }

    /**
     * DROP TABLE → CREATE TABLE → INSERT → SELECT.
     */
    public function testDropCreateInsert(): void
    {
        $this->pdo->exec("INSERT INTO chain_t1 VALUES (1, 'OldData')");
        $this->pdo->exec('DROP TABLE chain_t1');
        $this->pdo->exec('CREATE TABLE chain_t1 (id INT PRIMARY KEY, label VARCHAR(50))');
        $this->pdo->exec("INSERT INTO chain_t1 VALUES (1, 'NewData')");

        $stmt = $this->pdo->query('SELECT label FROM chain_t1 WHERE id = 1');
        $this->assertSame('NewData', $stmt->fetchColumn());
    }

    /**
     * Multi-table operations in sequence.
     */
    public function testMultiTableSequence(): void
    {
        $this->pdo->exec("INSERT INTO chain_t1 VALUES (1, 'Alice')");
        $this->pdo->exec("INSERT INTO chain_t1 VALUES (2, 'Bob')");
        $this->pdo->exec("INSERT INTO chain_t2 VALUES (1, 1, 100)");
        $this->pdo->exec("INSERT INTO chain_t2 VALUES (2, 1, 200)");
        $this->pdo->exec("INSERT INTO chain_t2 VALUES (3, 2, 150)");

        // JOIN across both tables
        $stmt = $this->pdo->query(
            'SELECT t1.name, SUM(t2.value) as total
             FROM chain_t1 t1
             JOIN chain_t2 t2 ON t1.id = t2.ref_id
             GROUP BY t1.name
             ORDER BY t1.name'
        );
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertEquals(300, (int) $rows[0]['total']);
        $this->assertSame('Bob', $rows[1]['name']);
        $this->assertEquals(150, (int) $rows[1]['total']);

        // Delete from t2, then check
        $this->pdo->exec('DELETE FROM chain_t2 WHERE ref_id = 1');

        $stmt = $this->pdo->query(
            'SELECT t1.name, COALESCE(SUM(t2.value), 0) as total
             FROM chain_t1 t1
             LEFT JOIN chain_t2 t2 ON t1.id = t2.ref_id
             GROUP BY t1.name
             ORDER BY t1.name'
        );
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertEquals(0, (int) $rows[0]['total']);
    }

    /**
     * CREATE TABLE → INSERT → DROP → CREATE same name → INSERT different schema.
     */
    public function testRecreateWithDifferentSchema(): void
    {
        $this->pdo->exec("INSERT INTO chain_t1 VALUES (1, 'Old')");

        $this->pdo->exec('DROP TABLE chain_t1');
        $this->pdo->exec('CREATE TABLE chain_t1 (id INT PRIMARY KEY, score INT, grade VARCHAR(2))');
        $this->pdo->exec("INSERT INTO chain_t1 VALUES (1, 95, 'A')");

        $stmt = $this->pdo->query('SELECT grade FROM chain_t1 WHERE id = 1');
        $this->assertSame('A', $stmt->fetchColumn());
    }

    /**
     * Physical isolation across all operations.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO chain_t1 VALUES (1, 'Shadow')");
        $this->pdo->exec("INSERT INTO chain_t2 VALUES (1, 1, 999)");

        $this->pdo->disableZtd();

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM chain_t1');
        $this->assertSame(0, (int) $stmt->fetchColumn());

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM chain_t2');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
