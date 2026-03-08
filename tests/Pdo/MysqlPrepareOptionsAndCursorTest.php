<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests prepare() with options, cursor behavior, and framework-style patterns on MySQL.
 * @spec SPEC-3.4
 */
class MysqlPrepareOptionsAndCursorTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE poc_items_m (id INT PRIMARY KEY, name VARCHAR(50), price DECIMAL(10,2), category VARCHAR(20))';
    }

    protected function getTableNames(): array
    {
        return ['poc_items_m'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO poc_items_m VALUES (1, 'Widget', 9.99, 'tools')");
        $this->pdo->exec("INSERT INTO poc_items_m VALUES (2, 'Gadget', 24.99, 'electronics')");
        $this->pdo->exec("INSERT INTO poc_items_m VALUES (3, 'Sprocket', 4.50, 'tools')");
        $this->pdo->exec("INSERT INTO poc_items_m VALUES (4, 'Gizmo', 15.00, 'electronics')");
        $this->pdo->exec("INSERT INTO poc_items_m VALUES (5, 'Doohickey', 7.25, 'misc')");
    }

    public function testPrepareWithEmptyOptionsArray(): void
    {
        $stmt = $this->pdo->prepare('SELECT name FROM poc_items_m WHERE id = ?', []);
        $stmt->execute([1]);
        $this->assertSame('Widget', $stmt->fetchColumn());
    }

    public function testFetchAllThenCountVsDirectCount(): void
    {
        $stmt = $this->pdo->query("SELECT * FROM poc_items_m WHERE category = 'tools'");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $manualCount = count($rows);

        $stmt = $this->pdo->query("SELECT COUNT(*) FROM poc_items_m WHERE category = 'tools'");
        $dbCount = (int) $stmt->fetchColumn();

        $this->assertSame($manualCount, $dbCount);
        $this->assertSame(2, $dbCount);
    }

    public function testPaginationWithPreparedLimitOffset(): void
    {
        $stmt = $this->pdo->prepare('SELECT name FROM poc_items_m ORDER BY id LIMIT ? OFFSET ?');

        $stmt->bindValue(1, 2, PDO::PARAM_INT);
        $stmt->bindValue(2, 0, PDO::PARAM_INT);
        $stmt->execute();
        $page1 = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertSame(['Widget', 'Gadget'], $page1);

        $stmt->bindValue(1, 2, PDO::PARAM_INT);
        $stmt->bindValue(2, 2, PDO::PARAM_INT);
        $stmt->execute();
        $page2 = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertSame(['Sprocket', 'Gizmo'], $page2);
    }

    public function testCloseCursorThenReExecute(): void
    {
        $stmt = $this->pdo->prepare('SELECT name FROM poc_items_m WHERE category = ?');

        $stmt->execute(['tools']);
        $first = $stmt->fetchColumn();
        $this->assertSame('Widget', $first);

        $stmt->closeCursor();

        $stmt->execute(['electronics']);
        $names = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertSame(['Gadget', 'Gizmo'], $names);
    }

    public function testMixedExecAndQueryWorkflow(): void
    {
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM poc_items_m');
        $beforeCount = (int) $stmt->fetchColumn();
        $this->assertSame(5, $beforeCount);

        $this->pdo->exec("INSERT INTO poc_items_m VALUES (6, 'NewItem', 1.00, 'misc')");

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM poc_items_m');
        $afterCount = (int) $stmt->fetchColumn();
        $this->assertSame(6, $afterCount);

        $this->pdo->exec("DELETE FROM poc_items_m WHERE id = 3");

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM poc_items_m');
        $finalCount = (int) $stmt->fetchColumn();
        $this->assertSame(5, $finalCount);
    }

    public function testMultiplePreparedStatementsCoexist(): void
    {
        $select = $this->pdo->prepare('SELECT name FROM poc_items_m WHERE id = ?');
        $count = $this->pdo->prepare('SELECT COUNT(*) FROM poc_items_m WHERE category = ?');

        $count->execute(['tools']);
        $toolCount = (int) $count->fetchColumn();
        $this->assertSame(2, $toolCount);

        $select->execute([1]);
        $this->assertSame('Widget', $select->fetchColumn());
    }
}
