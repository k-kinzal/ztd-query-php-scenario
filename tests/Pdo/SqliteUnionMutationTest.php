<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests UNION queries with mutations — verifying that UNION correctly
 * reflects shadow store changes (INSERT, UPDATE, DELETE) and works
 * with various UNION patterns.
 * @spec pending
 */
class SqliteUnionMutationTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE um_employees (id INTEGER PRIMARY KEY, name TEXT, dept TEXT)',
            'CREATE TABLE um_contractors (id INTEGER PRIMARY KEY, name TEXT, dept TEXT)',
            'CREATE TABLE um_results (id INTEGER PRIMARY KEY, source TEXT, name TEXT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['um_employees', 'um_contractors', 'um_results'];
    }


    public function testUnionReflectsInserts(): void
    {
        $stmt = $this->pdo->query("
            SELECT name FROM um_employees WHERE dept = 'Eng'
            UNION ALL
            SELECT name FROM um_contractors WHERE dept = 'Eng'
            ORDER BY name
        ");
        $names = array_column($stmt->fetchAll(PDO::FETCH_ASSOC), 'name');
        $this->assertSame(['Alice', 'Carol', 'Dave'], $names);

        // Add a new person
        $this->pdo->exec("INSERT INTO um_contractors (id, name, dept) VALUES (3, 'Frank', 'Eng')");

        $stmt = $this->pdo->query("
            SELECT name FROM um_employees WHERE dept = 'Eng'
            UNION ALL
            SELECT name FROM um_contractors WHERE dept = 'Eng'
            ORDER BY name
        ");
        $names = array_column($stmt->fetchAll(PDO::FETCH_ASSOC), 'name');
        $this->assertSame(['Alice', 'Carol', 'Dave', 'Frank'], $names);
    }

    public function testUnionReflectsUpdates(): void
    {
        // Move Bob to Eng
        $this->pdo->exec("UPDATE um_employees SET dept = 'Eng' WHERE name = 'Bob'");

        $stmt = $this->pdo->query("
            SELECT name FROM um_employees WHERE dept = 'Eng'
            UNION ALL
            SELECT name FROM um_contractors WHERE dept = 'Eng'
            ORDER BY name
        ");
        $names = array_column($stmt->fetchAll(PDO::FETCH_ASSOC), 'name');
        $this->assertSame(['Alice', 'Bob', 'Carol', 'Dave'], $names);
    }

    public function testUnionReflectsDeletes(): void
    {
        $this->pdo->exec("DELETE FROM um_employees WHERE name = 'Alice'");

        $stmt = $this->pdo->query("
            SELECT name FROM um_employees WHERE dept = 'Eng'
            UNION ALL
            SELECT name FROM um_contractors WHERE dept = 'Eng'
            ORDER BY name
        ");
        $names = array_column($stmt->fetchAll(PDO::FETCH_ASSOC), 'name');
        $this->assertSame(['Carol', 'Dave'], $names);
    }

    public function testUnionDistinct(): void
    {
        // Same name in both tables
        $this->pdo->exec("INSERT INTO um_contractors (id, name, dept) VALUES (3, 'Alice', 'Eng')");

        // UNION (distinct) removes duplicates
        $stmt = $this->pdo->query("
            SELECT name FROM um_employees WHERE dept = 'Eng'
            UNION
            SELECT name FROM um_contractors WHERE dept = 'Eng'
            ORDER BY name
        ");
        $names = array_column($stmt->fetchAll(PDO::FETCH_ASSOC), 'name');
        $this->assertSame(['Alice', 'Carol', 'Dave'], $names);

        // UNION ALL keeps duplicates
        $stmt = $this->pdo->query("
            SELECT name FROM um_employees WHERE dept = 'Eng'
            UNION ALL
            SELECT name FROM um_contractors WHERE dept = 'Eng'
            ORDER BY name
        ");
        $names = array_column($stmt->fetchAll(PDO::FETCH_ASSOC), 'name');
        $this->assertSame(['Alice', 'Alice', 'Carol', 'Dave'], $names);
    }

    public function testUnionWithAggregation(): void
    {
        $stmt = $this->pdo->query("
            SELECT dept, COUNT(*) AS cnt, 'employee' AS source FROM um_employees GROUP BY dept
            UNION ALL
            SELECT dept, COUNT(*) AS cnt, 'contractor' AS source FROM um_contractors GROUP BY dept
            ORDER BY dept, source
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        // Eng: employees=2, contractors=1; Ops: contractors=1; Sales: employees=1
        $this->assertCount(4, $rows);
    }

    public function testUnionWithPreparedStatement(): void
    {
        $stmt = $this->pdo->prepare("
            SELECT name FROM um_employees WHERE dept = ?
            UNION ALL
            SELECT name FROM um_contractors WHERE dept = ?
            ORDER BY name
        ");
        $stmt->execute(['Eng', 'Eng']);
        $names = array_column($stmt->fetchAll(PDO::FETCH_ASSOC), 'name');
        $this->assertSame(['Alice', 'Carol', 'Dave'], $names);

        // Re-execute with different dept
        $stmt->execute(['Sales', 'Sales']);
        $names = array_column($stmt->fetchAll(PDO::FETCH_ASSOC), 'name');
        $this->assertSame(['Bob'], $names);
    }

    public function testExceptReflectsMutations(): void
    {
        // EXCEPT: employees who are NOT also contractors
        $this->pdo->exec("INSERT INTO um_contractors (id, name, dept) VALUES (3, 'Alice', 'Eng')");

        $stmt = $this->pdo->query("
            SELECT name FROM um_employees
            EXCEPT
            SELECT name FROM um_contractors
            ORDER BY name
        ");
        $names = array_column($stmt->fetchAll(PDO::FETCH_ASSOC), 'name');
        // Alice is in both → excluded; Bob, Carol only in employees
        $this->assertSame(['Bob', 'Carol'], $names);

        // Remove Alice from contractors
        $this->pdo->exec("DELETE FROM um_contractors WHERE name = 'Alice'");

        $stmt = $this->pdo->query("
            SELECT name FROM um_employees
            EXCEPT
            SELECT name FROM um_contractors
            ORDER BY name
        ");
        $names = array_column($stmt->fetchAll(PDO::FETCH_ASSOC), 'name');
        // Now Alice is back
        $this->assertSame(['Alice', 'Bob', 'Carol'], $names);
    }

    public function testIntersectReflectsMutations(): void
    {
        $this->pdo->exec("INSERT INTO um_contractors (id, name, dept) VALUES (3, 'Bob', 'Sales')");

        $stmt = $this->pdo->query("
            SELECT name FROM um_employees
            INTERSECT
            SELECT name FROM um_contractors
            ORDER BY name
        ");
        $names = array_column($stmt->fetchAll(PDO::FETCH_ASSOC), 'name');
        // Bob is in both
        $this->assertSame(['Bob'], $names);
    }
}
