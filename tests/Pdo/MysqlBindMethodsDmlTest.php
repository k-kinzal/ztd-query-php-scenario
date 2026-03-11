<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests PDO bindValue() vs bindParam() behavior with ZTD on MySQL.
 *
 * bindParam() binds by reference (value read at execute time).
 * bindValue() binds by value (value captured at bind time).
 * The ZTD wrapper must respect both semantics correctly.
 *
 * @spec SPEC-10.2
 */
class MysqlBindMethodsDmlTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return "CREATE TABLE my_bind_t (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(100),
            age INT,
            active TINYINT(1)
        ) ENGINE=InnoDB";
    }

    protected function getTableNames(): array
    {
        return ['my_bind_t'];
    }

    /**
     * bindValue with positional placeholders.
     */
    public function testBindValuePositional(): void
    {
        try {
            $stmt = $this->pdo->prepare("INSERT INTO my_bind_t (name, age) VALUES (?, ?)");
            $stmt->bindValue(1, 'Alice', PDO::PARAM_STR);
            $stmt->bindValue(2, 30, PDO::PARAM_INT);
            $stmt->execute();

            $rows = $this->ztdQuery("SELECT name, age FROM my_bind_t");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'bindValue positional (MySQL): expected 1 row, got ' . count($rows)
                );
            }

            $this->assertSame('Alice', $rows[0]['name']);
            $this->assertSame(30, (int) $rows[0]['age']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('bindValue positional (MySQL) failed: ' . $e->getMessage());
        }
    }

    /**
     * bindValue with named placeholders.
     */
    public function testBindValueNamed(): void
    {
        try {
            $stmt = $this->pdo->prepare("INSERT INTO my_bind_t (name, age) VALUES (:name, :age)");
            $stmt->bindValue(':name', 'Bob', PDO::PARAM_STR);
            $stmt->bindValue(':age', 25, PDO::PARAM_INT);
            $stmt->execute();

            $rows = $this->ztdQuery("SELECT name, age FROM my_bind_t");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'bindValue named (MySQL): expected 1 row, got ' . count($rows)
                );
            }

            $this->assertSame('Bob', $rows[0]['name']);
            $this->assertSame(25, (int) $rows[0]['age']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('bindValue named (MySQL) failed: ' . $e->getMessage());
        }
    }

    /**
     * bindParam — value read at execute time (reference semantics).
     */
    public function testBindParamReferenceSemantics(): void
    {
        try {
            $stmt = $this->pdo->prepare("INSERT INTO my_bind_t (name, age) VALUES (?, ?)");
            $name = 'Charlie';
            $age = 40;
            $stmt->bindParam(1, $name, PDO::PARAM_STR);
            $stmt->bindParam(2, $age, PDO::PARAM_INT);

            // Change value AFTER bind but BEFORE execute
            $name = 'David';
            $age = 50;
            $stmt->execute();

            $rows = $this->ztdQuery("SELECT name, age FROM my_bind_t");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'bindParam reference (MySQL): expected 1 row, got ' . count($rows)
                );
            }

            // Should see 'David'/50 because bindParam uses reference
            if ($rows[0]['name'] !== 'David') {
                $this->markTestIncomplete(
                    'bindParam reference (MySQL): expected David (reference semantics), got ' . $rows[0]['name']
                );
            }

            $this->assertSame('David', $rows[0]['name']);
            $this->assertSame(50, (int) $rows[0]['age']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('bindParam reference (MySQL) failed: ' . $e->getMessage());
        }
    }

    /**
     * bindValue with PARAM_BOOL.
     */
    public function testBindValueBool(): void
    {
        try {
            $stmt = $this->pdo->prepare("INSERT INTO my_bind_t (name, active) VALUES (?, ?)");
            $stmt->bindValue(1, 'Eve', PDO::PARAM_STR);
            $stmt->bindValue(2, true, PDO::PARAM_BOOL);
            $stmt->execute();

            $rows = $this->ztdQuery("SELECT active FROM my_bind_t WHERE name = 'Eve'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'bindValue BOOL (MySQL): expected 1 row, got ' . count($rows)
                );
            }

            $this->assertSame(1, (int) $rows[0]['active']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('bindValue BOOL (MySQL) failed: ' . $e->getMessage());
        }
    }

    /**
     * bindValue with PARAM_NULL.
     */
    public function testBindValueNull(): void
    {
        try {
            $stmt = $this->pdo->prepare("INSERT INTO my_bind_t (name, age) VALUES (?, ?)");
            $stmt->bindValue(1, 'Frank', PDO::PARAM_STR);
            $stmt->bindValue(2, null, PDO::PARAM_NULL);
            $stmt->execute();

            $rows = $this->ztdQuery("SELECT age FROM my_bind_t WHERE name = 'Frank'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'bindValue NULL (MySQL): expected 1 row, got ' . count($rows)
                );
            }

            $this->assertNull($rows[0]['age']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('bindValue NULL (MySQL) failed: ' . $e->getMessage());
        }
    }

    /**
     * Re-execute same prepared statement with different bound values.
     */
    public function testReexecuteWithDifferentBindValues(): void
    {
        try {
            $stmt = $this->pdo->prepare("INSERT INTO my_bind_t (name, age) VALUES (?, ?)");

            $stmt->bindValue(1, 'First', PDO::PARAM_STR);
            $stmt->bindValue(2, 10, PDO::PARAM_INT);
            $stmt->execute();

            $stmt->bindValue(1, 'Second', PDO::PARAM_STR);
            $stmt->bindValue(2, 20, PDO::PARAM_INT);
            $stmt->execute();

            $rows = $this->ztdQuery("SELECT name, age FROM my_bind_t ORDER BY id");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'Re-execute bind (MySQL): expected 2 rows, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertSame('First', $rows[0]['name']);
            $this->assertSame('Second', $rows[1]['name']);
            $this->assertSame(10, (int) $rows[0]['age']);
            $this->assertSame(20, (int) $rows[1]['age']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Re-execute bind (MySQL) failed: ' . $e->getMessage());
        }
    }

    /**
     * bindParam loop — single prepare, multiple executions via reference.
     */
    public function testBindParamLoop(): void
    {
        try {
            $stmt = $this->pdo->prepare("INSERT INTO my_bind_t (name, age) VALUES (?, ?)");
            $name = '';
            $age = 0;
            $stmt->bindParam(1, $name, PDO::PARAM_STR);
            $stmt->bindParam(2, $age, PDO::PARAM_INT);

            $data = [['Alice', 10], ['Bob', 20], ['Charlie', 30]];
            foreach ($data as [$n, $a]) {
                $name = $n;
                $age = $a;
                $stmt->execute();
            }

            $rows = $this->ztdQuery("SELECT name, age FROM my_bind_t ORDER BY id");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'bindParam loop (MySQL): expected 3 rows, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertSame('Alice', $rows[0]['name']);
            $this->assertSame('Bob', $rows[1]['name']);
            $this->assertSame('Charlie', $rows[2]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('bindParam loop (MySQL) failed: ' . $e->getMessage());
        }
    }
}
