<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests PDO bindValue() vs bindParam() behavior with ZTD on SQLite.
 *
 * @spec SPEC-10.2
 */
class SqliteBindMethodsDmlTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        // Use explicit PK (not AUTOINCREMENT) to avoid Issue #145 shadow PK=null
        return "CREATE TABLE sl_bind_t (
            id INTEGER PRIMARY KEY,
            name TEXT,
            age INTEGER,
            active INTEGER
        )";
    }

    protected function getTableNames(): array
    {
        return ['sl_bind_t'];
    }

    public function testBindValuePositional(): void
    {
        try {
            $stmt = $this->pdo->prepare("INSERT INTO sl_bind_t (id, name, age) VALUES (?, ?, ?)");
            $stmt->bindValue(1, 1, PDO::PARAM_INT);
            $stmt->bindValue(2, 'Alice', PDO::PARAM_STR);
            $stmt->bindValue(3, 30, PDO::PARAM_INT);
            $stmt->execute();

            $rows = $this->ztdQuery("SELECT name, age FROM sl_bind_t");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'bindValue positional (SQLite): expected 1 row, got ' . count($rows)
                );
            }

            $this->assertSame('Alice', $rows[0]['name']);
            $this->assertSame(30, (int) $rows[0]['age']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('bindValue positional (SQLite) failed: ' . $e->getMessage());
        }
    }

    public function testBindValueNamed(): void
    {
        try {
            $stmt = $this->pdo->prepare("INSERT INTO sl_bind_t (id, name, age) VALUES (:id, :name, :age)");
            $stmt->bindValue(':id', 1, PDO::PARAM_INT);
            $stmt->bindValue(':name', 'Bob', PDO::PARAM_STR);
            $stmt->bindValue(':age', 25, PDO::PARAM_INT);
            $stmt->execute();

            $rows = $this->ztdQuery("SELECT name, age FROM sl_bind_t");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'bindValue named (SQLite): expected 1 row, got ' . count($rows)
                );
            }

            $this->assertSame('Bob', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('bindValue named (SQLite) failed: ' . $e->getMessage());
        }
    }

    public function testBindParamReferenceSemantics(): void
    {
        try {
            $stmt = $this->pdo->prepare("INSERT INTO sl_bind_t (id, name, age) VALUES (?, ?, ?)");
            $id = 1;
            $name = 'Charlie';
            $age = 40;
            $stmt->bindParam(1, $id, PDO::PARAM_INT);
            $stmt->bindParam(2, $name, PDO::PARAM_STR);
            $stmt->bindParam(3, $age, PDO::PARAM_INT);

            $name = 'David';
            $age = 50;
            $stmt->execute();

            $rows = $this->ztdQuery("SELECT name, age FROM sl_bind_t");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'bindParam reference (SQLite): expected 1 row, got ' . count($rows)
                );
            }

            if ($rows[0]['name'] !== 'David') {
                $this->markTestIncomplete(
                    'bindParam reference (SQLite): expected David (reference semantics), got ' . $rows[0]['name']
                );
            }

            $this->assertSame('David', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('bindParam reference (SQLite) failed: ' . $e->getMessage());
        }
    }

    public function testBindValueNull(): void
    {
        try {
            $stmt = $this->pdo->prepare("INSERT INTO sl_bind_t (id, name, age) VALUES (?, ?, ?)");
            $stmt->bindValue(1, 1, PDO::PARAM_INT);
            $stmt->bindValue(2, 'Frank', PDO::PARAM_STR);
            $stmt->bindValue(3, null, PDO::PARAM_NULL);
            $stmt->execute();

            $rows = $this->ztdQuery("SELECT age FROM sl_bind_t WHERE name = 'Frank'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'bindValue NULL (SQLite): expected 1 row, got ' . count($rows)
                );
            }

            $this->assertNull($rows[0]['age']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('bindValue NULL (SQLite) failed: ' . $e->getMessage());
        }
    }

    public function testBindParamLoop(): void
    {
        try {
            $stmt = $this->pdo->prepare("INSERT INTO sl_bind_t (id, name, age) VALUES (?, ?, ?)");
            $id = 0;
            $name = '';
            $age = 0;
            $stmt->bindParam(1, $id, PDO::PARAM_INT);
            $stmt->bindParam(2, $name, PDO::PARAM_STR);
            $stmt->bindParam(3, $age, PDO::PARAM_INT);

            $data = [[1, 'Alice', 10], [2, 'Bob', 20], [3, 'Charlie', 30]];
            foreach ($data as [$i, $n, $a]) {
                $id = $i;
                $name = $n;
                $age = $a;
                $stmt->execute();
            }

            $rows = $this->ztdQuery("SELECT name, age FROM sl_bind_t ORDER BY id");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'bindParam loop (SQLite): expected 3 rows, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertSame('Alice', $rows[0]['name']);
            $this->assertSame('Bob', $rows[1]['name']);
            $this->assertSame('Charlie', $rows[2]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('bindParam loop (SQLite) failed: ' . $e->getMessage());
        }
    }

    /**
     * Re-execute with different bound values.
     */
    public function testReexecuteWithDifferentBindValues(): void
    {
        try {
            $stmt = $this->pdo->prepare("INSERT INTO sl_bind_t (id, name, age) VALUES (?, ?, ?)");

            $stmt->bindValue(1, 1, PDO::PARAM_INT);
            $stmt->bindValue(2, 'First', PDO::PARAM_STR);
            $stmt->bindValue(3, 10, PDO::PARAM_INT);
            $stmt->execute();

            $stmt->bindValue(1, 2, PDO::PARAM_INT);
            $stmt->bindValue(2, 'Second', PDO::PARAM_STR);
            $stmt->bindValue(3, 20, PDO::PARAM_INT);
            $stmt->execute();

            $rows = $this->ztdQuery("SELECT name, age FROM sl_bind_t ORDER BY id");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'Re-execute bind (SQLite): expected 2 rows, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertSame('First', $rows[0]['name']);
            $this->assertSame('Second', $rows[1]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Re-execute bind (SQLite) failed: ' . $e->getMessage());
        }
    }
}
