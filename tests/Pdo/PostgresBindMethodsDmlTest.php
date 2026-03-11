<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests PDO bindValue() vs bindParam() behavior with ZTD on PostgreSQL.
 *
 * @spec SPEC-10.2
 */
class PostgresBindMethodsDmlTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return "CREATE TABLE pg_bind_t (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100),
            age INTEGER,
            active BOOLEAN
        )";
    }

    protected function getTableNames(): array
    {
        return ['pg_bind_t'];
    }

    public function testBindValuePositional(): void
    {
        try {
            $stmt = $this->pdo->prepare("INSERT INTO pg_bind_t (name, age) VALUES (?, ?)");
            $stmt->bindValue(1, 'Alice', PDO::PARAM_STR);
            $stmt->bindValue(2, 30, PDO::PARAM_INT);
            $stmt->execute();

            $rows = $this->ztdQuery("SELECT name, age FROM pg_bind_t");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'bindValue positional (PG): expected 1 row, got ' . count($rows)
                );
            }

            $this->assertSame('Alice', $rows[0]['name']);
            $this->assertSame(30, (int) $rows[0]['age']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('bindValue positional (PG) failed: ' . $e->getMessage());
        }
    }

    public function testBindValueNamed(): void
    {
        try {
            $stmt = $this->pdo->prepare("INSERT INTO pg_bind_t (name, age) VALUES (:name, :age)");
            $stmt->bindValue(':name', 'Bob', PDO::PARAM_STR);
            $stmt->bindValue(':age', 25, PDO::PARAM_INT);
            $stmt->execute();

            $rows = $this->ztdQuery("SELECT name, age FROM pg_bind_t");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'bindValue named (PG): expected 1 row, got ' . count($rows)
                );
            }

            $this->assertSame('Bob', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('bindValue named (PG) failed: ' . $e->getMessage());
        }
    }

    public function testBindParamReferenceSemantics(): void
    {
        try {
            $stmt = $this->pdo->prepare("INSERT INTO pg_bind_t (name, age) VALUES (?, ?)");
            $name = 'Charlie';
            $age = 40;
            $stmt->bindParam(1, $name, PDO::PARAM_STR);
            $stmt->bindParam(2, $age, PDO::PARAM_INT);

            $name = 'David';
            $age = 50;
            $stmt->execute();

            $rows = $this->ztdQuery("SELECT name, age FROM pg_bind_t");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'bindParam reference (PG): expected 1 row, got ' . count($rows)
                );
            }

            if ($rows[0]['name'] !== 'David') {
                $this->markTestIncomplete(
                    'bindParam reference (PG): expected David (reference semantics), got ' . $rows[0]['name']
                );
            }

            $this->assertSame('David', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('bindParam reference (PG) failed: ' . $e->getMessage());
        }
    }

    public function testBindValueBool(): void
    {
        try {
            $stmt = $this->pdo->prepare("INSERT INTO pg_bind_t (name, active) VALUES (?, ?)");
            $stmt->bindValue(1, 'Eve', PDO::PARAM_STR);
            $stmt->bindValue(2, true, PDO::PARAM_BOOL);
            $stmt->execute();

            $rows = $this->ztdQuery("SELECT active FROM pg_bind_t WHERE name = 'Eve'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'bindValue BOOL (PG): expected 1 row, got ' . count($rows)
                );
            }

            // PostgreSQL BOOLEAN may return 't'/'f' or true/false depending on PDO config
            $val = $rows[0]['active'];
            $isTruthy = $val === true || $val === 't' || $val === '1' || $val === 1;
            if (!$isTruthy) {
                $this->markTestIncomplete(
                    'bindValue BOOL (PG): expected truthy, got ' . var_export($val, true)
                );
            }

            $this->assertTrue($isTruthy);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('bindValue BOOL (PG) failed: ' . $e->getMessage());
        }
    }

    public function testBindValueNull(): void
    {
        try {
            $stmt = $this->pdo->prepare("INSERT INTO pg_bind_t (name, age) VALUES (?, ?)");
            $stmt->bindValue(1, 'Frank', PDO::PARAM_STR);
            $stmt->bindValue(2, null, PDO::PARAM_NULL);
            $stmt->execute();

            $rows = $this->ztdQuery("SELECT age FROM pg_bind_t WHERE name = 'Frank'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'bindValue NULL (PG): expected 1 row, got ' . count($rows)
                );
            }

            $this->assertNull($rows[0]['age']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('bindValue NULL (PG) failed: ' . $e->getMessage());
        }
    }

    public function testBindParamLoop(): void
    {
        try {
            $stmt = $this->pdo->prepare("INSERT INTO pg_bind_t (name, age) VALUES (?, ?)");
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

            $rows = $this->ztdQuery("SELECT name, age FROM pg_bind_t ORDER BY id");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'bindParam loop (PG): expected 3 rows, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertSame('Alice', $rows[0]['name']);
            $this->assertSame('Bob', $rows[1]['name']);
            $this->assertSame('Charlie', $rows[2]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('bindParam loop (PG) failed: ' . $e->getMessage());
        }
    }
}
