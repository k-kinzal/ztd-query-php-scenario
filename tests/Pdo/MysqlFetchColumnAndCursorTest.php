<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests fetchColumn(), closeCursor(), and FETCH_CLASS on MySQL ZTD PDO.
 * @spec SPEC-3.4
 */
class MysqlFetchColumnAndCursorTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE fc_test_m (id INT PRIMARY KEY, name VARCHAR(50), score INT)';
    }

    protected function getTableNames(): array
    {
        return ['fc_test_m'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO fc_test_m VALUES (1, 'Alice', 100)");
        $this->pdo->exec("INSERT INTO fc_test_m VALUES (2, 'Bob', 85)");
        $this->pdo->exec("INSERT INTO fc_test_m VALUES (3, 'Charlie', 70)");
    }

    public function testFetchColumnDefaultFirstColumn(): void
    {
        $stmt = $this->pdo->query('SELECT name, score FROM fc_test_m WHERE id = 1');
        $value = $stmt->fetchColumn();
        $this->assertSame('Alice', $value);
    }

    public function testFetchColumnSecondColumn(): void
    {
        $stmt = $this->pdo->query('SELECT name, score FROM fc_test_m WHERE id = 1');
        $value = $stmt->fetchColumn(1);
        $this->assertSame(100, (int) $value);
    }

    public function testFetchColumnIteration(): void
    {
        $stmt = $this->pdo->query('SELECT name FROM fc_test_m ORDER BY id');
        $names = [];
        while (($name = $stmt->fetchColumn()) !== false) {
            $names[] = $name;
        }
        $this->assertSame(['Alice', 'Bob', 'Charlie'], $names);
    }

    public function testFetchColumnReturnsFalseWhenExhausted(): void
    {
        $stmt = $this->pdo->query('SELECT name FROM fc_test_m WHERE id = 999');
        $value = $stmt->fetchColumn();
        $this->assertFalse($value);
    }

    public function testCloseCursorAllowsNewQuery(): void
    {
        $stmt = $this->pdo->query('SELECT name FROM fc_test_m ORDER BY id');
        $stmt->fetch(PDO::FETCH_ASSOC);
        $stmt->closeCursor();

        $stmt2 = $this->pdo->query('SELECT COUNT(*) AS cnt FROM fc_test_m');
        $row = $stmt2->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(3, (int) $row['cnt']);
    }

    public function testFetchObjectMode(): void
    {
        $stmt = $this->pdo->query('SELECT name, score FROM fc_test_m WHERE id = 1');
        $obj = $stmt->fetchObject();
        $this->assertInstanceOf(\stdClass::class, $obj);
        $this->assertSame('Alice', $obj->name);
        $this->assertSame(100, (int) $obj->score);
    }

    public function testFetchAllWithFetchColumn(): void
    {
        $stmt = $this->pdo->query('SELECT name FROM fc_test_m ORDER BY id');
        $names = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertSame(['Alice', 'Bob', 'Charlie'], $names);
    }

    public function testFetchKeyPairMode(): void
    {
        $stmt = $this->pdo->query('SELECT id, name FROM fc_test_m ORDER BY id');
        $pairs = $stmt->fetchAll(PDO::FETCH_KEY_PAIR);
        $this->assertSame([1 => 'Alice', 2 => 'Bob', 3 => 'Charlie'], $pairs);
    }
}
