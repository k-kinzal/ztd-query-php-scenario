<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests PDOStatement::nextRowset() behavior with ZTD on MySQL.
 *
 * nextRowset() delegates to the underlying driver. Since ZTD rewrites queries
 * to single-result CTE queries, nextRowset() returns false.
 * @spec SPEC-3.4
 */
class MysqlNextRowsetTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE nr_test_m (id INT PRIMARY KEY, name VARCHAR(50))';
    }

    protected function getTableNames(): array
    {
        return ['nr_test_m'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO nr_test_m VALUES (1, 'Alice')");
        $this->pdo->exec("INSERT INTO nr_test_m VALUES (2, 'Bob')");
    }

    public function testNextRowsetReturnsFalse(): void
    {
        $stmt = $this->pdo->query('SELECT * FROM nr_test_m ORDER BY id');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);

        $this->assertFalse($stmt->nextRowset());
    }

    public function testNextRowsetReturnsFalseOnPrepared(): void
    {
        $stmt = $this->pdo->prepare('SELECT name FROM nr_test_m WHERE id = ?');
        $stmt->execute([1]);
        $stmt->fetch(PDO::FETCH_ASSOC);

        $this->assertFalse($stmt->nextRowset());
    }
}
