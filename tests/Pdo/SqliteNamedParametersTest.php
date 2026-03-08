<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests PDO named parameter binding (:param) with ZTD shadow operations on SQLite.
 *
 * Ensures that named parameters in prepared statements work correctly
 * with the CTE rewriter, including bindValue, bindParam, and execute-time binding.
 */
class SqliteNamedParametersTest extends TestCase
{
    private ZtdPdo $pdo;

    protected function setUp(): void
    {
        $raw = new PDO('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        $raw->exec('CREATE TABLE sl_np_test (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)');
        $this->pdo = ZtdPdo::fromPdo($raw);

        $this->pdo->exec("INSERT INTO sl_np_test VALUES (1, 'Alice', 90)");
        $this->pdo->exec("INSERT INTO sl_np_test VALUES (2, 'Bob', 85)");
        $this->pdo->exec("INSERT INTO sl_np_test VALUES (3, 'Charlie', 95)");
    }

    /**
     * Named parameters via execute().
     */
    public function testNamedParamsViaExecute(): void
    {
        $stmt = $this->pdo->prepare('SELECT name FROM sl_np_test WHERE score > :min_score ORDER BY name');
        $stmt->execute([':min_score' => 88]);

        $names = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertSame(['Alice', 'Charlie'], $names);
    }

    /**
     * Named parameters via bindValue().
     */
    public function testNamedParamsViaBindValue(): void
    {
        $stmt = $this->pdo->prepare('SELECT name FROM sl_np_test WHERE id = :id');
        $stmt->bindValue(':id', 2, PDO::PARAM_INT);
        $stmt->execute();

        $this->assertSame('Bob', $stmt->fetchColumn());
    }

    /**
     * Named parameters via bindParam() with variable reference.
     */
    public function testNamedParamsViaBindParam(): void
    {
        $stmt = $this->pdo->prepare('SELECT name FROM sl_np_test WHERE id = :id');
        $id = 3;
        $stmt->bindParam(':id', $id, PDO::PARAM_INT);
        $stmt->execute();

        $this->assertSame('Charlie', $stmt->fetchColumn());
    }

    /**
     * Multiple named parameters in one query.
     */
    public function testMultipleNamedParams(): void
    {
        $stmt = $this->pdo->prepare('SELECT name FROM sl_np_test WHERE score >= :min AND score <= :max ORDER BY name');
        $stmt->execute([':min' => 85, ':max' => 90]);

        $names = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertSame(['Alice', 'Bob'], $names);
    }

    /**
     * Named parameters in INSERT.
     */
    public function testNamedParamsInInsert(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO sl_np_test VALUES (:id, :name, :score)');
        $stmt->execute([':id' => 4, ':name' => 'Diana', ':score' => 92]);

        $qstmt = $this->pdo->query('SELECT name FROM sl_np_test WHERE id = 4');
        $this->assertSame('Diana', $qstmt->fetchColumn());
    }

    /**
     * Named parameters in UPDATE.
     */
    public function testNamedParamsInUpdate(): void
    {
        $stmt = $this->pdo->prepare('UPDATE sl_np_test SET score = :new_score WHERE name = :name');
        $stmt->execute([':new_score' => 100, ':name' => 'Alice']);

        $qstmt = $this->pdo->query('SELECT score FROM sl_np_test WHERE name = \'Alice\'');
        $this->assertSame(100, (int) $qstmt->fetchColumn());
    }

    /**
     * Named parameters in DELETE.
     */
    public function testNamedParamsInDelete(): void
    {
        $stmt = $this->pdo->prepare('DELETE FROM sl_np_test WHERE id = :id');
        $stmt->execute([':id' => 2]);

        $qstmt = $this->pdo->query('SELECT COUNT(*) FROM sl_np_test');
        $this->assertSame(2, (int) $qstmt->fetchColumn());
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM sl_np_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
