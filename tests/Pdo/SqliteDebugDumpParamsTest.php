<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests debugDumpParams() on ZtdPdoStatement across various prepared statement scenarios.
 * @spec pending
 */
class SqliteDebugDumpParamsTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE ddp_test (id INT PRIMARY KEY, name VARCHAR(50), score INT)';
    }

    protected function getTableNames(): array
    {
        return ['ddp_test'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec('CREATE TABLE ddp_test (id INT PRIMARY KEY, name VARCHAR(50), score INT)');
        $this->pdo->exec("INSERT INTO ddp_test VALUES (1, 'Alice', 100)");
        $this->pdo->exec("INSERT INTO ddp_test VALUES (2, 'Bob', 85)");

        }

    public function testDebugDumpParamsOnPositionalSelect(): void
    {
        $stmt = $this->pdo->prepare('SELECT * FROM ddp_test WHERE id = ?');
        $stmt->execute([1]);

        ob_start();
        $stmt->debugDumpParams();
        $output = ob_get_clean();

        $this->assertIsString($output);
        $this->assertNotEmpty($output);
        // Should contain the SQL query
        $this->assertStringContainsString('SELECT', $output);
    }

    public function testDebugDumpParamsOnNamedSelect(): void
    {
        $stmt = $this->pdo->prepare('SELECT * FROM ddp_test WHERE id = :id');
        $stmt->execute([':id' => 1]);

        ob_start();
        $stmt->debugDumpParams();
        $output = ob_get_clean();

        $this->assertIsString($output);
        $this->assertStringContainsString('SELECT', $output);
    }

    public function testDebugDumpParamsBeforeExecute(): void
    {
        $stmt = $this->pdo->prepare('SELECT * FROM ddp_test WHERE id = ?');

        ob_start();
        $stmt->debugDumpParams();
        $output = ob_get_clean();

        // Should still produce output even before execute
        $this->assertIsString($output);
        $this->assertNotEmpty($output);
    }

    public function testDebugDumpParamsOnInsertShowsRewrittenSql(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO ddp_test (id, name, score) VALUES (?, ?, ?)');
        $stmt->execute([3, 'Charlie', 70]);

        ob_start();
        $stmt->debugDumpParams();
        $output = ob_get_clean();

        $this->assertIsString($output);
        // ZTD rewrites INSERT to SELECT for shadow store — debugDumpParams reflects the rewritten SQL
        $this->assertStringContainsString('SELECT', $output);
        $this->assertStringNotContainsString('INSERT', $output);
    }

    public function testDebugDumpParamsOnUpdateShowsRewrittenSql(): void
    {
        $stmt = $this->pdo->prepare('UPDATE ddp_test SET score = ? WHERE id = ?');
        $stmt->execute([200, 1]);

        ob_start();
        $stmt->debugDumpParams();
        $output = ob_get_clean();

        $this->assertIsString($output);
        // ZTD rewrites UPDATE to CTE WITH + SELECT — debugDumpParams reflects the rewritten SQL
        $this->assertStringContainsString('WITH', $output);
        $this->assertStringContainsString('SELECT', $output);
        $this->assertStringNotContainsString('UPDATE', $output);
    }

    public function testDebugDumpParamsOnDeleteShowsRewrittenSql(): void
    {
        $stmt = $this->pdo->prepare('DELETE FROM ddp_test WHERE id = ?');
        $stmt->execute([1]);

        ob_start();
        $stmt->debugDumpParams();
        $output = ob_get_clean();

        $this->assertIsString($output);
        // ZTD rewrites DELETE to CTE WITH + SELECT — debugDumpParams reflects the rewritten SQL
        $this->assertStringContainsString('WITH', $output);
        $this->assertStringContainsString('SELECT', $output);
        $this->assertStringNotContainsString('DELETE', $output);
    }

    public function testDebugDumpParamsWithBindValue(): void
    {
        $stmt = $this->pdo->prepare('SELECT * FROM ddp_test WHERE name = :name AND score > :min');
        $stmt->bindValue(':name', 'Alice', PDO::PARAM_STR);
        $stmt->bindValue(':min', 50, PDO::PARAM_INT);
        $stmt->execute();

        ob_start();
        $stmt->debugDumpParams();
        $output = ob_get_clean();

        $this->assertIsString($output);
        $this->assertStringContainsString('SELECT', $output);
    }

    public function testDebugDumpParamsWithMultiplePositionalParams(): void
    {
        $stmt = $this->pdo->prepare('SELECT * FROM ddp_test WHERE id > ? AND score < ?');
        $stmt->execute([0, 200]);

        ob_start();
        $stmt->debugDumpParams();
        $output = ob_get_clean();

        $this->assertIsString($output);
        // Params count should be reflected in output
        $this->assertStringContainsString('Params:', $output);
    }

    public function testDebugDumpParamsAfterReExecution(): void
    {
        $stmt = $this->pdo->prepare('SELECT * FROM ddp_test WHERE id = ?');
        $stmt->execute([1]);
        $stmt->fetchAll();

        // Re-execute with different param
        $stmt->execute([2]);

        ob_start();
        $stmt->debugDumpParams();
        $output = ob_get_clean();

        $this->assertIsString($output);
        $this->assertNotEmpty($output);
    }
}
