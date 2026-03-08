<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests debugDumpParams() on ZtdPdoStatement with MySQL PDO.
 * Confirms ZTD rewrites are visible in debug output.
 * @spec pending
 */
class MysqlDebugDumpParamsTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE ddp_mysql (id INT PRIMARY KEY, name VARCHAR(50), score INT)';
    }

    protected function getTableNames(): array
    {
        return ['ddp_mysql'];
    }


    public function testDebugDumpParamsOnSelect(): void
    {
        $stmt = $this->pdo->prepare('SELECT * FROM ddp_mysql WHERE id = ?');
        $stmt->execute([1]);

        ob_start();
        $stmt->debugDumpParams();
        $output = ob_get_clean();

        $this->assertIsString($output);
        $this->assertStringContainsString('SELECT', $output);
        $this->assertStringContainsString('Params:', $output);
    }

    public function testDebugDumpParamsOnInsertShowsRewrittenSql(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO ddp_mysql (id, name, score) VALUES (?, ?, ?)');
        $stmt->execute([1, 'Alice', 100]);

        ob_start();
        $stmt->debugDumpParams();
        $output = ob_get_clean();

        $this->assertIsString($output);
        // ZTD rewrites INSERT to SELECT for shadow store
        $this->assertStringContainsString('SELECT', $output);
    }

    public function testDebugDumpParamsOnUpdateShowsRewrittenSql(): void
    {
        $this->pdo->exec("INSERT INTO ddp_mysql VALUES (1, 'Alice', 100)");

        $stmt = $this->pdo->prepare('UPDATE ddp_mysql SET score = ? WHERE id = ?');
        $stmt->execute([200, 1]);

        ob_start();
        $stmt->debugDumpParams();
        $output = ob_get_clean();

        $this->assertIsString($output);
        // ZTD rewrites UPDATE to CTE WITH + SELECT
        $this->assertStringContainsString('WITH', $output);
        $this->assertStringContainsString('SELECT', $output);
    }

    public function testDebugDumpParamsWithNamedParams(): void
    {
        $stmt = $this->pdo->prepare('SELECT * FROM ddp_mysql WHERE name = :name');
        $stmt->execute([':name' => 'Alice']);

        ob_start();
        $stmt->debugDumpParams();
        $output = ob_get_clean();

        $this->assertIsString($output);
        $this->assertStringContainsString('Params:', $output);
    }
}
