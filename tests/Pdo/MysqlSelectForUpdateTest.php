<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests SELECT...FOR UPDATE locking clause on MySQL PDO.
 *
 * FOR UPDATE is preserved in CTE-rewritten SQL but is effectively a no-op
 * since the query reads from CTE data, not physical rows.
 * @spec SPEC-10.2.11
 */
class MysqlSelectForUpdateTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pdo_mfu_test (id INT PRIMARY KEY, name VARCHAR(50), balance INT)';
    }

    protected function getTableNames(): array
    {
        return ['pdo_mfu_test'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pdo_mfu_test VALUES (1, 'Alice', 100)");
        $this->pdo->exec("INSERT INTO pdo_mfu_test VALUES (2, 'Bob', 200)");
    }

    /**
     * SELECT...FOR UPDATE returns correct shadow data.
     */
    public function testSelectForUpdateReturnsData(): void
    {
        $stmt = $this->pdo->query('SELECT name, balance FROM pdo_mfu_test WHERE id = 1 FOR UPDATE');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Alice', $row['name']);
        $this->assertSame(100, (int) $row['balance']);
    }

    /**
     * SELECT...FOR SHARE also works.
     */
    public function testSelectForShareReturnsData(): void
    {
        $stmt = $this->pdo->query('SELECT name FROM pdo_mfu_test WHERE id = 2 FOR SHARE');
        $this->assertSame('Bob', $stmt->fetchColumn());
    }

    /**
     * SELECT...LOCK IN SHARE MODE (MySQL-specific) works.
     */
    public function testLockInShareModeReturnsData(): void
    {
        $stmt = $this->pdo->query('SELECT name FROM pdo_mfu_test WHERE id = 1 LOCK IN SHARE MODE');
        $this->assertSame('Alice', $stmt->fetchColumn());
    }

    /**
     * Physical isolation maintained.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_mfu_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
