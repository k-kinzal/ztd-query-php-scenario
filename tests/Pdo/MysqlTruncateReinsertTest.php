<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests TRUNCATE TABLE + re-insert workflow on MySQL PDO.
 *
 * TRUNCATE clears the shadow store, then new INSERTs populate cleanly.
 * @spec SPEC-5.3
 */
class MysqlTruncateReinsertTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pdo_mtr_test (id INT PRIMARY KEY, name VARCHAR(50))';
    }

    protected function getTableNames(): array
    {
        return ['pdo_mtr_test'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pdo_mtr_test VALUES (1, 'Alice')");
        $this->pdo->exec("INSERT INTO pdo_mtr_test VALUES (2, 'Bob')");
    }

    /**
     * TRUNCATE clears shadow store.
     */
    public function testTruncateClearsShadow(): void
    {
        $this->pdo->exec('TRUNCATE TABLE pdo_mtr_test');

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_mtr_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }

    /**
     * INSERT after TRUNCATE works.
     */
    public function testInsertAfterTruncate(): void
    {
        $this->pdo->exec('TRUNCATE TABLE pdo_mtr_test');

        $this->pdo->exec("INSERT INTO pdo_mtr_test VALUES (1, 'Charlie')");

        $stmt = $this->pdo->query('SELECT name FROM pdo_mtr_test WHERE id = 1');
        $this->assertSame('Charlie', $stmt->fetchColumn());
    }

    /**
     * Multiple truncate-reinsert cycles work.
     */
    public function testMultipleTruncateCycles(): void
    {
        $this->pdo->exec('TRUNCATE TABLE pdo_mtr_test');
        $this->pdo->exec("INSERT INTO pdo_mtr_test VALUES (1, 'Round1')");

        $this->pdo->exec('TRUNCATE TABLE pdo_mtr_test');
        $this->pdo->exec("INSERT INTO pdo_mtr_test VALUES (1, 'Round2')");

        $stmt = $this->pdo->query('SELECT name FROM pdo_mtr_test WHERE id = 1');
        $this->assertSame('Round2', $stmt->fetchColumn());

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_mtr_test');
        $this->assertSame(1, (int) $stmt->fetchColumn());
    }

    /**
     * Physical isolation after TRUNCATE.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec('TRUNCATE TABLE pdo_mtr_test');
        $this->pdo->exec("INSERT INTO pdo_mtr_test VALUES (1, 'New')");

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_mtr_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
