<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests DELETE without WHERE clause on MySQL PDO.
 *
 * MySQL correctly clears the shadow store.
 * @spec SPEC-4.3
 */
class MysqlDeleteWithoutWhereTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pdo_mdww_test (id INT PRIMARY KEY, name VARCHAR(50))';
    }

    protected function getTableNames(): array
    {
        return ['pdo_mdww_test'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pdo_mdww_test VALUES (1, 'Alice')");
        $this->pdo->exec("INSERT INTO pdo_mdww_test VALUES (2, 'Bob')");
        $this->pdo->exec("INSERT INTO pdo_mdww_test VALUES (3, 'Charlie')");
    }

    /**
     * DELETE without WHERE works correctly on MySQL.
     */
    public function testDeleteWithoutWhereWorks(): void
    {
        $this->pdo->exec('DELETE FROM pdo_mdww_test');

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_mdww_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }

    /**
     * DELETE with WHERE 1=1 also works.
     */
    public function testDeleteWithWhereTrueWorks(): void
    {
        $this->pdo->exec('DELETE FROM pdo_mdww_test WHERE 1=1');

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_mdww_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec('DELETE FROM pdo_mdww_test');

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_mdww_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
