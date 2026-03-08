<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests the CTE shadow replacement behavior on MySQL PDO: physical data is NOT
 * visible through ZTD queries — the shadow store replaces the physical table.
 * @spec SPEC-2.2
 */
class MysqlPhysicalShadowOverlayTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mysql_pso_products (id INT PRIMARY KEY, name VARCHAR(50), price DECIMAL(10,2), category VARCHAR(30))';
    }

    protected function getTableNames(): array
    {
        return ['mysql_pso_products'];
    }


    public function testPhysicalDataNotVisibleThroughZtd(): void
    {
        $stmt = $this->pdo->query("SELECT COUNT(*) AS cnt FROM mysql_pso_products");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $row['cnt']);
    }

    public function testOnlyShadowInsertedDataVisible(): void
    {
        $this->pdo->exec("INSERT INTO mysql_pso_products VALUES (10, 'Shadow Widget', 39.99, 'shadow')");

        $stmt = $this->pdo->query("SELECT COUNT(*) AS cnt FROM mysql_pso_products");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(1, (int) $row['cnt']);
    }

    public function testUpdateOnPhysicalRowMatchesNothing(): void
    {
        $affected = $this->pdo->exec("UPDATE mysql_pso_products SET price = 999.99 WHERE id = 1");
        $this->assertSame(0, $affected);
    }

    public function testPhysicalDataUntouched(): void
    {
        $this->pdo->exec("INSERT INTO mysql_pso_products VALUES (10, 'Shadow Only', 99.99, 'shadow')");

        $this->pdo->disableZtd();

        $stmt = $this->pdo->query("SELECT COUNT(*) AS cnt FROM mysql_pso_products");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(3, (int) $row['cnt']);

        $stmt = $this->pdo->query("SELECT * FROM mysql_pso_products WHERE id = 10");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertFalse($row);
    }

    public function testInsertWithOverlappingPhysicalId(): void
    {
        $this->pdo->exec("INSERT INTO mysql_pso_products VALUES (1, 'Shadow Widget', 99.99, 'shadow')");

        $stmt = $this->pdo->query("SELECT name FROM mysql_pso_products WHERE id = 1");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Shadow Widget', $row['name']);
    }
}
