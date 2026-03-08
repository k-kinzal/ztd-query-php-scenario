<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests the CTE shadow replacement behavior on MySQLi: physical data is NOT
 * visible through ZTD queries — the shadow store replaces the physical table.
 * @spec SPEC-2.2
 */
class PhysicalShadowOverlayTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_pso_products (id INT PRIMARY KEY, name VARCHAR(50), price DECIMAL(10,2), category VARCHAR(30))';
    }

    protected function getTableNames(): array
    {
        return ['mi_pso_products'];
    }


    public function testPhysicalDataNotVisibleThroughZtd(): void
    {
        $result = $this->mysqli->query("SELECT COUNT(*) AS cnt FROM mi_pso_products");
        $row = $result->fetch_assoc();
        $this->assertSame(0, (int) $row['cnt']);
    }

    public function testOnlyShadowInsertedDataVisible(): void
    {
        $this->mysqli->query("INSERT INTO mi_pso_products VALUES (10, 'Shadow Widget', 39.99, 'shadow')");

        $result = $this->mysqli->query("SELECT COUNT(*) AS cnt FROM mi_pso_products");
        $row = $result->fetch_assoc();
        $this->assertSame(1, (int) $row['cnt']);
    }

    public function testPhysicalDataUntouched(): void
    {
        $this->mysqli->query("INSERT INTO mi_pso_products VALUES (10, 'Shadow Only', 99.99, 'shadow')");

        $this->mysqli->disableZtd();

        $result = $this->mysqli->query("SELECT COUNT(*) AS cnt FROM mi_pso_products");
        $row = $result->fetch_assoc();
        $this->assertSame(3, (int) $row['cnt']);
    }

    public function testInsertWithOverlappingPhysicalId(): void
    {
        $this->mysqli->query("INSERT INTO mi_pso_products VALUES (1, 'Shadow Widget', 99.99, 'shadow')");

        $result = $this->mysqli->query("SELECT name FROM mi_pso_products WHERE id = 1");
        $row = $result->fetch_assoc();
        $this->assertSame('Shadow Widget', $row['name']);
    }
}
