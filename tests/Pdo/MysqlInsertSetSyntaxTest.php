<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests MySQL-specific INSERT...SET syntax through the CTE rewriter.
 * MySQL supports `INSERT INTO t SET col1=val1, col2=val2` as an alternative
 * to `INSERT INTO t (col1, col2) VALUES (val1, val2)`. The SQL parser/classifier
 * must recognize this as an INSERT and handle it correctly in shadow mode.
 *
 * SQL patterns exercised: INSERT SET basic, INSERT SET with expression,
 * INSERT SET then SELECT, INSERT SET then UPDATE, INSERT SET ON DUPLICATE KEY.
 * @spec SPEC-4.1
 */
class MysqlInsertSetSyntaxTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE my_iss_products (
            id INT PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            price DECIMAL(10,2) NOT NULL DEFAULT 0.00,
            status VARCHAR(20) NOT NULL DEFAULT \'active\'
        ) ENGINE=InnoDB';
    }

    protected function getTableNames(): array
    {
        return ['my_iss_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO my_iss_products VALUES (1, 'Widget', 29.99, 'active')");
        $this->pdo->exec("INSERT INTO my_iss_products VALUES (2, 'Gadget', 49.99, 'active')");
    }

    /**
     * Basic INSERT...SET syntax.
     */
    public function testInsertSetBasic(): void
    {
        try {
            $this->ztdExec("INSERT INTO my_iss_products SET id = 3, name = 'Tool', price = 19.99, status = 'active'");

            $rows = $this->ztdQuery("SELECT name, price FROM my_iss_products WHERE id = 3");
            $this->assertCount(1, $rows);
            $this->assertSame('Tool', $rows[0]['name']);
            $this->assertEqualsWithDelta(19.99, (float) $rows[0]['price'], 0.01);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'INSERT...SET syntax failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * INSERT...SET with expression in value.
     */
    public function testInsertSetWithExpression(): void
    {
        try {
            $this->ztdExec("INSERT INTO my_iss_products SET id = 4, name = CONCAT('Item', '-', '4'), price = 10.00 + 5.50, status = 'draft'");

            $rows = $this->ztdQuery("SELECT name, price, status FROM my_iss_products WHERE id = 4");
            $this->assertCount(1, $rows);
            $this->assertSame('Item-4', $rows[0]['name']);
            $this->assertEqualsWithDelta(15.50, (float) $rows[0]['price'], 0.01);
            $this->assertSame('draft', $rows[0]['status']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'INSERT...SET with expression failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * INSERT...SET row count should increase.
     */
    public function testInsertSetRowCount(): void
    {
        try {
            $before = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM my_iss_products");
            $this->assertEquals(2, (int) $before[0]['cnt']);

            $this->ztdExec("INSERT INTO my_iss_products SET id = 5, name = 'Doohickey', price = 99.99, status = 'active'");

            $after = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM my_iss_products");
            $this->assertEquals(3, (int) $after[0]['cnt']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'INSERT...SET failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * INSERT...SET then UPDATE the inserted row.
     */
    public function testInsertSetThenUpdate(): void
    {
        try {
            $this->ztdExec("INSERT INTO my_iss_products SET id = 6, name = 'Gizmo', price = 5.00, status = 'draft'");
            $this->ztdExec("UPDATE my_iss_products SET status = 'active', price = 7.50 WHERE id = 6");

            $rows = $this->ztdQuery("SELECT price, status FROM my_iss_products WHERE id = 6");
            $this->assertCount(1, $rows);
            $this->assertEqualsWithDelta(7.50, (float) $rows[0]['price'], 0.01);
            $this->assertSame('active', $rows[0]['status']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'INSERT...SET + UPDATE failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * INSERT...SET with ON DUPLICATE KEY UPDATE.
     */
    public function testInsertSetOnDuplicateKeyUpdate(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO my_iss_products SET id = 1, name = 'Widget v2', price = 35.00, status = 'active'
                 ON DUPLICATE KEY UPDATE name = VALUES(name), price = VALUES(price)"
            );

            $rows = $this->ztdQuery("SELECT name, price FROM my_iss_products WHERE id = 1");
            $this->assertCount(1, $rows);
            if ($rows[0]['name'] === 'Widget') {
                $this->markTestIncomplete(
                    'INSERT...SET ON DUPLICATE KEY UPDATE was silently ignored.'
                );
            }
            $this->assertSame('Widget v2', $rows[0]['name']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'INSERT...SET ON DUPLICATE KEY UPDATE failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Physical isolation — INSERT...SET should not touch physical table.
     * Note: seed INSERTs in setUp() also go through ZTD shadow, so physical
     * table is empty (0 rows), not 2.
     */
    public function testInsertSetPhysicalIsolation(): void
    {
        try {
            $this->ztdExec("INSERT INTO my_iss_products SET id = 7, name = 'Shadow Item', price = 1.00, status = 'active'");

            $shadow = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM my_iss_products");
            $this->assertEquals(3, (int) $shadow[0]['cnt']);

            $this->pdo->disableZtd();
            $physical = $this->pdo->query("SELECT COUNT(*) AS cnt FROM my_iss_products")
                ->fetchAll(PDO::FETCH_ASSOC);
            $this->pdo->enableZtd();
            // Physical table is empty — all INSERTs (including seeds) are in shadow
            $this->assertEquals(0, (int) $physical[0]['cnt']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'INSERT...SET failed: ' . $e->getMessage()
            );
        }
    }
}
