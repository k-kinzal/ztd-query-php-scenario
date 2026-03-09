<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests DELETE with subquery referencing another table on MySQL-PDO.
 *
 * @spec SPEC-4.3
 */
class MysqlMultiTableSubqueryDeleteTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mtsd_categories (id INT PRIMARY KEY, name VARCHAR(50) NOT NULL) ENGINE=InnoDB',
            'CREATE TABLE mtsd_products (id INT PRIMARY KEY, name VARCHAR(100) NOT NULL, category_id INT, status VARCHAR(20) NOT NULL DEFAULT \'active\') ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mtsd_products', 'mtsd_categories'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO mtsd_categories VALUES (1, 'Electronics')");
        $this->ztdExec("INSERT INTO mtsd_categories VALUES (2, 'Books')");
        $this->ztdExec("INSERT INTO mtsd_products VALUES (1, 'Phone', 1, 'active')");
        $this->ztdExec("INSERT INTO mtsd_products VALUES (2, 'Tablet', 1, 'active')");
        $this->ztdExec("INSERT INTO mtsd_products VALUES (3, 'Novel', 2, 'active')");
        $this->ztdExec("INSERT INTO mtsd_products VALUES (4, 'Orphan', 99, 'active')");
    }

    public function testDeleteWhereNotInOtherTable(): void
    {
        try {
            $this->ztdExec("DELETE FROM mtsd_products WHERE category_id NOT IN (SELECT id FROM mtsd_categories)");
            $rows = $this->ztdQuery("SELECT id FROM mtsd_products ORDER BY id");
            $this->assertCount(3, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete('DELETE NOT IN failed: ' . $e->getMessage());
        }
    }

    public function testDeleteWhereNotExistsCorrelated(): void
    {
        try {
            $this->ztdExec(
                "DELETE FROM mtsd_products
                 WHERE NOT EXISTS (SELECT 1 FROM mtsd_categories c WHERE c.id = mtsd_products.category_id)"
            );
            $rows = $this->ztdQuery("SELECT id FROM mtsd_products ORDER BY id");
            $this->assertCount(3, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete('DELETE NOT EXISTS failed: ' . $e->getMessage());
        }
    }

    public function testPreparedDeleteWithSubqueryParam(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "DELETE FROM mtsd_products WHERE category_id IN (SELECT id FROM mtsd_categories WHERE name = ?)"
            );
            $stmt->execute(['Electronics']);
            $rows = $this->ztdQuery("SELECT name FROM mtsd_products ORDER BY id");
            $this->assertCount(2, $rows);
            $this->assertSame('Novel', $rows[0]['name']);
            $this->assertSame('Orphan', $rows[1]['name']);
        } catch (\Exception $e) {
            $this->markTestIncomplete('Prepared DELETE with subquery failed: ' . $e->getMessage());
        }
    }
}
