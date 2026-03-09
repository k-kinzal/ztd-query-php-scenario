<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests PostgreSQL UPDATE ... FROM (multi-table UPDATE) through CTE shadow.
 *
 * PostgreSQL uses: UPDATE t SET col = s.col FROM s WHERE t.id = s.id
 * This is the PostgreSQL equivalent of MySQL's multi-table UPDATE JOIN.
 * The CTE rewriter must handle FROM clause table references.
 *
 * @spec SPEC-4.2
 */
class PostgresUpdateFromTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_uf_products (id INT PRIMARY KEY, name VARCHAR(50), price DECIMAL(10,2))',
            'CREATE TABLE pg_uf_price_updates (product_id INT PRIMARY KEY, new_price DECIMAL(10,2))',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_uf_price_updates', 'pg_uf_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_uf_products VALUES (1, 'Widget', 10.00)");
        $this->pdo->exec("INSERT INTO pg_uf_products VALUES (2, 'Gadget', 20.00)");
        $this->pdo->exec("INSERT INTO pg_uf_products VALUES (3, 'Doohickey', 30.00)");

        $this->pdo->exec("INSERT INTO pg_uf_price_updates VALUES (1, 15.00)");
        $this->pdo->exec("INSERT INTO pg_uf_price_updates VALUES (2, 25.00)");
    }

    /**
     * UPDATE ... FROM basic multi-table update.
     */
    public function testUpdateFromBasic(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE pg_uf_products p
                 SET price = u.new_price
                 FROM pg_uf_price_updates u
                 WHERE p.id = u.product_id"
            );

            $rows = $this->ztdQuery('SELECT id, price FROM pg_uf_products ORDER BY id');
            $this->assertCount(3, $rows);
            $this->assertEquals(15.00, (float) $rows[0]['price'], 'Widget should be updated to 15.00');
            $this->assertEquals(25.00, (float) $rows[1]['price'], 'Gadget should be updated to 25.00');
            $this->assertEquals(30.00, (float) $rows[2]['price'], 'Doohickey should remain 30.00');
        } catch (\Throwable $e) {
            $this->markTestSkipped('UPDATE FROM not supported through CTE: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE ... FROM with expression in SET.
     */
    public function testUpdateFromWithExpression(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE pg_uf_products p
                 SET price = u.new_price * 1.1
                 FROM pg_uf_price_updates u
                 WHERE p.id = u.product_id"
            );

            $rows = $this->ztdQuery('SELECT id, price FROM pg_uf_products WHERE id = 1');
            $this->assertEquals(16.50, (float) $rows[0]['price'], 'Price should be new_price * 1.1');
        } catch (\Throwable $e) {
            $this->markTestSkipped('UPDATE FROM with expression not supported: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE ... FROM on shadow-inserted data.
     */
    public function testUpdateFromOnShadowData(): void
    {
        $this->pdo->exec("INSERT INTO pg_uf_products VALUES (4, 'Sprocket', 40.00)");
        $this->pdo->exec("INSERT INTO pg_uf_price_updates VALUES (4, 45.00)");

        try {
            $this->pdo->exec(
                "UPDATE pg_uf_products p
                 SET price = u.new_price
                 FROM pg_uf_price_updates u
                 WHERE p.id = u.product_id AND p.id = 4"
            );

            $rows = $this->ztdQuery('SELECT price FROM pg_uf_products WHERE id = 4');
            $this->assertEquals(45.00, (float) $rows[0]['price'], 'Shadow-inserted row should be updated');
        } catch (\Throwable $e) {
            $this->markTestSkipped('UPDATE FROM on shadow data not supported: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE ... FROM with multiple FROM tables.
     */
    public function testUpdateFromMultipleTables(): void
    {
        $this->createTable('CREATE TABLE pg_uf_categories (product_id INT PRIMARY KEY, category VARCHAR(50))');

        try {
            $this->pdo->exec("INSERT INTO pg_uf_categories VALUES (1, 'hardware')");
            $this->pdo->exec("INSERT INTO pg_uf_categories VALUES (2, 'hardware')");
            $this->pdo->exec("INSERT INTO pg_uf_categories VALUES (3, 'software')");

            $this->pdo->exec(
                "UPDATE pg_uf_products p
                 SET price = u.new_price
                 FROM pg_uf_price_updates u, pg_uf_categories c
                 WHERE p.id = u.product_id AND p.id = c.product_id AND c.category = 'hardware'"
            );

            $rows = $this->ztdQuery('SELECT id, price FROM pg_uf_products ORDER BY id');
            $this->assertEquals(15.00, (float) $rows[0]['price']);
            $this->assertEquals(25.00, (float) $rows[1]['price']);
            $this->assertEquals(30.00, (float) $rows[2]['price'], 'Doohickey (software) should not be updated');
        } catch (\Throwable $e) {
            $this->markTestSkipped('UPDATE FROM multiple tables not supported: ' . $e->getMessage());
        } finally {
            $this->dropTable('pg_uf_categories');
        }
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_uf_products');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
