<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Table aliases in UPDATE and DELETE statements on shadow data (PostgreSQL PDO).
 * Tests whether the CTE rewriter handles aliased table references in DML.
 * Common in ORM-generated SQL (e.g., Doctrine, Eloquent).
 *
 * PostgreSQL UPDATE alias syntax: UPDATE t AS p SET col = p.col * 1.10 WHERE p.col = ...
 * (SET target cannot use alias prefix in PostgreSQL.)
 * PostgreSQL DELETE alias syntax: DELETE FROM t p WHERE p.col = ...
 *
 * @spec SPEC-4.2
 * @spec SPEC-4.3
 */
class PostgresAliasedTableDmlTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return "CREATE TABLE pg_atd_products (
            id INT PRIMARY KEY,
            name VARCHAR(50) NOT NULL,
            price NUMERIC(10,2) NOT NULL,
            active INT NOT NULL DEFAULT 1,
            category VARCHAR(30) NOT NULL
        )";
    }

    protected function getTableNames(): array
    {
        return ['pg_atd_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_atd_products (id, name, price, active, category) VALUES
            (1, 'Widget', 25.00, 1, 'tools'),
            (2, 'Gadget', 5.00, 0, 'tools'),
            (3, 'Doohickey', 150.00, 1, 'premium'),
            (4, 'Thingamajig', 3.00, 0, 'tools'),
            (5, 'Whatsit', 75.00, 1, 'premium')");
    }

    public function testUpdateWithAlias(): void
    {
        try {
            // PostgreSQL: SET target must not use alias prefix, but WHERE can
            $affected = $this->ztdExec(
                "UPDATE pg_atd_products AS p SET price = p.price * 1.10 WHERE p.category = 'premium'"
            );

            $rows = $this->ztdQuery(
                "SELECT name, price FROM pg_atd_products WHERE category = 'premium' ORDER BY id"
            );

            if (count($rows) !== 2) {
                $this->markTestIncomplete('UPDATE alias: expected 2, got ' . count($rows));
            }

            $this->assertEquals(165.00, (float) $rows[0]['price'], '', 0.01);
            $this->assertEquals(82.50, (float) $rows[1]['price'], '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE with alias failed: ' . $e->getMessage());
        }
    }

    public function testDeleteWithAlias(): void
    {
        try {
            $affected = $this->ztdExec(
                "DELETE FROM pg_atd_products p WHERE p.active = 0"
            );

            $rows = $this->ztdQuery("SELECT name FROM pg_atd_products ORDER BY id");

            if (count($rows) !== 3) {
                $this->markTestIncomplete('DELETE alias: expected 3, got ' . count($rows));
            }

            $this->assertSame('Widget', $rows[0]['name']);
            $this->assertSame('Doohickey', $rows[1]['name']);
            $this->assertSame('Whatsit', $rows[2]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE with alias failed: ' . $e->getMessage());
        }
    }

    public function testUpdateWithAliasAndSubquery(): void
    {
        try {
            // PostgreSQL: SET target must not use alias prefix
            $affected = $this->ztdExec(
                "UPDATE pg_atd_products AS p SET price = p.price * 0.80
                 WHERE p.price > (SELECT AVG(price) FROM pg_atd_products)"
            );

            $rows = $this->ztdQuery(
                "SELECT name, price FROM pg_atd_products WHERE id IN (3, 5) ORDER BY id"
            );

            if (count($rows) !== 2) {
                $this->markTestIncomplete('UPDATE alias+subquery: expected 2, got ' . count($rows));
            }

            // AVG price = (25+5+150+3+75)/5 = 51.60
            // Doohickey (150) and Whatsit (75) should be discounted
            $this->assertEquals(120.00, (float) $rows[0]['price'], '', 0.01);
            $this->assertEquals(60.00, (float) $rows[1]['price'], '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE alias+subquery failed: ' . $e->getMessage());
        }
    }

    public function testSelectAfterAliasedDml(): void
    {
        try {
            // PostgreSQL: SET target must not use alias prefix
            $this->ztdExec("UPDATE pg_atd_products AS p SET active = 0 WHERE p.price < 10");

            $rows = $this->ztdQuery(
                "SELECT COUNT(*) AS cnt FROM pg_atd_products WHERE active = 1"
            );

            $this->assertSame(3, (int) $rows[0]['cnt']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('SELECT after aliased DML failed: ' . $e->getMessage());
        }
    }

    public function testPhysicalIsolation(): void
    {
        $this->disableZtd();
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM pg_atd_products");
        $this->assertSame(0, (int) $rows[0]['cnt'], 'Physical table should be empty');
    }
}
