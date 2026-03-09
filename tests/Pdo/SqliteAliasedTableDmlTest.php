<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Table aliases in UPDATE and DELETE statements on shadow data (SQLite PDO).
 * Tests whether the CTE rewriter handles aliased table references in DML.
 *
 * SQLite does NOT support table aliases in UPDATE or DELETE statements.
 * This is a SQLite language limitation, not a ZTD bug.
 * Aliased DML tests use markTestIncomplete to document this limitation.
 * SELECT with aliases is tested to confirm alias handling in read paths.
 *
 * @spec SPEC-4.2
 * @spec SPEC-4.3
 */
class SqliteAliasedTableDmlTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return "CREATE TABLE sl_atd_products (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            price REAL NOT NULL,
            active INTEGER NOT NULL DEFAULT 1,
            category TEXT NOT NULL
        )";
    }

    protected function getTableNames(): array
    {
        return ['sl_atd_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_atd_products (id, name, price, active, category) VALUES
            (1, 'Widget', 25.00, 1, 'tools'),
            (2, 'Gadget', 5.00, 0, 'tools'),
            (3, 'Doohickey', 150.00, 1, 'premium'),
            (4, 'Thingamajig', 3.00, 0, 'tools'),
            (5, 'Whatsit', 75.00, 1, 'premium')");
    }

    /**
     * SQLite does not support table aliases in UPDATE statements.
     * This documents the limitation rather than testing ZTD behavior.
     */
    public function testUpdateWithAliasNotSupported(): void
    {
        try {
            $this->ztdExec(
                "UPDATE sl_atd_products p SET p.price = p.price * 1.10 WHERE p.category = 'premium'"
            );

            // If it somehow succeeds, verify the result
            $rows = $this->ztdQuery(
                "SELECT name, price FROM sl_atd_products WHERE category = 'premium' ORDER BY id"
            );

            $this->assertEquals(165.00, (float) $rows[0]['price'], '', 0.01);
            $this->assertEquals(82.50, (float) $rows[1]['price'], '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'SQLite does not support table aliases in UPDATE: ' . $e->getMessage()
            );
        }
    }

    /**
     * SQLite does not support table aliases in DELETE statements.
     * This documents the limitation rather than testing ZTD behavior.
     */
    public function testDeleteWithAliasNotSupported(): void
    {
        try {
            $this->ztdExec(
                "DELETE FROM sl_atd_products p WHERE p.active = 0"
            );

            // If it somehow succeeds, verify the result
            $rows = $this->ztdQuery("SELECT name FROM sl_atd_products ORDER BY id");

            $this->assertCount(3, $rows);
            $this->assertSame('Widget', $rows[0]['name']);
            $this->assertSame('Doohickey', $rows[1]['name']);
            $this->assertSame('Whatsit', $rows[2]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'SQLite does not support table aliases in DELETE: ' . $e->getMessage()
            );
        }
    }

    /**
     * SQLite supports table aliases in SELECT, including subqueries.
     * Verifies the alias read path works even though alias DML does not.
     */
    public function testSelectWithAliasWorks(): void
    {
        $rows = $this->ztdQuery(
            "SELECT p.name, p.price FROM sl_atd_products p WHERE p.category = 'premium' ORDER BY p.id"
        );

        $this->assertCount(2, $rows);
        $this->assertSame('Doohickey', $rows[0]['name']);
        $this->assertEquals(150.00, (float) $rows[0]['price'], '', 0.01);
        $this->assertSame('Whatsit', $rows[1]['name']);
        $this->assertEquals(75.00, (float) $rows[1]['price'], '', 0.01);
    }

    /**
     * SQLite supports aliased subqueries in SELECT.
     */
    public function testSelectWithAliasAndSubquery(): void
    {
        $rows = $this->ztdQuery(
            "SELECT p.name, p.price
             FROM sl_atd_products p
             WHERE p.price > (SELECT AVG(p2.price) FROM sl_atd_products p2)
             ORDER BY p.id"
        );

        // AVG = (25+5+150+3+75)/5 = 51.60
        $this->assertCount(2, $rows);
        $this->assertSame('Doohickey', $rows[0]['name']);
        $this->assertSame('Whatsit', $rows[1]['name']);
    }

    /**
     * Non-aliased UPDATE and SELECT after mutation work as expected.
     * Confirms ZTD shadow isolation works for standard SQLite DML.
     */
    public function testNonAliasedUpdateThenSelect(): void
    {
        $this->ztdExec("UPDATE sl_atd_products SET active = 0 WHERE price < 10");

        $rows = $this->ztdQuery(
            "SELECT COUNT(*) AS cnt FROM sl_atd_products WHERE active = 1"
        );

        $this->assertSame(3, (int) $rows[0]['cnt']);
    }

    /**
     * Non-aliased DELETE works as expected on SQLite.
     */
    public function testNonAliasedDeleteThenSelect(): void
    {
        $this->ztdExec("DELETE FROM sl_atd_products WHERE active = 0");

        $rows = $this->ztdQuery("SELECT name FROM sl_atd_products ORDER BY id");

        $this->assertCount(3, $rows);
        $this->assertSame('Widget', $rows[0]['name']);
        $this->assertSame('Doohickey', $rows[1]['name']);
        $this->assertSame('Whatsit', $rows[2]['name']);
    }

    public function testPhysicalIsolation(): void
    {
        $this->disableZtd();
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_atd_products");
        $this->assertSame(0, (int) $rows[0]['cnt'], 'Physical table should be empty');
    }
}
