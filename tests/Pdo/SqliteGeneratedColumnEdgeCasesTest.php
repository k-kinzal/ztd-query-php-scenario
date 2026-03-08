<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Extended generated column scenarios: generated columns in WHERE, GROUP BY,
 * ORDER BY, and behavior after updating source columns.
 *
 * Key finding: Generated column values are NULL in the ZTD shadow store because
 * the CTE does not re-evaluate generation expressions. The shadow store captures
 * literal values at INSERT time, but generated columns are not included in the
 * INSERT value list (they are computed by the DB engine), so the shadow store
 * stores NULL for them.
 *
 * @spec SPEC-10.2.22
 */
class SqliteGeneratedColumnEdgeCasesTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE products (
            id INTEGER PRIMARY KEY,
            name TEXT,
            price REAL,
            quantity INTEGER,
            total REAL GENERATED ALWAYS AS (price * quantity) STORED
        )';
    }

    protected function getTableNames(): array
    {
        return ['products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO products (id, name, price, quantity) VALUES (1, 'Widget', 9.99, 10)");
        $this->pdo->exec("INSERT INTO products (id, name, price, quantity) VALUES (2, 'Gadget', 29.99, 5)");
        $this->pdo->exec("INSERT INTO products (id, name, price, quantity) VALUES (3, 'Sprocket', 4.99, 20)");
    }

    public function testGeneratedColumnIsNullInShadow(): void
    {
        // Generated column values are NULL in shadow store — the CTE does not
        // re-evaluate the generation expression, and the INSERT omits generated
        // columns, so the shadow store has NULL for the 'total' column.
        $rows = $this->ztdQuery("SELECT name, total FROM products ORDER BY id");
        $this->assertNull($rows[0]['total']); // Widget: should be 99.90 but is NULL
        $this->assertNull($rows[1]['total']); // Gadget: should be 149.95 but is NULL
        $this->assertNull($rows[2]['total']); // Sprocket: should be 99.80 but is NULL
    }

    public function testSourceColumnsPreservedCorrectly(): void
    {
        // Even though generated column is NULL, source columns work fine
        $rows = $this->ztdQuery("SELECT name, price, quantity FROM products ORDER BY id");
        $this->assertEqualsWithDelta(9.99, (float) $rows[0]['price'], 0.01);
        $this->assertSame(10, (int) $rows[0]['quantity']);
    }

    public function testManualExpressionWorkaround(): void
    {
        // Workaround: compute the expression manually in SELECT instead of
        // relying on the generated column
        $rows = $this->ztdQuery("
            SELECT name, price * quantity AS computed_total
            FROM products
            ORDER BY computed_total DESC
        ");
        $this->assertSame('Gadget', $rows[0]['name']);
        $this->assertEqualsWithDelta(149.95, (float) $rows[0]['computed_total'], 0.01);
    }

    public function testGeneratedColumnWhereReturnsEmpty(): void
    {
        // WHERE on generated column: since total is NULL in shadow,
        // WHERE total > 100 returns nothing
        $rows = $this->ztdQuery("SELECT name FROM products WHERE total > 100");
        $this->assertCount(0, $rows);
    }

    public function testManualExpressionInWhere(): void
    {
        // Workaround: use the expression directly in WHERE
        $rows = $this->ztdQuery("
            SELECT name FROM products WHERE price * quantity > 100 ORDER BY name
        ");
        $this->assertCount(1, $rows);
        $this->assertSame('Gadget', $rows[0]['name']);
    }

    public function testUpdateSourceColumnGeneratedStaysNull(): void
    {
        $this->pdo->exec("UPDATE products SET price = 19.99 WHERE id = 1");

        $rows = $this->ztdQuery("SELECT price, quantity, total FROM products WHERE id = 1");
        $this->assertEqualsWithDelta(19.99, (float) $rows[0]['price'], 0.01);
        $this->assertSame(10, (int) $rows[0]['quantity']);
        // Generated column remains NULL after source column update
        $this->assertNull($rows[0]['total']);
    }

    public function testAggregateOnGeneratedColumnIsNull(): void
    {
        // SUM of NULLs is NULL
        $rows = $this->ztdQuery("SELECT SUM(total) AS grand_total FROM products");
        $this->assertNull($rows[0]['grand_total']);
    }

    public function testAggregateOnManualExpression(): void
    {
        // Workaround: aggregate on manual expression
        $rows = $this->ztdQuery("SELECT SUM(price * quantity) AS grand_total FROM products");
        $this->assertEqualsWithDelta(349.65, (float) $rows[0]['grand_total'], 0.01);
    }

    public function testGroupByWithManualExpression(): void
    {
        $rows = $this->ztdQuery("
            SELECT CASE
                WHEN price * quantity < 100 THEN 'low'
                WHEN price * quantity < 150 THEN 'mid'
                ELSE 'high'
            END AS tier,
            COUNT(*) AS cnt
            FROM products
            GROUP BY tier
            ORDER BY tier
        ");
        $this->assertGreaterThanOrEqual(1, count($rows));
    }

    public function testPreparedWithManualExpressionReturnsEmpty(): void
    {
        // Prepared statement with arithmetic expression and bound param in WHERE
        // returns empty on SQLite — likely related to how the CTE rewriter handles
        // the expression + placeholder combination. The non-prepared version works.
        $rows = $this->ztdPrepareAndExecute(
            "SELECT name, price * quantity AS computed_total FROM products WHERE price * quantity > ? ORDER BY computed_total",
            [100]
        );
        // Known limitation: returns empty
        $this->assertCount(0, $rows);
    }

    public function testNonPreparedWithManualExpression(): void
    {
        // Non-prepared version works correctly
        $rows = $this->ztdQuery(
            "SELECT name, price * quantity AS computed_total FROM products WHERE price * quantity > 100 ORDER BY computed_total"
        );
        $this->assertCount(1, $rows);
        $this->assertSame('Gadget', $rows[0]['name']);
    }

    public function testDeleteByManualExpression(): void
    {
        $this->pdo->exec("DELETE FROM products WHERE price * quantity < 100");

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM products");
        $this->assertSame(1, (int) $rows[0]['cnt']); // Only Gadget
    }

    public function testCoalesceOnGeneratedColumn(): void
    {
        // COALESCE can provide a fallback for NULL generated column
        $rows = $this->ztdQuery("
            SELECT name, COALESCE(total, price * quantity) AS effective_total
            FROM products
            ORDER BY id
        ");
        $this->assertEqualsWithDelta(99.90, (float) $rows[0]['effective_total'], 0.01);
        $this->assertEqualsWithDelta(149.95, (float) $rows[1]['effective_total'], 0.01);
    }
}
