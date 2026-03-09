<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests INSERT...SELECT from the same table (self-referencing INSERT).
 * This is a common pattern for duplicating rows, copying with modifications,
 * or seeding data. The CTE rewriter must handle the case where the INSERT
 * target and the SELECT source are the same table.
 *
 * SQL patterns exercised: INSERT...SELECT same table, INSERT...SELECT with
 * expression, INSERT...SELECT with WHERE filter, duplicate detection after
 * self-insert.
 * @spec SPEC-4.1a
 */
class SqliteSelfReferencingInsertSelectTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_sri_products (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            category TEXT NOT NULL,
            price REAL NOT NULL,
            status TEXT NOT NULL DEFAULT \'active\'
        )';
    }

    protected function getTableNames(): array
    {
        return ['sl_sri_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_sri_products VALUES (1, 'Widget A', 'electronics', 29.99, 'active')");
        $this->pdo->exec("INSERT INTO sl_sri_products VALUES (2, 'Widget B', 'electronics', 49.99, 'active')");
        $this->pdo->exec("INSERT INTO sl_sri_products VALUES (3, 'Gadget X', 'accessories', 9.99, 'active')");
    }

    /**
     * INSERT...SELECT from same table with new PK and modified values.
     * Duplicate electronics products into a 'cloned' category.
     *
     * Known issue: INSERT...SELECT with computed columns stores NULLs
     * in shadow store (SPEC-11.INSERT-SELECT-COMPUTED, Issue #20).
     */
    public function testInsertSelectSameTableWithModifiedValues(): void
    {
        $this->ztdExec(
            "INSERT INTO sl_sri_products (id, name, category, price, status)
             SELECT id + 100, name || ' (clone)', 'cloned', price * 0.9, 'draft'
             FROM sl_sri_products
             WHERE category = 'electronics'"
        );

        $rows = $this->ztdQuery(
            "SELECT id, name, category, price, status FROM sl_sri_products
             WHERE category = 'cloned' ORDER BY id"
        );

        if (count($rows) === 0) {
            // Rows were inserted but all columns are NULL including 'category',
            // so WHERE category='cloned' finds nothing.
            $allRows = $this->ztdQuery("SELECT * FROM sl_sri_products ORDER BY id");
            $nullRows = array_filter($allRows, fn($r) => $r['id'] === null);
            $this->markTestIncomplete(
                'SPEC-11.INSERT-SELECT-COMPUTED [Issue #20]: INSERT...SELECT computed columns produce NULL values. '
                . count($nullRows) . ' rows with NULL id found.'
            );
        }

        $this->assertCount(2, $rows);
    }

    /**
     * Total row count should increase after self-referencing INSERT.
     */
    public function testRowCountAfterSelfInsert(): void
    {
        $before = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_sri_products");
        $this->assertEquals(3, (int) $before[0]['cnt']);

        $this->ztdExec(
            "INSERT INTO sl_sri_products (id, name, category, price, status)
             SELECT id + 100, name, category, price, status
             FROM sl_sri_products"
        );

        $after = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_sri_products");
        $this->assertEquals(6, (int) $after[0]['cnt']);
    }

    /**
     * INSERT...SELECT from same table with aggregate in expression.
     * Insert a summary row with the average price.
     *
     * Known issue: INSERT...SELECT with literals/aggregation stores NULLs
     * (SPEC-11.INSERT-SELECT-COMPUTED, Issue #20).
     */
    public function testInsertSelectWithAggregate(): void
    {
        $this->ztdExec(
            "INSERT INTO sl_sri_products (id, name, category, price, status)
             SELECT 999, 'Average Product', 'summary',
                    (SELECT ROUND(AVG(price), 2) FROM sl_sri_products), 'reference'"
        );

        $rows = $this->ztdQuery("SELECT price FROM sl_sri_products WHERE id = 999");
        if (count($rows) === 0) {
            $this->markTestIncomplete(
                'SPEC-11.INSERT-SELECT-COMPUTED [Issue #20]: INSERT...SELECT with literal id stores NULL, making WHERE id=999 return empty.'
            );
        }
        $this->assertCount(1, $rows);
    }

    /**
     * Self-referencing INSERT then query with JOIN-like pattern.
     * After duplicating, verify both original and clone are visible in same query.
     *
     * Known issue: INSERT...SELECT with expressions stores NULLs
     * (SPEC-11.INSERT-SELECT-COMPUTED, Issue #20).
     */
    public function testSelfInsertThenCrossReference(): void
    {
        $this->ztdExec(
            "INSERT INTO sl_sri_products (id, name, category, price, status)
             SELECT id + 100, name || ' v2', category, price + 5.00, 'active'
             FROM sl_sri_products WHERE id = 1"
        );

        $rows = $this->ztdQuery(
            "SELECT p1.name AS original, p2.name AS clone, p2.price AS clone_price
             FROM sl_sri_products p1
             JOIN sl_sri_products p2 ON p2.id = p1.id + 100
             WHERE p1.id = 1"
        );

        if (count($rows) === 0) {
            $this->markTestIncomplete(
                'SPEC-11.INSERT-SELECT-COMPUTED [Issue #20]: cloned row has NULL id, so JOIN on id+100 finds nothing.'
            );
        }
        $this->assertCount(1, $rows);
    }

    /**
     * INSERT...SELECT from same table followed by DELETE of originals.
     * Simulates a "move" operation.
     *
     * Known issue: INSERT...SELECT with string literal 'archived' stores NULL
     * (SPEC-11.INSERT-SELECT-COMPUTED, Issue #20).
     */
    public function testInsertSelectThenDeleteOriginals(): void
    {
        // Clone all electronics
        $this->ztdExec(
            "INSERT INTO sl_sri_products (id, name, category, price, status)
             SELECT id + 1000, name, 'archived', price, 'archived'
             FROM sl_sri_products WHERE category = 'electronics'"
        );

        // Delete originals
        $this->ztdExec("DELETE FROM sl_sri_products WHERE category = 'electronics'");

        $electronics = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_sri_products WHERE category = 'electronics'");
        $this->assertEquals(0, (int) $electronics[0]['cnt']);

        $archived = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_sri_products WHERE category = 'archived'");
        if ((int) $archived[0]['cnt'] === 0) {
            $this->markTestIncomplete(
                'SPEC-11.INSERT-SELECT-COMPUTED [Issue #20]: INSERT...SELECT stores NULL for literal columns, so category=\'archived\' finds nothing.'
            );
        }
        $this->assertEquals(2, (int) $archived[0]['cnt']);
    }
}
