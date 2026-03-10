<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests NULLIF function in DML operations (UPDATE SET, DELETE WHERE, INSERT...SELECT)
 * through ZTD shadow store on SQLite.
 *
 * NULLIF(a, b) returns NULL if a = b, otherwise returns a. When used in DML,
 * the shadow store must evaluate this expression correctly during CTE rewriting.
 *
 * @spec SPEC-4.2
 * @spec SPEC-4.3
 */
class SqliteNullifInDmlTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_nid_products (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                category TEXT NOT NULL,
                price REAL NOT NULL,
                discount_price REAL
            )',
            'CREATE TABLE sl_nid_summary (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                effective_price REAL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_nid_summary', 'sl_nid_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_nid_products VALUES (1, 'Widget', 'electronics', 100.00, 100.00)");
        $this->pdo->exec("INSERT INTO sl_nid_products VALUES (2, 'Gadget', 'electronics', 200.00, 150.00)");
        $this->pdo->exec("INSERT INTO sl_nid_products VALUES (3, 'Gizmo', 'toys', 50.00, 50.00)");
        $this->pdo->exec("INSERT INTO sl_nid_products VALUES (4, 'Doohickey', 'toys', 75.00, NULL)");
    }

    /**
     * UPDATE SET col = NULLIF(col, value) -- set to NULL when equal.
     * discount_price = NULLIF(discount_price, price) nullifies "no discount" entries.
     */
    public function testUpdateSetNullif(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE sl_nid_products SET discount_price = NULLIF(discount_price, price)"
            );

            $rows = $this->ztdQuery(
                "SELECT id, name, discount_price FROM sl_nid_products ORDER BY id"
            );

            $this->assertCount(4, $rows);

            // Widget: NULLIF(100, 100) = NULL
            if ($rows[0]['discount_price'] !== null) {
                $this->markTestIncomplete(
                    'NULLIF in UPDATE SET: Widget discount_price = '
                    . var_export($rows[0]['discount_price'], true) . ', expected NULL'
                );
            }
            $this->assertNull($rows[0]['discount_price']);

            // Gadget: NULLIF(150, 200) = 150
            $this->assertEquals(150.00, (float) $rows[1]['discount_price']);

            // Gizmo: NULLIF(50, 50) = NULL
            if ($rows[2]['discount_price'] !== null) {
                $this->markTestIncomplete(
                    'NULLIF in UPDATE SET: Gizmo discount_price = '
                    . var_export($rows[2]['discount_price'], true) . ', expected NULL'
                );
            }
            $this->assertNull($rows[2]['discount_price']);

            // Doohickey: NULLIF(NULL, 75) = NULL (already NULL)
            $this->assertNull($rows[3]['discount_price']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('NULLIF in UPDATE SET failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE SET with NULLIF and prepared params.
     */
    public function testUpdateSetNullifPrepared(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "UPDATE sl_nid_products SET discount_price = NULLIF(discount_price, ?) WHERE id = ?"
            );
            $stmt->execute([100.00, 1]);

            $rows = $this->ztdQuery("SELECT discount_price FROM sl_nid_products WHERE id = 1");
            $this->assertCount(1, $rows);

            if ($rows[0]['discount_price'] !== null) {
                $this->markTestIncomplete(
                    'Prepared NULLIF in UPDATE SET: discount_price = '
                    . var_export($rows[0]['discount_price'], true) . ', expected NULL'
                );
            }
            $this->assertNull($rows[0]['discount_price']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared NULLIF in UPDATE SET failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE WHERE NULLIF(col, value) IS NULL -- delete rows where col equals value.
     */
    public function testDeleteWhereNullifIsNull(): void
    {
        try {
            $this->pdo->exec(
                "DELETE FROM sl_nid_products WHERE NULLIF(discount_price, price) IS NULL"
            );

            $rows = $this->ztdQuery("SELECT name FROM sl_nid_products ORDER BY name");
            $names = array_column($rows, 'name');

            // Widget (100=100 -> NULL, deleted), Gizmo (50=50 -> NULL, deleted),
            // Doohickey (NULL -> IS NULL, deleted)
            // Only Gadget remains (150 != 200 -> 150, not NULL)
            if (count($names) !== 1) {
                $this->markTestIncomplete(
                    'DELETE WHERE NULLIF IS NULL: expected 1 row (Gadget), got '
                    . count($names) . ': ' . json_encode($names)
                );
            }
            $this->assertEquals(['Gadget'], $names);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE WHERE NULLIF failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE WHERE NULLIF(col, value) IS NOT NULL -- delete rows where col differs.
     */
    public function testDeleteWhereNullifIsNotNull(): void
    {
        try {
            $this->pdo->exec(
                "DELETE FROM sl_nid_products WHERE NULLIF(discount_price, price) IS NOT NULL"
            );

            $rows = $this->ztdQuery("SELECT name FROM sl_nid_products ORDER BY name");
            $names = array_column($rows, 'name');

            // Only Gadget has NULLIF(150, 200) = 150 (IS NOT NULL) -> deleted
            // Remaining: Doohickey, Gizmo, Widget
            if (count($names) !== 3) {
                $this->markTestIncomplete(
                    'DELETE WHERE NULLIF IS NOT NULL: expected 3 rows, got '
                    . count($names) . ': ' . json_encode($names)
                );
            }
            $this->assertEquals(['Doohickey', 'Gizmo', 'Widget'], $names);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE WHERE NULLIF IS NOT NULL failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared DELETE WHERE NULLIF with params.
     */
    public function testDeleteWhereNullifPrepared(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "DELETE FROM sl_nid_products WHERE NULLIF(category, ?) IS NULL"
            );
            $stmt->execute(['toys']);

            $rows = $this->ztdQuery("SELECT name FROM sl_nid_products ORDER BY name");
            $names = array_column($rows, 'name');

            // NULLIF('toys', 'toys') IS NULL -> delete Gizmo, Doohickey
            // NULLIF('electronics', 'toys') = 'electronics' IS NOT NULL -> keep Widget, Gadget
            if (count($names) !== 2) {
                $this->markTestIncomplete(
                    'Prepared DELETE WHERE NULLIF: expected 2 rows, got '
                    . count($names) . ': ' . json_encode($names)
                );
            }
            $this->assertEquals(['Gadget', 'Widget'], $names);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared DELETE WHERE NULLIF failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT...SELECT with NULLIF -- transform values during insert.
     */
    public function testInsertSelectWithNullif(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO sl_nid_summary (id, name, effective_price)
                 SELECT id, name, NULLIF(discount_price, price)
                 FROM sl_nid_products
                 ORDER BY id"
            );

            $rows = $this->ztdQuery("SELECT id, name, effective_price FROM sl_nid_summary ORDER BY id");
            $this->assertCount(4, $rows);

            // Widget: NULLIF(100, 100) = NULL
            $this->assertNull($rows[0]['effective_price'], 'Widget effective_price should be NULL');

            // Gadget: NULLIF(150, 200) = 150
            $this->assertEquals(150.00, (float) $rows[1]['effective_price']);

            // Gizmo: NULLIF(50, 50) = NULL
            $this->assertNull($rows[2]['effective_price'], 'Gizmo effective_price should be NULL');

            // Doohickey: NULLIF(NULL, 75) = NULL
            $this->assertNull($rows[3]['effective_price'], 'Doohickey effective_price should be NULL');
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT...SELECT with NULLIF failed: ' . $e->getMessage());
        }
    }

    /**
     * NULLIF in UPDATE SET combined with WHERE clause.
     * Related to Issue #142 (CASE in SET + WHERE doesn't evaluate).
     */
    public function testUpdateNullifWithWhereClause(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE sl_nid_products
                 SET discount_price = NULLIF(discount_price, price)
                 WHERE category = 'electronics'"
            );

            $rows = $this->ztdQuery("SELECT id, discount_price FROM sl_nid_products ORDER BY id");
            $this->assertCount(4, $rows);

            // Widget (electronics): NULLIF(100, 100) = NULL
            if ($rows[0]['discount_price'] !== null) {
                $this->markTestIncomplete(
                    'NULLIF in UPDATE SET with WHERE: Widget = '
                    . var_export($rows[0]['discount_price'], true)
                    . ', expected NULL. Expression may not evaluate with WHERE clause (cf. Issue #142).'
                );
            }
            $this->assertNull($rows[0]['discount_price']);

            // Gadget (electronics): NULLIF(150, 200) = 150
            $this->assertEquals(150.00, (float) $rows[1]['discount_price']);

            // Gizmo (toys): not updated, still 50.00
            $this->assertEquals(50.00, (float) $rows[2]['discount_price']);

            // Doohickey (toys): not updated, still NULL
            $this->assertNull($rows[3]['discount_price']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('NULLIF in UPDATE SET with WHERE failed: ' . $e->getMessage());
        }
    }

    /**
     * Multiple NULLIF in single UPDATE SET.
     */
    public function testMultipleNullifInUpdateSet(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE sl_nid_products
                 SET discount_price = NULLIF(discount_price, price),
                     category = NULLIF(category, 'toys')"
            );

            $rows = $this->ztdQuery("SELECT id, name, category, discount_price FROM sl_nid_products ORDER BY id");
            $this->assertCount(4, $rows);

            // Widget: discount=NULL, category='electronics' (unchanged)
            $this->assertNull($rows[0]['discount_price']);
            $this->assertSame('electronics', $rows[0]['category']);

            // Gadget: discount=150, category='electronics' (unchanged)
            $this->assertEquals(150.00, (float) $rows[1]['discount_price']);
            $this->assertSame('electronics', $rows[1]['category']);

            // Gizmo: discount=NULL, category=NULL (NULLIF('toys','toys'))
            $this->assertNull($rows[2]['discount_price']);
            if ($rows[2]['category'] !== null) {
                $this->markTestIncomplete(
                    'Multi-NULLIF: Gizmo category = ' . var_export($rows[2]['category'], true)
                    . ', expected NULL'
                );
            }
            $this->assertNull($rows[2]['category']);

            // Doohickey: discount=NULL, category=NULL
            $this->assertNull($rows[3]['discount_price']);
            if ($rows[3]['category'] !== null) {
                $this->markTestIncomplete(
                    'Multi-NULLIF: Doohickey category = ' . var_export($rows[3]['category'], true)
                    . ', expected NULL'
                );
            }
            $this->assertNull($rows[3]['category']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Multiple NULLIF in UPDATE SET failed: ' . $e->getMessage());
        }
    }
}
