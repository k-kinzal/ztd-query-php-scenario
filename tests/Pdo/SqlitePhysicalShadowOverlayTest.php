<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests the CTE shadow replacement behavior: when a table has pre-existing
 * physical data, the CTE shadow REPLACES (not overlays) the physical table.
 * Physical data is NOT visible through ZTD queries — only shadow data is.
 *
 * This is a critical usability characteristic: the shadow store starts empty
 * and only data inserted/updated/deleted through ZTD is visible.
 * @spec SPEC-2.2
 */
class SqlitePhysicalShadowOverlayTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL, category TEXT)';
    }

    protected function getTableNames(): array
    {
        return ['products'];
    }

    private PDO $raw;


    protected function setUp(): void
    {
        parent::setUp();

        $this->raw = new PDO('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        $this->raw->exec('CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL, category TEXT)');
        // Pre-populate with physical data BEFORE wrapping with ZTD
        $this->raw->exec("INSERT INTO products VALUES (1, 'Widget', 29.99, 'electronics')");
        $this->raw->exec("INSERT INTO products VALUES (2, 'Gadget', 49.99, 'electronics')");
        $this->raw->exec("INSERT INTO products VALUES (3, 'Gizmo', 19.99, 'toys')");
    }

    /**
     * Physical data is NOT visible through ZTD queries — the CTE shadow
     * replaces the physical table entirely. The shadow starts empty.
     */
    public function testPhysicalDataNotVisibleThroughZtd(): void
    {
        $stmt = $this->pdo->query("SELECT COUNT(*) AS cnt FROM products");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $row['cnt']); // Shadow is empty — physical data hidden
    }

    /**
     * Only shadow-inserted data is visible.
     */
    public function testOnlyShadowInsertedDataVisible(): void
    {
        $this->pdo->exec("INSERT INTO products VALUES (10, 'Shadow Widget', 39.99, 'shadow')");

        $stmt = $this->pdo->query("SELECT COUNT(*) AS cnt FROM products");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(1, (int) $row['cnt']); // Only the shadow insert

        $stmt = $this->pdo->query("SELECT name FROM products WHERE id = 10");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Shadow Widget', $row['name']);
    }

    /**
     * UPDATE on physical row has no effect since shadow doesn't see physical data.
     * The UPDATE targets 0 rows.
     */
    public function testUpdateOnPhysicalRowMatchesNothing(): void
    {
        $affected = $this->pdo->exec("UPDATE products SET price = 999.99 WHERE id = 1");
        $this->assertSame(0, $affected); // No rows matched — physical data not in shadow
    }

    /**
     * DELETE on physical row has no effect since shadow doesn't see physical data.
     */
    public function testDeleteOnPhysicalRowMatchesNothing(): void
    {
        $affected = $this->pdo->exec("DELETE FROM products WHERE id = 1");
        $this->assertSame(0, $affected); // No rows matched
    }

    /**
     * Shadow mutations work independently of physical data.
     */
    public function testShadowMutationsWorkIndependently(): void
    {
        // Insert into shadow
        $this->pdo->exec("INSERT INTO products VALUES (10, 'Alpha', 10.00, 'test')");
        $this->pdo->exec("INSERT INTO products VALUES (20, 'Beta', 20.00, 'test')");

        // Update shadow data
        $this->pdo->exec("UPDATE products SET price = 15.00 WHERE id = 10");

        // Delete shadow data
        $this->pdo->exec("DELETE FROM products WHERE id = 20");

        // Verify shadow state
        $stmt = $this->pdo->query("SELECT * FROM products ORDER BY id");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame(10, (int) $rows[0]['id']);
        $this->assertEqualsWithDelta(15.00, (float) $rows[0]['price'], 0.01);
    }

    /**
     * Disabling ZTD reveals the physical data, which is unchanged.
     */
    public function testPhysicalDataUntouchedAfterShadowMutations(): void
    {
        $this->pdo->exec("INSERT INTO products VALUES (10, 'Shadow Only', 99.99, 'shadow')");

        // Disable ZTD — physical data visible
        $this->pdo->disableZtd();

        $stmt = $this->pdo->query("SELECT COUNT(*) AS cnt FROM products");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(3, (int) $row['cnt']); // Original 3 physical rows

        // Shadow row not in physical
        $stmt = $this->pdo->query("SELECT * FROM products WHERE id = 10");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertFalse($row);

        // Physical rows unchanged
        $stmt = $this->pdo->query("SELECT name FROM products WHERE id = 1");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Widget', $row['name']);
    }

    /**
     * Re-enabling ZTD after disable preserves shadow state.
     * Shadow data persists across toggle cycles.
     */
    public function testReEnablingZtdPreservesShadow(): void
    {
        $this->pdo->exec("INSERT INTO products VALUES (10, 'Before Toggle', 10.00, 'test')");

        $this->pdo->disableZtd();
        $this->pdo->enableZtd();

        // Shadow data persists across toggle
        $stmt = $this->pdo->query("SELECT COUNT(*) AS cnt FROM products");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(1, (int) $row['cnt']);

        $stmt = $this->pdo->query("SELECT name FROM products WHERE id = 10");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Before Toggle', $row['name']);
    }

    /**
     * Aggregates on empty shadow return correct empty-set values.
     */
    public function testAggregateOnEmptyShadow(): void
    {
        $stmt = $this->pdo->query("SELECT COUNT(*) AS cnt, SUM(price) AS total, AVG(price) AS avg_price FROM products");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $row['cnt']);
        $this->assertNull($row['total']);
        $this->assertNull($row['avg_price']);
    }

    /**
     * INSERT with IDs overlapping physical IDs works — shadow doesn't check physical PK.
     */
    public function testInsertWithOverlappingPhysicalId(): void
    {
        // Physical has id=1, shadow can also insert id=1
        $this->pdo->exec("INSERT INTO products VALUES (1, 'Shadow Widget', 99.99, 'shadow')");

        $stmt = $this->pdo->query("SELECT name FROM products WHERE id = 1");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Shadow Widget', $row['name']); // Shadow version
    }

    /**
     * Complex queries on shadow-only data work correctly regardless of physical data.
     */
    public function testComplexQueryOnShadowOnly(): void
    {
        $this->pdo->exec("INSERT INTO products VALUES (1, 'Alpha', 10.00, 'A')");
        $this->pdo->exec("INSERT INTO products VALUES (2, 'Beta', 20.00, 'A')");
        $this->pdo->exec("INSERT INTO products VALUES (3, 'Gamma', 30.00, 'B')");

        $stmt = $this->pdo->query("
            SELECT category, COUNT(*) AS cnt, SUM(price) AS total
            FROM products
            GROUP BY category
            HAVING COUNT(*) > 1
            ORDER BY category
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('A', $rows[0]['category']);
        $this->assertSame(2, (int) $rows[0]['cnt']);
        $this->assertEqualsWithDelta(30.00, (float) $rows[0]['total'], 0.01);
    }
}
