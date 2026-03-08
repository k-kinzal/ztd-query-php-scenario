<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests realistic upsert-like workflows using SQLite's INSERT OR REPLACE and
 * INSERT OR IGNORE (since ON CONFLICT has known issues under ZTD — see SPEC-11.SQLITE-ON-CONFLICT).
 * Also covers check-then-insert and read-modify-write patterns.
 * @spec SPEC-4.2a
 */
class SqliteUpsertWorkflowTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_uw_settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )',
            'CREATE TABLE sl_uw_counters (
                name TEXT PRIMARY KEY,
                count INTEGER NOT NULL DEFAULT 0,
                last_incremented TEXT
            )',
            'CREATE TABLE sl_uw_products (
                sku TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                price REAL NOT NULL,
                stock INTEGER NOT NULL DEFAULT 0,
                category TEXT NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_uw_settings', 'sl_uw_counters', 'sl_uw_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Settings (key-value store)
        $this->pdo->exec("INSERT INTO sl_uw_settings VALUES ('theme', 'dark', '2024-01-01')");
        $this->pdo->exec("INSERT INTO sl_uw_settings VALUES ('language', 'en', '2024-01-01')");
        $this->pdo->exec("INSERT INTO sl_uw_settings VALUES ('timezone', 'UTC', '2024-01-01')");
        $this->pdo->exec("INSERT INTO sl_uw_settings VALUES ('notifications', 'on', '2024-01-01')");

        // Counters
        $this->pdo->exec("INSERT INTO sl_uw_counters VALUES ('page_views', 100, '2024-01-15')");
        $this->pdo->exec("INSERT INTO sl_uw_counters VALUES ('api_calls', 5000, '2024-01-15')");
        $this->pdo->exec("INSERT INTO sl_uw_counters VALUES ('errors', 3, '2024-01-15')");

        // Products
        $this->pdo->exec("INSERT INTO sl_uw_products VALUES ('SKU001', 'Widget A', 9.99, 50, 'hardware')");
        $this->pdo->exec("INSERT INTO sl_uw_products VALUES ('SKU002', 'Widget B', 14.99, 30, 'hardware')");
        $this->pdo->exec("INSERT INTO sl_uw_products VALUES ('SKU003', 'Gadget X', 29.99, 10, 'electronics')");
        $this->pdo->exec("INSERT INTO sl_uw_products VALUES ('SKU004', 'Gadget Y', 49.99, 5, 'electronics')");
        $this->pdo->exec("INSERT INTO sl_uw_products VALUES ('SKU005', 'Tool Z', 7.99, 100, 'hardware')");
    }

    /**
     * INSERT OR REPLACE for idempotent writes — updating existing setting.
     */
    public function testInsertOrReplaceUpdatesExisting(): void
    {
        $this->pdo->exec("INSERT OR REPLACE INTO sl_uw_settings VALUES ('theme', 'light', '2024-02-01')");

        $rows = $this->ztdQuery("SELECT value, updated_at FROM sl_uw_settings WHERE key = 'theme'");
        $this->assertCount(1, $rows);
        $this->assertSame('light', $rows[0]['value']);
        $this->assertSame('2024-02-01', $rows[0]['updated_at']);

        // Total count unchanged
        $count = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_uw_settings");
        $this->assertSame(4, (int) $count[0]['cnt']);
    }

    /**
     * INSERT OR REPLACE for idempotent writes — inserting new setting.
     */
    public function testInsertOrReplaceInsertsNew(): void
    {
        $this->pdo->exec("INSERT OR REPLACE INTO sl_uw_settings VALUES ('font_size', '14px', '2024-02-01')");

        $rows = $this->ztdQuery("SELECT value FROM sl_uw_settings WHERE key = 'font_size'");
        $this->assertCount(1, $rows);
        $this->assertSame('14px', $rows[0]['value']);

        // Total count increased
        $count = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_uw_settings");
        $this->assertSame(5, (int) $count[0]['cnt']);
    }

    /**
     * INSERT OR IGNORE for skip-if-exists — key already exists.
     */
    public function testInsertOrIgnoreSkipsExisting(): void
    {
        $this->pdo->exec("INSERT OR IGNORE INTO sl_uw_settings VALUES ('theme', 'green', '2024-03-01')");

        // Value should still be the original
        $rows = $this->ztdQuery("SELECT value FROM sl_uw_settings WHERE key = 'theme'");
        $this->assertSame('dark', $rows[0]['value']);
    }

    /**
     * INSERT OR IGNORE for skip-if-exists — key does not exist.
     */
    public function testInsertOrIgnoreInsertsNew(): void
    {
        $this->pdo->exec("INSERT OR IGNORE INTO sl_uw_settings VALUES ('sidebar', 'collapsed', '2024-03-01')");

        $rows = $this->ztdQuery("SELECT value FROM sl_uw_settings WHERE key = 'sidebar'");
        $this->assertCount(1, $rows);
        $this->assertSame('collapsed', $rows[0]['value']);
    }

    /**
     * Check-then-insert pattern: SELECT to check existence, then conditional INSERT.
     */
    public function testCheckThenInsertPattern(): void
    {
        // Check if product SKU exists
        $existing = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_uw_products WHERE sku = 'SKU006'");
        $this->assertSame(0, (int) $existing[0]['cnt']);

        // Product doesn't exist, so insert
        $this->pdo->exec("INSERT INTO sl_uw_products VALUES ('SKU006', 'New Item', 19.99, 25, 'accessories')");

        // Verify it was inserted
        $rows = $this->ztdQuery("SELECT name, price FROM sl_uw_products WHERE sku = 'SKU006'");
        $this->assertCount(1, $rows);
        $this->assertSame('New Item', $rows[0]['name']);

        // Check existing product
        $existing2 = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_uw_products WHERE sku = 'SKU001'");
        $this->assertSame(1, (int) $existing2[0]['cnt']);
        // Don't insert since it already exists
    }

    /**
     * Upsert counter pattern: read current value, then INSERT OR REPLACE with incremented value.
     */
    public function testUpsertCounterPattern(): void
    {
        // Read current counter value
        $current = $this->ztdQuery("SELECT count FROM sl_uw_counters WHERE name = 'page_views'");
        $currentCount = (int) $current[0]['count'];
        $this->assertSame(100, $currentCount);

        // Increment and upsert
        $newCount = $currentCount + 1;
        $this->pdo->exec("INSERT OR REPLACE INTO sl_uw_counters VALUES ('page_views', $newCount, '2024-02-01')");

        // Verify incremented value
        $rows = $this->ztdQuery("SELECT count FROM sl_uw_counters WHERE name = 'page_views'");
        $this->assertSame(101, (int) $rows[0]['count']);
    }

    /**
     * Batch upsert: multiple INSERT OR REPLACE in sequence.
     */
    public function testBatchUpsert(): void
    {
        // Update existing products and add new ones
        $this->pdo->exec("INSERT OR REPLACE INTO sl_uw_products VALUES ('SKU001', 'Widget A v2', 11.99, 45, 'hardware')");
        $this->pdo->exec("INSERT OR REPLACE INTO sl_uw_products VALUES ('SKU002', 'Widget B v2', 16.99, 25, 'hardware')");
        $this->pdo->exec("INSERT OR REPLACE INTO sl_uw_products VALUES ('SKU006', 'Gizmo', 39.99, 15, 'electronics')");
        $this->pdo->exec("INSERT OR REPLACE INTO sl_uw_products VALUES ('SKU007', 'Sensor', 24.99, 40, 'electronics')");

        // Verify updated rows
        $widget = $this->ztdQuery("SELECT name, price FROM sl_uw_products WHERE sku = 'SKU001'");
        $this->assertSame('Widget A v2', $widget[0]['name']);
        $this->assertEqualsWithDelta(11.99, (float) $widget[0]['price'], 0.01);

        // Verify new rows
        $gizmo = $this->ztdQuery("SELECT name FROM sl_uw_products WHERE sku = 'SKU006'");
        $this->assertSame('Gizmo', $gizmo[0]['name']);

        // Total count: 5 original + 2 new = 7
        $count = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_uw_products");
        $this->assertSame(7, (int) $count[0]['cnt']);
    }

    /**
     * Verify old data is replaced, not accumulated.
     */
    public function testOldDataReplacedNotAccumulated(): void
    {
        // Replace same key multiple times
        $this->pdo->exec("INSERT OR REPLACE INTO sl_uw_settings VALUES ('theme', 'blue', '2024-02-01')");
        $this->pdo->exec("INSERT OR REPLACE INTO sl_uw_settings VALUES ('theme', 'green', '2024-03-01')");
        $this->pdo->exec("INSERT OR REPLACE INTO sl_uw_settings VALUES ('theme', 'red', '2024-04-01')");

        // Only the last value should remain
        $rows = $this->ztdQuery("SELECT value, updated_at FROM sl_uw_settings WHERE key = 'theme'");
        $this->assertCount(1, $rows);
        $this->assertSame('red', $rows[0]['value']);
        $this->assertSame('2024-04-01', $rows[0]['updated_at']);

        // Total count unchanged
        $count = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_uw_settings");
        $this->assertSame(4, (int) $count[0]['cnt']);
    }

    /**
     * INSERT OR REPLACE preserving non-updated columns using read-modify-write pattern.
     * Since REPLACE replaces the entire row, we must read first, then write back
     * all columns including the ones we don't want to change.
     */
    public function testReadModifyWritePattern(): void
    {
        // Read the current product state
        $product = $this->ztdQuery("SELECT * FROM sl_uw_products WHERE sku = 'SKU003'");
        $this->assertCount(1, $product);
        $originalName = $product[0]['name'];
        $originalCategory = $product[0]['category'];

        // Only update price and stock, preserving name and category
        $newPrice = 34.99;
        $newStock = 15;
        $this->pdo->exec(
            "INSERT OR REPLACE INTO sl_uw_products VALUES ('SKU003', '$originalName', $newPrice, $newStock, '$originalCategory')"
        );

        // Verify price and stock updated
        $updated = $this->ztdQuery("SELECT * FROM sl_uw_products WHERE sku = 'SKU003'");
        $this->assertSame('Gadget X', $updated[0]['name']); // preserved
        $this->assertEqualsWithDelta(34.99, (float) $updated[0]['price'], 0.01); // updated
        $this->assertSame(15, (int) $updated[0]['stock']); // updated
        $this->assertSame('electronics', $updated[0]['category']); // preserved
    }

    /**
     * Physical isolation — shadow data does not reach physical table.
     */
    public function testPhysicalIsolation(): void
    {
        // Do an upsert
        $this->pdo->exec("INSERT OR REPLACE INTO sl_uw_settings VALUES ('theme', 'neon', '2024-05-01')");

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_uw_settings");
        $this->assertSame(4, (int) $rows[0]['cnt']);

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query("SELECT COUNT(*) FROM sl_uw_settings");
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
