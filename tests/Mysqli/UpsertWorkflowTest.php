<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests realistic upsert workflows using MySQL's INSERT ... ON DUPLICATE KEY UPDATE
 * and INSERT IGNORE. Also covers check-then-insert and read-modify-write patterns.
 * @spec SPEC-4.2a
 */
class UpsertWorkflowTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_uw_settings (
                `key` VARCHAR(100) PRIMARY KEY,
                value VARCHAR(255) NOT NULL,
                updated_at DATE NOT NULL
            )',
            'CREATE TABLE mi_uw_counters (
                name VARCHAR(100) PRIMARY KEY,
                count INT NOT NULL DEFAULT 0,
                last_incremented DATE
            )',
            'CREATE TABLE mi_uw_products (
                sku VARCHAR(50) PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                price DECIMAL(10,2) NOT NULL,
                stock INT NOT NULL DEFAULT 0,
                category VARCHAR(50) NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_uw_settings', 'mi_uw_counters', 'mi_uw_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Settings (key-value store)
        $this->mysqli->query("INSERT INTO mi_uw_settings VALUES ('theme', 'dark', '2024-01-01')");
        $this->mysqli->query("INSERT INTO mi_uw_settings VALUES ('language', 'en', '2024-01-01')");
        $this->mysqli->query("INSERT INTO mi_uw_settings VALUES ('timezone', 'UTC', '2024-01-01')");
        $this->mysqli->query("INSERT INTO mi_uw_settings VALUES ('notifications', 'on', '2024-01-01')");

        // Counters
        $this->mysqli->query("INSERT INTO mi_uw_counters VALUES ('page_views', 100, '2024-01-15')");
        $this->mysqli->query("INSERT INTO mi_uw_counters VALUES ('api_calls', 5000, '2024-01-15')");
        $this->mysqli->query("INSERT INTO mi_uw_counters VALUES ('errors', 3, '2024-01-15')");

        // Products
        $this->mysqli->query("INSERT INTO mi_uw_products VALUES ('SKU001', 'Widget A', 9.99, 50, 'hardware')");
        $this->mysqli->query("INSERT INTO mi_uw_products VALUES ('SKU002', 'Widget B', 14.99, 30, 'hardware')");
        $this->mysqli->query("INSERT INTO mi_uw_products VALUES ('SKU003', 'Gadget X', 29.99, 10, 'electronics')");
        $this->mysqli->query("INSERT INTO mi_uw_products VALUES ('SKU004', 'Gadget Y', 49.99, 5, 'electronics')");
        $this->mysqli->query("INSERT INTO mi_uw_products VALUES ('SKU005', 'Tool Z', 7.99, 100, 'hardware')");
    }

    /**
     * INSERT ... ON DUPLICATE KEY UPDATE for idempotent writes — updating existing setting.
     */
    public function testOnDuplicateKeyUpdateUpdatesExisting(): void
    {
        $this->mysqli->query(
            "INSERT INTO mi_uw_settings (`key`, value, updated_at) VALUES ('theme', 'light', '2024-02-01')
             ON DUPLICATE KEY UPDATE value = VALUES(value), updated_at = VALUES(updated_at)"
        );

        $rows = $this->ztdQuery("SELECT value, updated_at FROM mi_uw_settings WHERE `key` = 'theme'");
        $this->assertCount(1, $rows);
        $this->assertSame('light', $rows[0]['value']);
        $this->assertSame('2024-02-01', $rows[0]['updated_at']);

        // Total count unchanged
        $count = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_uw_settings");
        $this->assertSame(4, (int) $count[0]['cnt']);
    }

    /**
     * INSERT ... ON DUPLICATE KEY UPDATE for idempotent writes — inserting new setting.
     */
    public function testOnDuplicateKeyUpdateInsertsNew(): void
    {
        $this->mysqli->query(
            "INSERT INTO mi_uw_settings (`key`, value, updated_at) VALUES ('font_size', '14px', '2024-02-01')
             ON DUPLICATE KEY UPDATE value = VALUES(value), updated_at = VALUES(updated_at)"
        );

        $rows = $this->ztdQuery("SELECT value FROM mi_uw_settings WHERE `key` = 'font_size'");
        $this->assertCount(1, $rows);
        $this->assertSame('14px', $rows[0]['value']);

        // Total count increased
        $count = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_uw_settings");
        $this->assertSame(5, (int) $count[0]['cnt']);
    }

    /**
     * INSERT IGNORE for skip-if-exists — key already exists.
     */
    public function testInsertIgnoreSkipsExisting(): void
    {
        $this->mysqli->query("INSERT IGNORE INTO mi_uw_settings VALUES ('theme', 'green', '2024-03-01')");

        // Value should still be the original
        $rows = $this->ztdQuery("SELECT value FROM mi_uw_settings WHERE `key` = 'theme'");
        $this->assertSame('dark', $rows[0]['value']);
    }

    /**
     * INSERT IGNORE for skip-if-exists — key does not exist.
     */
    public function testInsertIgnoreInsertsNew(): void
    {
        $this->mysqli->query("INSERT IGNORE INTO mi_uw_settings VALUES ('sidebar', 'collapsed', '2024-03-01')");

        $rows = $this->ztdQuery("SELECT value FROM mi_uw_settings WHERE `key` = 'sidebar'");
        $this->assertCount(1, $rows);
        $this->assertSame('collapsed', $rows[0]['value']);
    }

    /**
     * Check-then-insert pattern: SELECT to check existence, then conditional INSERT.
     */
    public function testCheckThenInsertPattern(): void
    {
        // Check if product SKU exists
        $existing = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_uw_products WHERE sku = 'SKU006'");
        $this->assertSame(0, (int) $existing[0]['cnt']);

        // Product doesn't exist, so insert
        $this->mysqli->query("INSERT INTO mi_uw_products VALUES ('SKU006', 'New Item', 19.99, 25, 'accessories')");

        // Verify it was inserted
        $rows = $this->ztdQuery("SELECT name, price FROM mi_uw_products WHERE sku = 'SKU006'");
        $this->assertCount(1, $rows);
        $this->assertSame('New Item', $rows[0]['name']);

        // Check existing product
        $existing2 = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_uw_products WHERE sku = 'SKU001'");
        $this->assertSame(1, (int) $existing2[0]['cnt']);
        // Don't insert since it already exists
    }

    /**
     * Upsert counter pattern: read current value, then upsert with incremented value.
     */
    public function testUpsertCounterPattern(): void
    {
        // Read current counter value
        $current = $this->ztdQuery("SELECT count FROM mi_uw_counters WHERE name = 'page_views'");
        $currentCount = (int) $current[0]['count'];
        $this->assertSame(100, $currentCount);

        // Increment and upsert
        $newCount = $currentCount + 1;
        $this->mysqli->query(
            "INSERT INTO mi_uw_counters VALUES ('page_views', $newCount, '2024-02-01')
             ON DUPLICATE KEY UPDATE count = VALUES(count), last_incremented = VALUES(last_incremented)"
        );

        // Verify incremented value
        $rows = $this->ztdQuery("SELECT count FROM mi_uw_counters WHERE name = 'page_views'");
        $this->assertSame(101, (int) $rows[0]['count']);
    }

    /**
     * Batch upsert: multiple INSERT ... ON DUPLICATE KEY UPDATE in sequence.
     */
    public function testBatchUpsert(): void
    {
        // Update existing products and add new ones
        $this->mysqli->query(
            "INSERT INTO mi_uw_products VALUES ('SKU001', 'Widget A v2', 11.99, 45, 'hardware')
             ON DUPLICATE KEY UPDATE name = VALUES(name), price = VALUES(price), stock = VALUES(stock), category = VALUES(category)"
        );
        $this->mysqli->query(
            "INSERT INTO mi_uw_products VALUES ('SKU002', 'Widget B v2', 16.99, 25, 'hardware')
             ON DUPLICATE KEY UPDATE name = VALUES(name), price = VALUES(price), stock = VALUES(stock), category = VALUES(category)"
        );
        $this->mysqli->query(
            "INSERT INTO mi_uw_products VALUES ('SKU006', 'Gizmo', 39.99, 15, 'electronics')
             ON DUPLICATE KEY UPDATE name = VALUES(name), price = VALUES(price), stock = VALUES(stock), category = VALUES(category)"
        );
        $this->mysqli->query(
            "INSERT INTO mi_uw_products VALUES ('SKU007', 'Sensor', 24.99, 40, 'electronics')
             ON DUPLICATE KEY UPDATE name = VALUES(name), price = VALUES(price), stock = VALUES(stock), category = VALUES(category)"
        );

        // Verify updated rows
        $widget = $this->ztdQuery("SELECT name, price FROM mi_uw_products WHERE sku = 'SKU001'");
        $this->assertSame('Widget A v2', $widget[0]['name']);
        $this->assertEqualsWithDelta(11.99, (float) $widget[0]['price'], 0.01);

        // Verify new rows
        $gizmo = $this->ztdQuery("SELECT name FROM mi_uw_products WHERE sku = 'SKU006'");
        $this->assertSame('Gizmo', $gizmo[0]['name']);

        // Total count: 5 original + 2 new = 7
        $count = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_uw_products");
        $this->assertSame(7, (int) $count[0]['cnt']);
    }

    /**
     * Verify old data is replaced, not accumulated (multiple upserts on same key).
     */
    public function testOldDataReplacedNotAccumulated(): void
    {
        // Upsert same key multiple times
        $this->mysqli->query(
            "INSERT INTO mi_uw_settings (`key`, value, updated_at) VALUES ('theme', 'blue', '2024-02-01')
             ON DUPLICATE KEY UPDATE value = VALUES(value), updated_at = VALUES(updated_at)"
        );
        $this->mysqli->query(
            "INSERT INTO mi_uw_settings (`key`, value, updated_at) VALUES ('theme', 'green', '2024-03-01')
             ON DUPLICATE KEY UPDATE value = VALUES(value), updated_at = VALUES(updated_at)"
        );
        $this->mysqli->query(
            "INSERT INTO mi_uw_settings (`key`, value, updated_at) VALUES ('theme', 'red', '2024-04-01')
             ON DUPLICATE KEY UPDATE value = VALUES(value), updated_at = VALUES(updated_at)"
        );

        // Only the last value should remain
        $rows = $this->ztdQuery("SELECT value, updated_at FROM mi_uw_settings WHERE `key` = 'theme'");
        $this->assertCount(1, $rows);
        $this->assertSame('red', $rows[0]['value']);
        $this->assertSame('2024-04-01', $rows[0]['updated_at']);

        // Total count unchanged
        $count = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_uw_settings");
        $this->assertSame(4, (int) $count[0]['cnt']);
    }

    /**
     * Read-modify-write pattern: read current state, update only desired columns.
     * Unlike SQLite's INSERT OR REPLACE (which replaces the entire row), MySQL's
     * ON DUPLICATE KEY UPDATE can target specific columns.
     */
    public function testReadModifyWritePattern(): void
    {
        // Read the current product state
        $product = $this->ztdQuery("SELECT * FROM mi_uw_products WHERE sku = 'SKU003'");
        $this->assertCount(1, $product);
        $this->assertSame('Gadget X', $product[0]['name']);
        $this->assertSame('electronics', $product[0]['category']);

        // Only update price and stock, preserving name and category
        $this->mysqli->query(
            "INSERT INTO mi_uw_products VALUES ('SKU003', 'Gadget X', 34.99, 15, 'electronics')
             ON DUPLICATE KEY UPDATE price = VALUES(price), stock = VALUES(stock)"
        );

        // Verify price and stock updated
        $updated = $this->ztdQuery("SELECT * FROM mi_uw_products WHERE sku = 'SKU003'");
        $this->assertSame('Gadget X', $updated[0]['name']); // preserved
        $this->assertEqualsWithDelta(34.99, (float) $updated[0]['price'], 0.01); // updated
        $this->assertSame(15, (int) $updated[0]['stock']); // updated
        $this->assertSame('electronics', $updated[0]['category']); // preserved
    }

    /**
     * Prepared INSERT ... ON DUPLICATE KEY UPDATE with bind_param.
     */
    public function testPreparedUpsert(): void
    {
        $stmt = $this->mysqli->prepare(
            "INSERT INTO mi_uw_settings (`key`, value, updated_at) VALUES (?, ?, ?)
             ON DUPLICATE KEY UPDATE value = VALUES(value), updated_at = VALUES(updated_at)"
        );

        // Upsert existing key
        $key = 'language';
        $value = 'fr';
        $date = '2024-06-01';
        $stmt->bind_param('sss', $key, $value, $date);
        $stmt->execute();

        $rows = $this->ztdQuery("SELECT value FROM mi_uw_settings WHERE `key` = 'language'");
        $this->assertSame('fr', $rows[0]['value']);

        // Upsert new key
        $key = 'currency';
        $value = 'EUR';
        $date = '2024-06-01';
        $stmt->bind_param('sss', $key, $value, $date);
        $stmt->execute();

        $rows = $this->ztdQuery("SELECT value FROM mi_uw_settings WHERE `key` = 'currency'");
        $this->assertCount(1, $rows);
        $this->assertSame('EUR', $rows[0]['value']);

        // Total count: 4 original + 1 new = 5
        $count = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_uw_settings");
        $this->assertSame(5, (int) $count[0]['cnt']);
    }

    /**
     * Physical isolation — shadow data does not reach physical table.
     */
    public function testPhysicalIsolation(): void
    {
        // Do an upsert
        $this->mysqli->query(
            "INSERT INTO mi_uw_settings (`key`, value, updated_at) VALUES ('theme', 'neon', '2024-05-01')
             ON DUPLICATE KEY UPDATE value = VALUES(value), updated_at = VALUES(updated_at)"
        );

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_uw_settings");
        $this->assertSame(4, (int) $rows[0]['cnt']);

        $this->mysqli->disableZtd();
        $result = $this->mysqli->query("SELECT COUNT(*) AS cnt FROM mi_uw_settings");
        $this->assertSame(0, (int) $result->fetch_assoc()['cnt']);
    }
}
