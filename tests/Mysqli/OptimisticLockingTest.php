<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Optimistic locking: UPDATE WHERE version = ? then check affected rows.
 * Common in web apps to prevent lost updates from concurrent requests.
 * @spec SPEC-4.2, SPEC-4.4
 */
class OptimisticLockingTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_ol_products (id INT PRIMARY KEY, name VARCHAR(255), price DECIMAL(10,2), version INT NOT NULL DEFAULT 1)';
    }

    protected function getTableNames(): array
    {
        return ['mi_ol_products'];
    }

    private function seed(): void
    {
        $this->mysqli->query("INSERT INTO mi_ol_products (id, name, price, version) VALUES (1, 'Widget', 9.99, 1)");
        $this->mysqli->query("INSERT INTO mi_ol_products (id, name, price, version) VALUES (2, 'Gadget', 24.99, 1)");
    }

    public function testUpdateWithCorrectVersion(): void
    {
        $this->seed();

        $this->mysqli->query("UPDATE mi_ol_products SET price = 12.99, version = version + 1 WHERE id = 1 AND version = 1");
        $affected = $this->mysqli->lastAffectedRows();
        $this->assertSame(1, $affected);

        $rows = $this->ztdQuery('SELECT price, version FROM mi_ol_products WHERE id = 1');
        $this->assertSame(12.99, (float) $rows[0]['price']);
        $this->assertSame(2, (int) $rows[0]['version']);
    }

    public function testUpdateWithStaleVersionAffectsZeroRows(): void
    {
        $this->seed();

        // First update succeeds
        $this->mysqli->query("UPDATE mi_ol_products SET price = 12.99, version = version + 1 WHERE id = 1 AND version = 1");
        $this->assertSame(1, $this->mysqli->lastAffectedRows());

        // Second update with stale version = 1 should affect 0 rows
        $this->mysqli->query("UPDATE mi_ol_products SET price = 15.99, version = version + 1 WHERE id = 1 AND version = 1");
        $this->assertSame(0, $this->mysqli->lastAffectedRows());

        // Price should remain 12.99 from the first update
        $rows = $this->ztdQuery('SELECT price, version FROM mi_ol_products WHERE id = 1');
        $this->assertSame(12.99, (float) $rows[0]['price']);
        $this->assertSame(2, (int) $rows[0]['version']);
    }

    public function testReadVersionThenConditionalUpdate(): void
    {
        $this->seed();

        // Simulate: read current version, then update only if version matches
        $rows = $this->ztdQuery('SELECT id, price, version FROM mi_ol_products WHERE id = 1');
        $currentVersion = (int) $rows[0]['version'];
        $this->assertSame(1, $currentVersion);

        $this->mysqli->query("UPDATE mi_ol_products SET price = 19.99, version = version + 1 WHERE id = 1 AND version = {$currentVersion}");
        $this->assertSame(1, $this->mysqli->lastAffectedRows());

        $rows = $this->ztdQuery('SELECT version FROM mi_ol_products WHERE id = 1');
        $this->assertSame(2, (int) $rows[0]['version']);
    }

    public function testPreparedOptimisticUpdate(): void
    {
        $this->seed();

        // First prepared UPDATE succeeds
        $stmt = $this->mysqli->prepare("UPDATE mi_ol_products SET price = ?, version = version + 1 WHERE id = ? AND version = ?");
        $newPrice = 14.99;
        $id = 1;
        $version = 1;
        $stmt->bind_param('dii', $newPrice, $id, $version);
        $stmt->execute();
        $this->assertSame(1, $stmt->ztdAffectedRows());

        // Re-prepare to get fresh snapshot (SPEC-3.2: snapshot is frozen at prepare time)
        $stmt2 = $this->mysqli->prepare("UPDATE mi_ol_products SET price = ?, version = version + 1 WHERE id = ? AND version = ?");
        $newPrice2 = 19.99;
        $stmt2->bind_param('dii', $newPrice2, $id, $version);
        $stmt2->execute();
        $this->assertSame(0, $stmt2->ztdAffectedRows());
    }

    public function testMultipleSequentialVersionBumps(): void
    {
        $this->seed();

        for ($v = 1; $v <= 5; $v++) {
            $newPrice = 9.99 + $v;
            $this->mysqli->query("UPDATE mi_ol_products SET price = {$newPrice}, version = version + 1 WHERE id = 1 AND version = {$v}");
            $this->assertSame(1, $this->mysqli->lastAffectedRows());
        }

        $rows = $this->ztdQuery('SELECT price, version FROM mi_ol_products WHERE id = 1');
        $this->assertSame(14.99, (float) $rows[0]['price']);
        $this->assertSame(6, (int) $rows[0]['version']);
    }

    public function testOtherRowsUnaffectedByVersionedUpdate(): void
    {
        $this->seed();

        $this->mysqli->query("UPDATE mi_ol_products SET price = 99.99, version = version + 1 WHERE id = 1 AND version = 1");

        // Row 2 should be untouched
        $rows = $this->ztdQuery('SELECT price, version FROM mi_ol_products WHERE id = 2');
        $this->assertSame(24.99, (float) $rows[0]['price']);
        $this->assertSame(1, (int) $rows[0]['version']);
    }
}
