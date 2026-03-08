<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Optimistic locking: UPDATE WHERE version = ? then check affected rows.
 * @spec SPEC-4.2, SPEC-4.4
 */
class SqliteOptimisticLockingTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE ol_products (id INTEGER PRIMARY KEY, name TEXT, price REAL, version INTEGER NOT NULL DEFAULT 1)';
    }

    protected function getTableNames(): array
    {
        return ['ol_products'];
    }

    private function seed(): void
    {
        $this->pdo->exec("INSERT INTO ol_products (id, name, price, version) VALUES (1, 'Widget', 9.99, 1)");
        $this->pdo->exec("INSERT INTO ol_products (id, name, price, version) VALUES (2, 'Gadget', 24.99, 1)");
    }

    public function testUpdateWithCorrectVersion(): void
    {
        $this->seed();

        $affected = $this->pdo->exec("UPDATE ol_products SET price = 12.99, version = version + 1 WHERE id = 1 AND version = 1");
        $this->assertSame(1, $affected);

        $rows = $this->ztdQuery('SELECT price, version FROM ol_products WHERE id = 1');
        $this->assertSame(12.99, (float) $rows[0]['price']);
        $this->assertSame(2, (int) $rows[0]['version']);
    }

    public function testUpdateWithStaleVersionAffectsZeroRows(): void
    {
        $this->seed();

        $this->pdo->exec("UPDATE ol_products SET price = 12.99, version = version + 1 WHERE id = 1 AND version = 1");

        $affected = $this->pdo->exec("UPDATE ol_products SET price = 15.99, version = version + 1 WHERE id = 1 AND version = 1");
        $this->assertSame(0, $affected);

        $rows = $this->ztdQuery('SELECT price, version FROM ol_products WHERE id = 1');
        $this->assertSame(12.99, (float) $rows[0]['price']);
        $this->assertSame(2, (int) $rows[0]['version']);
    }

    public function testPreparedOptimisticUpdate(): void
    {
        $this->seed();

        // First prepared UPDATE succeeds
        $stmt = $this->pdo->prepare("UPDATE ol_products SET price = ?, version = version + 1 WHERE id = ? AND version = ?");
        $stmt->execute([14.99, 1, 1]);
        $this->assertSame(1, $stmt->rowCount());

        // Re-prepare to get fresh snapshot (SPEC-3.2: snapshot is frozen at prepare time)
        $stmt2 = $this->pdo->prepare("UPDATE ol_products SET price = ?, version = version + 1 WHERE id = ? AND version = ?");
        $stmt2->execute([19.99, 1, 1]);
        $this->assertSame(0, $stmt2->rowCount());

        $rows = $this->ztdQuery('SELECT price FROM ol_products WHERE id = 1');
        $this->assertSame(14.99, (float) $rows[0]['price']);
    }

    public function testMultipleSequentialVersionBumps(): void
    {
        $this->seed();

        for ($v = 1; $v <= 5; $v++) {
            $newPrice = 9.99 + $v;
            $affected = $this->pdo->exec("UPDATE ol_products SET price = {$newPrice}, version = version + 1 WHERE id = 1 AND version = {$v}");
            $this->assertSame(1, $affected);
        }

        $rows = $this->ztdQuery('SELECT price, version FROM ol_products WHERE id = 1');
        $this->assertSame(14.99, (float) $rows[0]['price']);
        $this->assertSame(6, (int) $rows[0]['version']);
    }

    public function testReadThenConditionalUpdate(): void
    {
        $this->seed();

        $rows = $this->ztdQuery('SELECT version FROM ol_products WHERE id = 1');
        $ver = (int) $rows[0]['version'];

        $affected = $this->pdo->exec("UPDATE ol_products SET price = 19.99, version = version + 1 WHERE id = 1 AND version = {$ver}");
        $this->assertSame(1, $affected);
    }
}
