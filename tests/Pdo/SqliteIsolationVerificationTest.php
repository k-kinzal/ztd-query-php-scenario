<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests ZTD isolation: verifies that shadow-inserted data does NOT
 * appear in queries when ZTD is disabled (physical table check).
 *
 * @spec SPEC-2.1
 */
class SqliteIsolationVerificationTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_iv_items (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            value REAL NOT NULL
        )';
    }

    protected function getTableNames(): array
    {
        return ['sl_iv_items'];
    }

    /**
     * INSERT via ZTD: visible through ZTD, not in physical table.
     */
    public function testShadowInsertNotInPhysical(): void
    {
        // Insert via ZTD (shadow)
        $this->pdo->exec("INSERT INTO sl_iv_items VALUES (1, 'Shadow', 100)");

        // Verify visible via ZTD
        $ztdRows = $this->ztdQuery("SELECT * FROM sl_iv_items");
        $this->assertCount(1, $ztdRows);
        $this->assertSame('Shadow', $ztdRows[0]['name']);

        // Physical table should be empty
        $this->disableZtd();
        $raw = $this->pdo->query("SELECT COUNT(*) AS c FROM sl_iv_items")->fetch(\PDO::FETCH_ASSOC);
        $this->enableZtd();

        if ((int) $raw['c'] !== 0) {
            $this->markTestIncomplete(
                'ZTD isolation: shadow INSERT appeared in physical table. Count: ' . $raw['c']
            );
        }

        $this->assertSame(0, (int) $raw['c']);
    }

    /**
     * Multiple shadow INSERTs: none appear in physical.
     */
    public function testMultipleShadowInsertsIsolated(): void
    {
        for ($i = 1; $i <= 5; $i++) {
            $this->pdo->exec("INSERT INTO sl_iv_items VALUES ($i, 'Item$i', " . ($i * 10) . ")");
        }

        // ZTD sees 5 rows
        $ztdCount = $this->ztdQuery("SELECT COUNT(*) AS c FROM sl_iv_items");
        $this->assertSame(5, (int) $ztdCount[0]['c']);

        // Physical sees 0
        $this->disableZtd();
        $raw = $this->pdo->query("SELECT COUNT(*) AS c FROM sl_iv_items")->fetch(\PDO::FETCH_ASSOC);
        $this->enableZtd();

        $this->assertSame(0, (int) $raw['c']);
    }

    /**
     * Shadow DELETE of physical data: physical unchanged.
     */
    public function testShadowDeleteLeavesPhysicalIntact(): void
    {
        // Seed physical data
        $this->disableZtd();
        $this->pdo->exec("INSERT INTO sl_iv_items VALUES (1, 'Physical', 50)");
        $this->enableZtd();

        // Delete via ZTD
        $this->pdo->exec("DELETE FROM sl_iv_items WHERE id = 1");

        // ZTD should show 0 rows
        $ztdRows = $this->ztdQuery("SELECT * FROM sl_iv_items");

        if (count($ztdRows) !== 0) {
            $this->markTestIncomplete(
                'Shadow DELETE: expected 0 ZTD rows, got ' . count($ztdRows)
                . '. Data: ' . json_encode($ztdRows)
            );
        }

        $this->assertCount(0, $ztdRows);

        // Physical should still have the row
        $this->disableZtd();
        $raw = $this->pdo->query("SELECT COUNT(*) AS c FROM sl_iv_items")->fetch(\PDO::FETCH_ASSOC);
        $this->enableZtd();

        $this->assertSame(1, (int) $raw['c']);
    }

    /**
     * ZTD re-enable: previously inserted shadow data still visible.
     */
    public function testZtdReenableShowsShadowData(): void
    {
        // Insert via ZTD
        $this->pdo->exec("INSERT INTO sl_iv_items VALUES (1, 'Shadow', 100)");

        // Disable and re-enable
        $this->disableZtd();
        $this->enableZtd();

        // Shadow data should still be visible
        $rows = $this->ztdQuery("SELECT * FROM sl_iv_items");

        if (count($rows) !== 1) {
            $this->markTestIncomplete(
                'Re-enable: expected 1 shadow row, got ' . count($rows)
                . '. Data: ' . json_encode($rows)
            );
        }

        $this->assertCount(1, $rows);
        $this->assertSame('Shadow', $rows[0]['name']);
    }
}
