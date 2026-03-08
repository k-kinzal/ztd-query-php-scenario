<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests ZTD enable/disable cycle with data persistence on SQLite.
 *
 * Ensures shadow data survives enable/disable toggle cycles and that
 * physical operations during disabled periods don't leak into shadow.
 * @spec SPEC-2.1
 */
class SqliteEnableDisableCycleDataTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_edc_test (id INTEGER PRIMARY KEY, name TEXT, val INTEGER)';
    }

    protected function getTableNames(): array
    {
        return ['sl_edc_test'];
    }


    /**
     * Shadow data persists through disable/re-enable cycle.
     */
    public function testShadowDataPersistsThroughCycle(): void
    {
        $this->pdo->exec("INSERT INTO sl_edc_test VALUES (1, 'Alice', 100)");

        $this->pdo->disableZtd();
        $this->pdo->enableZtd();

        $stmt = $this->pdo->query('SELECT name FROM sl_edc_test WHERE id = 1');
        $this->assertSame('Alice', $stmt->fetchColumn());
    }

    /**
     * Physical INSERT while disabled is invisible when re-enabled.
     */
    public function testPhysicalInsertInvisibleAfterReEnable(): void
    {
        $this->pdo->exec("INSERT INTO sl_edc_test VALUES (1, 'Shadow', 100)");

        $this->pdo->disableZtd();
        $this->pdo->exec("INSERT INTO sl_edc_test VALUES (2, 'Physical', 200)");
        $this->pdo->enableZtd();

        // Shadow store only has the shadow row, not the physical one
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM sl_edc_test');
        $count = (int) $stmt->fetchColumn();
        // Shadow store replaces physical table — physical INSERT is not visible
        $this->assertSame(1, $count);
    }

    /**
     * Multiple toggle cycles accumulate shadow data.
     */
    public function testMultipleToggleCyclesAccumulate(): void
    {
        $this->pdo->exec("INSERT INTO sl_edc_test VALUES (1, 'First', 10)");

        $this->pdo->disableZtd();
        $this->pdo->enableZtd();

        $this->pdo->exec("INSERT INTO sl_edc_test VALUES (2, 'Second', 20)");

        $this->pdo->disableZtd();
        $this->pdo->enableZtd();

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM sl_edc_test');
        $this->assertSame(2, (int) $stmt->fetchColumn());
    }

    /**
     * UPDATE persists through toggle cycle.
     */
    public function testUpdatePersistsThroughCycle(): void
    {
        $this->pdo->exec("INSERT INTO sl_edc_test VALUES (1, 'Alice', 100)");
        $this->pdo->exec("UPDATE sl_edc_test SET val = 999 WHERE id = 1");

        $this->pdo->disableZtd();
        $this->pdo->enableZtd();

        $stmt = $this->pdo->query('SELECT val FROM sl_edc_test WHERE id = 1');
        $this->assertSame(999, (int) $stmt->fetchColumn());
    }

    /**
     * DELETE persists through toggle cycle.
     */
    public function testDeletePersistsThroughCycle(): void
    {
        $this->pdo->exec("INSERT INTO sl_edc_test VALUES (1, 'Alice', 100)");
        $this->pdo->exec("INSERT INTO sl_edc_test VALUES (2, 'Bob', 200)");
        $this->pdo->exec('DELETE FROM sl_edc_test WHERE id = 1');

        $this->pdo->disableZtd();
        $this->pdo->enableZtd();

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM sl_edc_test');
        $this->assertSame(1, (int) $stmt->fetchColumn());
    }
}
