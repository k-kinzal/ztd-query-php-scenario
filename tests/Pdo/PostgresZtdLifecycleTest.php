<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests ZTD lifecycle edge cases on PostgreSQL via PDO: toggle cycles, re-enable after disable,
 * multiple enable/disable sequences, and shadow store behavior across cycles.
 * @spec SPEC-2.1
 */
class PostgresZtdLifecycleTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pg_lifecycle_test (id INT PRIMARY KEY, val VARCHAR(255))';
    }

    protected function getTableNames(): array
    {
        return ['pg_lifecycle_test'];
    }


    public function testShadowDataPreservedAfterDisableEnable(): void
    {
        $this->pdo->exec("INSERT INTO pg_lifecycle_test (id, val) VALUES (1, 'shadow')");

        $stmt = $this->pdo->query('SELECT * FROM pg_lifecycle_test');
        $this->assertCount(1, $stmt->fetchAll(PDO::FETCH_ASSOC));

        $this->pdo->disableZtd();
        $this->pdo->enableZtd();

        // Shadow data preserved across toggle
        $stmt = $this->pdo->query('SELECT * FROM pg_lifecycle_test');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
    }

    public function testDisabledZtdReadsPhysicalData(): void
    {
        $this->pdo->disableZtd();
        $this->pdo->exec("INSERT INTO pg_lifecycle_test (id, val) VALUES (1, 'physical')");

        $stmt = $this->pdo->query('SELECT * FROM pg_lifecycle_test WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('physical', $rows[0]['val']);

        $this->pdo->enableZtd();

        // Physical data not visible via ZTD CTE shadow
        $stmt = $this->pdo->query('SELECT * FROM pg_lifecycle_test WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(0, $rows);

        // Clean up physical data
        $this->pdo->disableZtd();
        $this->pdo->exec('DELETE FROM pg_lifecycle_test WHERE id = 1');
    }

    public function testMultipleToggleCycles(): void
    {
        $this->pdo->exec("INSERT INTO pg_lifecycle_test (id, val) VALUES (1, 'cycle1')");

        $this->pdo->disableZtd();
        $this->pdo->enableZtd();
        $this->pdo->exec("INSERT INTO pg_lifecycle_test (id, val) VALUES (2, 'cycle2')");

        $this->pdo->disableZtd();
        $this->pdo->enableZtd();
        $this->pdo->exec("INSERT INTO pg_lifecycle_test (id, val) VALUES (3, 'cycle3')");

        $stmt = $this->pdo->query('SELECT * FROM pg_lifecycle_test ORDER BY id');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(3, $rows);
        $this->assertSame('cycle1', $rows[0]['val']);
        $this->assertSame('cycle2', $rows[1]['val']);
        $this->assertSame('cycle3', $rows[2]['val']);
    }

    public function testIsZtdEnabledReflectsState(): void
    {
        $this->assertTrue($this->pdo->isZtdEnabled());

        $this->pdo->disableZtd();
        $this->assertFalse($this->pdo->isZtdEnabled());

        $this->pdo->enableZtd();
        $this->assertTrue($this->pdo->isZtdEnabled());
    }

    public function testShadowWritesVisibleAfterReEnable(): void
    {
        $this->pdo->exec("INSERT INTO pg_lifecycle_test (id, val) VALUES (1, 'before')");

        $this->pdo->disableZtd();
        $this->pdo->exec("INSERT INTO pg_lifecycle_test (id, val) VALUES (2, 'physical')");
        $this->pdo->enableZtd();

        // Shadow data from before toggle still visible
        $stmt = $this->pdo->query('SELECT * FROM pg_lifecycle_test WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('before', $rows[0]['val']);

        // Physical data inserted while disabled NOT visible via ZTD
        $stmt = $this->pdo->query('SELECT * FROM pg_lifecycle_test WHERE id = 2');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(0, $rows);

        // Clean up physical data
        $this->pdo->disableZtd();
        $this->pdo->exec('DELETE FROM pg_lifecycle_test WHERE id = 2');
    }

    public function testManySequentialOperations(): void
    {
        for ($i = 1; $i <= 50; $i++) {
            $this->pdo->exec("INSERT INTO pg_lifecycle_test (id, val) VALUES ($i, 'item_$i')");
        }

        $stmt = $this->pdo->query('SELECT COUNT(*) as cnt FROM pg_lifecycle_test');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(50, (int) $row['cnt']);

        $this->pdo->exec("UPDATE pg_lifecycle_test SET val = 'updated' WHERE id <= 25");

        $stmt = $this->pdo->query("SELECT COUNT(*) as cnt FROM pg_lifecycle_test WHERE val = 'updated'");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(25, (int) $row['cnt']);

        $this->pdo->exec('DELETE FROM pg_lifecycle_test WHERE id <= 12');

        $stmt = $this->pdo->query('SELECT COUNT(*) as cnt FROM pg_lifecycle_test');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(38, (int) $row['cnt']);

        // Physical table empty
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) as cnt FROM pg_lifecycle_test');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $row['cnt']);
    }
}
