<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests ZTD lifecycle edge cases on MySQL via MySQLi: toggle cycles, re-enable after disable,
 * multiple enable/disable sequences, and shadow store behavior across cycles.
 * @spec SPEC-2.1
 */
class ZtdLifecycleTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mysqli_lifecycle_test (id INT PRIMARY KEY, val VARCHAR(255))';
    }

    protected function getTableNames(): array
    {
        return ['mysqli_lifecycle_test'];
    }


    public function testShadowDataPreservedAfterDisableEnable(): void
    {
        $this->mysqli->query("INSERT INTO mysqli_lifecycle_test (id, val) VALUES (1, 'shadow')");

        $result = $this->mysqli->query('SELECT * FROM mysqli_lifecycle_test');
        $this->assertSame(1, $result->num_rows);

        $this->mysqli->disableZtd();
        $this->mysqli->enableZtd();

        $result = $this->mysqli->query('SELECT * FROM mysqli_lifecycle_test');
        $this->assertSame(1, $result->num_rows);
    }

    public function testDisabledZtdReadsPhysicalData(): void
    {
        $this->mysqli->disableZtd();
        $this->mysqli->query("INSERT INTO mysqli_lifecycle_test (id, val) VALUES (1, 'physical')");

        $result = $this->mysqli->query('SELECT * FROM mysqli_lifecycle_test WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertSame('physical', $row['val']);

        $this->mysqli->enableZtd();

        // Physical data not visible via ZTD
        $result = $this->mysqli->query('SELECT * FROM mysqli_lifecycle_test WHERE id = 1');
        $this->assertSame(0, $result->num_rows);

        // Clean up
        $this->mysqli->disableZtd();
        $this->mysqli->query('DELETE FROM mysqli_lifecycle_test WHERE id = 1');
    }

    public function testMultipleToggleCycles(): void
    {
        $this->mysqli->query("INSERT INTO mysqli_lifecycle_test (id, val) VALUES (1, 'cycle1')");

        $this->mysqli->disableZtd();
        $this->mysqli->enableZtd();
        $this->mysqli->query("INSERT INTO mysqli_lifecycle_test (id, val) VALUES (2, 'cycle2')");

        $this->mysqli->disableZtd();
        $this->mysqli->enableZtd();
        $this->mysqli->query("INSERT INTO mysqli_lifecycle_test (id, val) VALUES (3, 'cycle3')");

        $result = $this->mysqli->query('SELECT * FROM mysqli_lifecycle_test ORDER BY id');
        $rows = $result->fetch_all(MYSQLI_ASSOC);

        $this->assertCount(3, $rows);
        $this->assertSame('cycle1', $rows[0]['val']);
        $this->assertSame('cycle2', $rows[1]['val']);
        $this->assertSame('cycle3', $rows[2]['val']);
    }

    public function testIsZtdEnabledReflectsState(): void
    {
        $this->assertTrue($this->mysqli->isZtdEnabled());

        $this->mysqli->disableZtd();
        $this->assertFalse($this->mysqli->isZtdEnabled());

        $this->mysqli->enableZtd();
        $this->assertTrue($this->mysqli->isZtdEnabled());
    }

    public function testShadowWritesVisibleAfterReEnable(): void
    {
        $this->mysqli->query("INSERT INTO mysqli_lifecycle_test (id, val) VALUES (1, 'before')");

        $this->mysqli->disableZtd();
        $this->mysqli->query("INSERT INTO mysqli_lifecycle_test (id, val) VALUES (2, 'physical')");
        $this->mysqli->enableZtd();

        $result = $this->mysqli->query('SELECT * FROM mysqli_lifecycle_test WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertSame('before', $row['val']);

        $result = $this->mysqli->query('SELECT * FROM mysqli_lifecycle_test WHERE id = 2');
        $this->assertSame(0, $result->num_rows);

        // Clean up
        $this->mysqli->disableZtd();
        $this->mysqli->query('DELETE FROM mysqli_lifecycle_test WHERE id = 2');
    }

    public function testManySequentialOperations(): void
    {
        for ($i = 1; $i <= 50; $i++) {
            $this->mysqli->query("INSERT INTO mysqli_lifecycle_test (id, val) VALUES ($i, 'item_$i')");
        }

        $result = $this->mysqli->query('SELECT COUNT(*) as cnt FROM mysqli_lifecycle_test');
        $this->assertSame(50, (int) $result->fetch_assoc()['cnt']);

        $this->mysqli->query("UPDATE mysqli_lifecycle_test SET val = 'updated' WHERE id <= 25");

        $result = $this->mysqli->query("SELECT COUNT(*) as cnt FROM mysqli_lifecycle_test WHERE val = 'updated'");
        $this->assertSame(25, (int) $result->fetch_assoc()['cnt']);

        $this->mysqli->query('DELETE FROM mysqli_lifecycle_test WHERE id <= 12');

        $result = $this->mysqli->query('SELECT COUNT(*) as cnt FROM mysqli_lifecycle_test');
        $this->assertSame(38, (int) $result->fetch_assoc()['cnt']);

        // Physical table empty
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) as cnt FROM mysqli_lifecycle_test');
        $this->assertSame(0, (int) $result->fetch_assoc()['cnt']);
    }
}
