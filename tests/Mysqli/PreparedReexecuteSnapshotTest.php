<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests prepared statement re-execution snapshot behavior on MySQLi ZTD.
 *
 * Key finding: Prepared statement CTEs are built at prepare-time from the
 * current shadow store snapshot. Re-executing does NOT see shadow changes
 * made after prepare().
 *
 * This is the MySQLi equivalent of SqlitePreparedReexecuteSnapshotTest.
 * @spec pending
 */
class PreparedReexecuteSnapshotTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_prep_snap_test (id INT PRIMARY KEY, name VARCHAR(50), score INT)';
    }

    protected function getTableNames(): array
    {
        return ['mi_prep_snap_test'];
    }


    /**
     * Prepare-time snapshot: re-executing SELECT does NOT see new shadow data.
     */
    public function testSelectReexecuteUsesStaleSnapshot(): void
    {
        $this->mysqli->query("INSERT INTO mi_prep_snap_test (id, name, score) VALUES (1, 'Alice', 90)");

        $stmt = $this->mysqli->prepare('SELECT COUNT(*) AS cnt FROM mi_prep_snap_test');

        // First execute: 1 row
        $stmt->execute();
        $result = $stmt->get_result();
        $this->assertSame(1, (int) $result->fetch_assoc()['cnt']);

        // Insert more after prepare
        $this->mysqli->query("INSERT INTO mi_prep_snap_test (id, name, score) VALUES (2, 'Bob', 80)");

        // Re-execute: still sees 1 (prepare-time snapshot)
        $stmt->execute();
        $result = $stmt->get_result();
        $count = (int) $result->fetch_assoc()['cnt'];
        $this->assertSame(1, $count, 'Prepared SELECT uses stale snapshot from prepare-time');
    }

    /**
     * New prepare() after mutation sees updated shadow data.
     */
    public function testNewPrepareSeesLatestData(): void
    {
        $this->mysqli->query("INSERT INTO mi_prep_snap_test (id, name, score) VALUES (1, 'Alice', 90)");
        $this->mysqli->query("INSERT INTO mi_prep_snap_test (id, name, score) VALUES (2, 'Bob', 80)");

        $stmt = $this->mysqli->prepare('SELECT COUNT(*) AS cnt FROM mi_prep_snap_test');
        $stmt->execute();
        $result = $stmt->get_result();
        $this->assertSame(2, (int) $result->fetch_assoc()['cnt']);
    }

    /**
     * Re-executing INSERT inserts multiple rows (additive, no CTE dependency).
     */
    public function testInsertReexecuteInsertsMultiple(): void
    {
        $stmt = $this->mysqli->prepare('INSERT INTO mi_prep_snap_test (id, name, score) VALUES (?, ?, ?)');

        $stmt->execute([1, 'Alice', 90]);
        $stmt->execute([2, 'Bob', 80]);
        $stmt->execute([3, 'Charlie', 70]);

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_prep_snap_test');
        $this->assertSame(3, (int) $result->fetch_assoc()['cnt']);
    }

    /**
     * Physical isolation preserved through re-execution.
     */
    public function testPhysicalIsolation(): void
    {
        $stmt = $this->mysqli->prepare('INSERT INTO mi_prep_snap_test (id, name, score) VALUES (?, ?, ?)');
        $stmt->execute([1, 'Alice', 90]);
        $stmt->execute([2, 'Bob', 80]);

        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_prep_snap_test');
        $this->assertSame(0, (int) $result->fetch_assoc()['cnt']);
    }
}
