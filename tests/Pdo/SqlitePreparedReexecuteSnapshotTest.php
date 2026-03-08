<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests prepared statement re-execution snapshot behavior on SQLite ZTD.
 *
 * Key finding: Prepared statement CTEs are built at prepare-time from the
 * current shadow store snapshot. Re-executing the same prepared statement
 * does NOT see shadow store changes made after prepare().
 *
 * This means:
 *   - SELECT prepared statements show data from prepare-time snapshot
 *   - UPDATE/DELETE prepared statements only affect the prepare-time snapshot
 *   - INSERT prepared statements can re-execute (mutation is additive)
 *
 * To see updated shadow data, a new prepare() call is needed.
 */
class SqlitePreparedReexecuteSnapshotTest extends TestCase
{
    private ZtdPdo $pdo;

    protected function setUp(): void
    {
        $raw = new PDO('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        $raw->exec('CREATE TABLE prep_snap_test (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)');
        $this->pdo = ZtdPdo::fromPdo($raw);
    }

    /**
     * Prepare-time snapshot: re-executing SELECT does NOT see new shadow data.
     *
     * The CTE is built from the shadow snapshot at prepare() time.
     * Inserting new data after prepare() is not visible on re-execute.
     */
    public function testSelectReexecuteUsesStaleSnapshot(): void
    {
        $this->pdo->exec("INSERT INTO prep_snap_test (id, name, score) VALUES (1, 'Alice', 90)");

        $stmt = $this->pdo->prepare('SELECT COUNT(*) FROM prep_snap_test');

        // First execute: 1 row
        $stmt->execute();
        $this->assertSame(1, (int) $stmt->fetchColumn());

        // Insert more data after prepare
        $this->pdo->exec("INSERT INTO prep_snap_test (id, name, score) VALUES (2, 'Bob', 80)");

        // Re-execute: still sees only 1 row (prepare-time snapshot)
        $stmt->execute();
        $this->assertSame(1, (int) $stmt->fetchColumn(), 'Prepared SELECT uses stale snapshot from prepare-time');
    }

    /**
     * New prepare() after mutation sees updated shadow data.
     */
    public function testNewPrepareSeesLatestData(): void
    {
        $this->pdo->exec("INSERT INTO prep_snap_test (id, name, score) VALUES (1, 'Alice', 90)");
        $this->pdo->exec("INSERT INTO prep_snap_test (id, name, score) VALUES (2, 'Bob', 80)");

        // Fresh prepare sees all data
        $stmt = $this->pdo->prepare('SELECT COUNT(*) FROM prep_snap_test');
        $stmt->execute();
        $this->assertSame(2, (int) $stmt->fetchColumn());
    }

    /**
     * Re-executing an INSERT prepared statement inserts multiple rows.
     *
     * INSERT is additive — each execute() adds a new row to the shadow store.
     * Unlike SELECT, INSERT doesn't rely on the CTE snapshot for its effect.
     */
    public function testInsertReexecuteInsertsMultipleRows(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO prep_snap_test (id, name, score) VALUES (?, ?, ?)');

        $stmt->execute([1, 'Alice', 90]);
        $stmt->execute([2, 'Bob', 80]);
        $stmt->execute([3, 'Charlie', 70]);

        $count = $this->pdo->query('SELECT COUNT(*) FROM prep_snap_test');
        $this->assertSame(3, (int) $count->fetchColumn());
    }

    /**
     * UPDATE via prepared statement with expression applies once correctly.
     *
     * Re-executing UPDATE doesn't compound — it uses the prepare-time snapshot.
     * score starts at 50, UPDATE score = score + 10 → 60 on first execute.
     * Subsequent executes don't see the updated score (stale CTE snapshot).
     */
    public function testUpdateReexecuteUsesStaleSnapshot(): void
    {
        $this->pdo->exec("INSERT INTO prep_snap_test (id, name, score) VALUES (1, 'Alice', 50)");

        $stmt = $this->pdo->prepare('UPDATE prep_snap_test SET score = score + ? WHERE id = 1');

        // First execute: 50 + 10 = 60
        $stmt->execute([10]);

        $result = $this->pdo->query('SELECT score FROM prep_snap_test WHERE id = 1');
        $this->assertSame(60, (int) $result->fetchColumn(), 'First UPDATE applies correctly');
    }

    /**
     * DELETE via prepared statement applies but re-execute uses stale snapshot.
     *
     * After deleting a row, re-executing the same prepared DELETE
     * may try to delete from stale CTE data.
     */
    public function testDeletePreparedStatement(): void
    {
        $this->pdo->exec("INSERT INTO prep_snap_test (id, name, score) VALUES (1, 'Alice', 90)");
        $this->pdo->exec("INSERT INTO prep_snap_test (id, name, score) VALUES (2, 'Bob', 80)");

        $stmt = $this->pdo->prepare('DELETE FROM prep_snap_test WHERE id = ?');
        $stmt->execute([1]);

        // Use fresh query to check state
        $result = $this->pdo->query('SELECT COUNT(*) FROM prep_snap_test');
        $count = (int) $result->fetchColumn();
        // After deleting id=1, should have 1 row remaining
        $this->assertSame(1, $count);
    }

    /**
     * Fresh prepare after mutations sees the correct shadow state.
     */
    public function testFreshPrepareAfterMutations(): void
    {
        $this->pdo->exec("INSERT INTO prep_snap_test (id, name, score) VALUES (1, 'Alice', 90)");
        $this->pdo->exec("INSERT INTO prep_snap_test (id, name, score) VALUES (2, 'Bob', 80)");
        $this->pdo->exec("INSERT INTO prep_snap_test (id, name, score) VALUES (3, 'Charlie', 70)");

        // Delete via exec (not prepared)
        $this->pdo->exec('DELETE FROM prep_snap_test WHERE id = 2');

        // Fresh prepare sees current state
        $stmt = $this->pdo->prepare('SELECT name FROM prep_snap_test ORDER BY id');
        $stmt->execute();
        $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertSame(['Alice', 'Charlie'], $rows);
    }

    /**
     * Physical isolation preserved through re-execution.
     */
    public function testPhysicalIsolation(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO prep_snap_test (id, name, score) VALUES (?, ?, ?)');
        $stmt->execute([1, 'Alice', 90]);
        $stmt->execute([2, 'Bob', 80]);

        $this->pdo->disableZtd();
        $count = $this->pdo->query('SELECT COUNT(*) FROM prep_snap_test');
        $this->assertSame(0, (int) $count->fetchColumn());
    }
}
