<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests that re-executing a prepared SELECT reflects shadow store mutations.
 *
 * When a prepared SELECT is executed, the CTE-rewritten SQL is baked into the
 * inner prepared statement. On subsequent execute() calls, the shadow store
 * may have changed (INSERT/UPDATE/DELETE via other statements), but the prepared
 * SELECT uses stale shadow data from the first execution.
 *
 * This is a new issue: prepared SELECT re-execution does not re-read the shadow
 * store. Fresh query() calls correctly reflect mutations, but re-executing a
 * previously-prepared SELECT does not.
 *
 * @spec SPEC-3.2
 */
class SqlitePreparedSelectReexecuteStaleTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE reexec (id INTEGER PRIMARY KEY, val TEXT, score INTEGER)';
    }

    protected function getTableNames(): array
    {
        return ['reexec'];
    }

    /**
     * Baseline: fresh query() sees UPDATE mutations.
     */
    public function testFreshQuerySeesUpdateMutation(): void
    {
        $this->pdo->exec("INSERT INTO reexec (id, val, score) VALUES (1, 'original', 10)");
        $this->pdo->exec("UPDATE reexec SET val = 'updated' WHERE id = 1");

        $rows = $this->ztdQuery('SELECT val FROM reexec WHERE id = 1');
        $this->assertSame('updated', $rows[0]['val']);
    }

    /**
     * Prepared SELECT re-execute after UPDATE should see mutation.
     *
     * Expected: re-execute returns 'updated'
     * Actual: re-execute returns 'original' (stale shadow data)
     */
    public function testPreparedSelectReexecuteAfterUpdate(): void
    {
        $this->pdo->exec("INSERT INTO reexec (id, val, score) VALUES (1, 'original', 10)");

        $stmt = $this->pdo->prepare('SELECT val FROM reexec WHERE id = ?');

        // First execution
        $stmt->execute([1]);
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('original', $row['val']);
        $stmt->closeCursor();

        // Mutate
        $this->pdo->exec("UPDATE reexec SET val = 'updated' WHERE id = 1");

        // Re-execute — should see the mutation
        $stmt->execute([1]);
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('updated', $row['val']);
    }

    /**
     * Prepared SELECT re-execute after INSERT should find new row.
     *
     * Expected: re-execute finds the newly inserted row
     * Actual: re-execute returns false (row not found)
     */
    public function testPreparedSelectReexecuteAfterInsert(): void
    {
        $this->pdo->exec("INSERT INTO reexec (id, val, score) VALUES (1, 'first', 10)");

        $stmt = $this->pdo->prepare('SELECT val FROM reexec WHERE id = ?');
        $stmt->execute([1]);
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('first', $row['val']);
        $stmt->closeCursor();

        // Insert new row
        $this->pdo->exec("INSERT INTO reexec (id, val, score) VALUES (2, 'second', 20)");

        // Re-execute for the new row
        $stmt->execute([2]);
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertNotFalse($row, 'Re-executed prepared SELECT should find newly inserted row');
        $this->assertSame('second', $row['val']);
    }

    /**
     * Prepared SELECT re-execute after DELETE should reflect deletion.
     */
    public function testPreparedSelectReexecuteAfterDelete(): void
    {
        $this->pdo->exec("INSERT INTO reexec (id, val, score) VALUES (1, 'a', 10), (2, 'b', 20)");

        $stmt = $this->pdo->prepare('SELECT COUNT(*) AS cnt FROM reexec');
        $stmt->execute();
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertEquals(2, (int) $row['cnt']);
        $stmt->closeCursor();

        // Delete a row
        $this->pdo->exec("DELETE FROM reexec WHERE id = 1");

        // Re-execute
        $stmt->execute();
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertEquals(1, (int) $row['cnt'], 'Re-executed prepared SELECT should reflect deletion');
    }

    /**
     * Prepared SELECT without closeCursor — re-execute after mutation.
     * (Same issue exists regardless of closeCursor)
     */
    public function testPreparedSelectReexecuteWithoutCloseCursor(): void
    {
        $this->pdo->exec("INSERT INTO reexec (id, val, score) VALUES (1, 'v1', 10)");

        $stmt = $this->pdo->prepare('SELECT val FROM reexec WHERE id = ?');
        $stmt->execute([1]);
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('v1', $row['val']);
        // Consume remaining rows but don't closeCursor
        while ($stmt->fetch()) {
        }

        $this->pdo->exec("UPDATE reexec SET val = 'v2' WHERE id = 1");

        $stmt->execute([1]);
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('v2', $row['val']);
    }

    /**
     * Contrast: DML prepared re-execute correctly applies to updated shadow.
     * (Uses exec() for INSERT to avoid Issue #23 — prepared INSERT rows can't be updated)
     */
    public function testDmlPreparedReexecuteWorksCorrectly(): void
    {
        $this->pdo->exec("INSERT INTO reexec (id, val, score) VALUES (1, 'a', 10), (2, 'b', 20)");

        $update = $this->pdo->prepare('UPDATE reexec SET score = ? WHERE id = ?');
        $update->execute([100, 1]);
        $update->execute([200, 2]);

        // Verify via fresh query
        $rows = $this->ztdQuery('SELECT id, score FROM reexec ORDER BY id');
        $this->assertEquals(100, (int) $rows[0]['score']);
        $this->assertEquals(200, (int) $rows[1]['score']);
    }

    /**
     * Multiple mutation cycles with prepared SELECT.
     */
    public function testMultipleMutationCyclesWithPreparedSelect(): void
    {
        $this->pdo->exec("INSERT INTO reexec (id, val, score) VALUES (1, 'v0', 0)");

        $sel = $this->pdo->prepare('SELECT score FROM reexec WHERE id = 1');

        for ($i = 1; $i <= 3; $i++) {
            $this->pdo->exec("UPDATE reexec SET score = $i WHERE id = 1");

            $sel->execute();
            $row = $sel->fetch(PDO::FETCH_ASSOC);
            $this->assertEquals($i, (int) $row['score'], "Cycle $i should see updated score");
            $sel->closeCursor();
        }
    }
}
