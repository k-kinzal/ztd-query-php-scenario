<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests prepared statement CTE snapshot behavior on SQLite.
 *
 * Prepared statements capture shadow store data at prepare() time.
 * Changes after prepare() are NOT visible to the prepared statement.
 * @spec pending
 */
class SqlitePreparedSnapshotBehaviorTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE psb_test (id INT PRIMARY KEY, name VARCHAR(50), score INT)';
    }

    protected function getTableNames(): array
    {
        return ['psb_test'];
    }


    /**
     * INSERT after prepare() is NOT visible to the prepared SELECT.
     */
    public function testInsertAfterPrepareNotVisible(): void
    {
        $stmt = $this->pdo->prepare('SELECT COUNT(*) FROM psb_test');

        // Insert after prepare
        $this->pdo->exec("INSERT INTO psb_test VALUES (3, 'Charlie', 90)");

        $stmt->execute();
        // Prepared SELECT sees 2 rows (snapshot at prepare time), not 3
        $this->assertSame(2, (int) $stmt->fetchColumn());
    }

    /**
     * Fresh prepare() after INSERT sees the new data.
     */
    public function testFreshPrepareSeesNewData(): void
    {
        $this->pdo->exec("INSERT INTO psb_test VALUES (3, 'Charlie', 90)");

        $stmt = $this->pdo->prepare('SELECT COUNT(*) FROM psb_test');
        $stmt->execute();
        $this->assertSame(3, (int) $stmt->fetchColumn());
    }

    /**
     * DELETE after prepare() is NOT visible to the prepared SELECT.
     */
    public function testDeleteAfterPrepareNotVisible(): void
    {
        $stmt = $this->pdo->prepare('SELECT COUNT(*) FROM psb_test');

        $this->pdo->exec("DELETE FROM psb_test WHERE name = 'Bob'");

        $stmt->execute();
        // Still sees 2 rows (snapshot at prepare time)
        $this->assertSame(2, (int) $stmt->fetchColumn());
    }

    /**
     * UPDATE after prepare() is NOT visible to the prepared SELECT.
     */
    public function testUpdateAfterPrepareNotVisible(): void
    {
        $stmt = $this->pdo->prepare('SELECT score FROM psb_test WHERE id = 1');

        $this->pdo->exec('UPDATE psb_test SET score = 999 WHERE id = 1');

        $stmt->execute();
        // Still sees original score 100
        $this->assertSame(100, (int) $stmt->fetchColumn());
    }

    /**
     * Re-execution of same prepared statement uses same stale snapshot.
     */
    public function testReExecutionUsesStaleSnapshot(): void
    {
        $stmt = $this->pdo->prepare('SELECT COUNT(*) FROM psb_test');

        $stmt->execute();
        $this->assertSame(2, (int) $stmt->fetchColumn());

        $this->pdo->exec("INSERT INTO psb_test VALUES (3, 'Charlie', 90)");

        // Re-execute: still 2 rows
        $stmt->execute();
        $this->assertSame(2, (int) $stmt->fetchColumn());
    }
}
