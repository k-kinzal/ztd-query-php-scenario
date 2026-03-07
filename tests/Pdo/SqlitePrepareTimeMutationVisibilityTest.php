<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests how CTE snapshotting at prepare time affects visibility of mutations
 * that occur between prepare() and execute(), and between multiple execute() calls.
 */
class SqlitePrepareTimeMutationVisibilityTest extends TestCase
{
    private ZtdPdo $pdo;

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);

        $this->pdo->exec('CREATE TABLE vis_users (id INT PRIMARY KEY, name VARCHAR(50))');
        $this->pdo->exec('CREATE TABLE vis_orders (id INT PRIMARY KEY, user_id INT, amount INT)');

        $this->pdo->exec("INSERT INTO vis_users VALUES (1, 'Alice')");
        $this->pdo->exec("INSERT INTO vis_users VALUES (2, 'Bob')");
        $this->pdo->exec("INSERT INTO vis_orders VALUES (1, 1, 100)");
        $this->pdo->exec("INSERT INTO vis_orders VALUES (2, 1, 200)");
    }

    public function testPreparedSelectDoesNotSeePostPrepareInsert(): void
    {
        // Prepare SELECT before inserting new data
        $stmt = $this->pdo->prepare('SELECT COUNT(*) FROM vis_users');

        // Insert after prepare
        $this->pdo->exec("INSERT INTO vis_users VALUES (3, 'Charlie')");

        // Execute — CTE snapshot was taken at prepare time, so Charlie is NOT visible
        $stmt->execute();
        $count = (int) $stmt->fetchColumn();
        $this->assertSame(2, $count);
    }

    public function testPreparedSelectDoesNotSeePostPrepareUpdate(): void
    {
        $stmt = $this->pdo->prepare('SELECT name FROM vis_users WHERE id = ?');

        // Update after prepare
        $this->pdo->exec("UPDATE vis_users SET name = 'UpdatedAlice' WHERE id = 1");

        // Execute — CTE snapshot frozen at prepare, so still sees 'Alice'
        $stmt->execute([1]);
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Alice', $row['name']);
    }

    public function testPreparedSelectDoesNotSeePostPrepareDelete(): void
    {
        $stmt = $this->pdo->prepare('SELECT COUNT(*) FROM vis_users');

        // Delete after prepare
        $this->pdo->exec("DELETE FROM vis_users WHERE id = 2");

        // Execute — CTE snapshot frozen at prepare, so Bob is still counted
        $stmt->execute();
        $count = (int) $stmt->fetchColumn();
        $this->assertSame(2, $count);
    }

    public function testNewPrepareAfterInsertSeesNewData(): void
    {
        $this->pdo->exec("INSERT INTO vis_users VALUES (3, 'Charlie')");

        // Prepare AFTER the insert — should see Charlie
        $stmt = $this->pdo->prepare('SELECT COUNT(*) FROM vis_users');
        $stmt->execute();
        $count = (int) $stmt->fetchColumn();
        $this->assertSame(3, $count);
    }

    public function testJoinPreparedBeforeMutationDoesNotReflectMutation(): void
    {
        // Prepare JOIN before mutation
        $stmt = $this->pdo->prepare(
            'SELECT u.name, SUM(o.amount) AS total
             FROM vis_users u
             JOIN vis_orders o ON o.user_id = u.id
             GROUP BY u.name'
        );

        // Add new order after prepare
        $this->pdo->exec("INSERT INTO vis_orders VALUES (3, 1, 300)");

        // Execute — CTE snapshot frozen, new order NOT visible
        $stmt->execute();
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame(300, (int) $rows[0]['total']); // 100 + 200 = 300 (not 600)
    }

    public function testTwoPreparedStatementsWithDifferentSnapshots(): void
    {
        // Prepare first statement
        $stmt1 = $this->pdo->prepare('SELECT COUNT(*) FROM vis_users');

        // Insert new data
        $this->pdo->exec("INSERT INTO vis_users VALUES (3, 'Charlie')");

        // Prepare second statement — sees different snapshot
        $stmt2 = $this->pdo->prepare('SELECT COUNT(*) FROM vis_users');

        // Execute both
        $stmt1->execute();
        $count1 = (int) $stmt1->fetchColumn();

        $stmt2->execute();
        $count2 = (int) $stmt2->fetchColumn();

        // stmt1 sees 2 (pre-insert snapshot), stmt2 sees 3 (post-insert snapshot)
        $this->assertSame(2, $count1);
        $this->assertSame(3, $count2);
    }

    public function testPreparedDeleteReExecuteUsesFrozenSnapshot(): void
    {
        // Prepare DELETE
        $stmt = $this->pdo->prepare('DELETE FROM vis_orders WHERE user_id = ?');

        // First execute — deletes 2 orders (id 1 and 2)
        $stmt->execute([1]);
        $this->assertSame(2, $stmt->rowCount());

        // Second execute — CTE snapshot frozen at prepare time,
        // so it still "sees" 2 orders even though they were already deleted
        $stmt->execute([1]);
        $this->assertSame(2, $stmt->rowCount());
    }

    public function testQueryAfterPreparedMutationSeesLatestState(): void
    {
        // Prepare and execute INSERT
        $stmt = $this->pdo->prepare('INSERT INTO vis_users (id, name) VALUES (?, ?)');
        $stmt->execute([3, 'Charlie']);

        // query() (not prepare) should see the latest state
        $result = $this->pdo->query('SELECT COUNT(*) FROM vis_users');
        $count = (int) $result->fetchColumn();
        // Note: due to issue #23, prepared INSERT rows may not be updatable,
        // but they should be visible in queries
        $this->assertSame(3, $count);
    }

    public function testExecAfterPreparedSelectSeesLatestState(): void
    {
        // Prepare a SELECT
        $stmt = $this->pdo->prepare('SELECT COUNT(*) FROM vis_users');

        // exec() insert (not prepared)
        $this->pdo->exec("INSERT INTO vis_users VALUES (3, 'Charlie')");

        // exec()-based query should see latest state
        $result = $this->pdo->query('SELECT COUNT(*) FROM vis_users');
        $count = (int) $result->fetchColumn();
        $this->assertSame(3, $count);

        // But the prepared statement still sees old snapshot
        $stmt->execute();
        $prepCount = (int) $stmt->fetchColumn();
        $this->assertSame(2, $prepCount);
    }
}
