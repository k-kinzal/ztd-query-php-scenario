<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests how CTE snapshotting at prepare time affects visibility of mutations
 * that occur between prepare() and execute() on MySQL.
 * @spec SPEC-2.1
 */
class MysqlPrepareTimeMutationVisibilityTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE vis_users_m (id INT PRIMARY KEY, name VARCHAR(50))',
            'CREATE TABLE vis_orders_m (id INT PRIMARY KEY, user_id INT, amount INT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['vis_orders_m', 'vis_users_m'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO vis_users_m VALUES (1, 'Alice')");
        $this->pdo->exec("INSERT INTO vis_users_m VALUES (2, 'Bob')");
        $this->pdo->exec("INSERT INTO vis_orders_m VALUES (1, 1, 100)");
        $this->pdo->exec("INSERT INTO vis_orders_m VALUES (2, 1, 200)");
    }

    public function testPreparedSelectDoesNotSeePostPrepareInsert(): void
    {
        $stmt = $this->pdo->prepare('SELECT COUNT(*) FROM vis_users_m');
        $this->pdo->exec("INSERT INTO vis_users_m VALUES (3, 'Charlie')");
        $stmt->execute();
        $count = (int) $stmt->fetchColumn();
        $this->assertSame(2, $count);
    }

    public function testPreparedSelectDoesNotSeePostPrepareUpdate(): void
    {
        $stmt = $this->pdo->prepare('SELECT name FROM vis_users_m WHERE id = ?');
        $this->pdo->exec("UPDATE vis_users_m SET name = 'UpdatedAlice' WHERE id = 1");
        $stmt->execute([1]);
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Alice', $row['name']);
    }

    public function testPreparedSelectDoesNotSeePostPrepareDelete(): void
    {
        $stmt = $this->pdo->prepare('SELECT COUNT(*) FROM vis_users_m');
        $this->pdo->exec("DELETE FROM vis_users_m WHERE id = 2");
        $stmt->execute();
        $count = (int) $stmt->fetchColumn();
        $this->assertSame(2, $count);
    }

    public function testNewPrepareAfterInsertSeesNewData(): void
    {
        $this->pdo->exec("INSERT INTO vis_users_m VALUES (3, 'Charlie')");
        $stmt = $this->pdo->prepare('SELECT COUNT(*) FROM vis_users_m');
        $stmt->execute();
        $count = (int) $stmt->fetchColumn();
        $this->assertSame(3, $count);
    }

    public function testTwoPreparedStatementsWithDifferentSnapshots(): void
    {
        $stmt1 = $this->pdo->prepare('SELECT COUNT(*) FROM vis_users_m');
        $this->pdo->exec("INSERT INTO vis_users_m VALUES (3, 'Charlie')");
        $stmt2 = $this->pdo->prepare('SELECT COUNT(*) FROM vis_users_m');

        $stmt1->execute();
        $count1 = (int) $stmt1->fetchColumn();
        $stmt2->execute();
        $count2 = (int) $stmt2->fetchColumn();

        $this->assertSame(2, $count1);
        $this->assertSame(3, $count2);
    }

    public function testQueryAfterPreparedMutationSeesLatestState(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO vis_users_m (id, name) VALUES (?, ?)');
        $stmt->execute([3, 'Charlie']);

        $result = $this->pdo->query('SELECT COUNT(*) FROM vis_users_m');
        $count = (int) $result->fetchColumn();
        $this->assertSame(3, $count);
    }

    public function testExecAfterPreparedSelectSeesLatestState(): void
    {
        $stmt = $this->pdo->prepare('SELECT COUNT(*) FROM vis_users_m');
        $this->pdo->exec("INSERT INTO vis_users_m VALUES (3, 'Charlie')");

        $result = $this->pdo->query('SELECT COUNT(*) FROM vis_users_m');
        $count = (int) $result->fetchColumn();
        $this->assertSame(3, $count);

        $stmt->execute();
        $prepCount = (int) $stmt->fetchColumn();
        $this->assertSame(2, $prepCount);
    }
}
