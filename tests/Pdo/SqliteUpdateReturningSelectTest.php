<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests UPDATE followed immediately by SELECT in common application patterns.
 *
 * Verifies that patterns like "update and return" work correctly through ZTD,
 * including UPDATE with WHERE subquery, conditional UPDATE, and UPDATE with
 * computed SET expressions.
 *
 * @spec SPEC-4.3
 */
class SqliteUpdateReturningSelectTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE accounts (id INTEGER PRIMARY KEY, name TEXT, balance INTEGER, status TEXT)',
            'CREATE TABLE txn_log (id INTEGER PRIMARY KEY, account_id INTEGER, amount INTEGER, type TEXT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['accounts', 'txn_log'];
    }

    /**
     * UPDATE with computed SET (balance = balance + amount) then SELECT.
     */
    public function testUpdateWithComputedSetThenSelect(): void
    {
        $this->pdo->exec("INSERT INTO accounts (id, name, balance, status) VALUES (1, 'Alice', 1000, 'active')");

        $this->pdo->exec("UPDATE accounts SET balance = balance + 500 WHERE id = 1");

        $rows = $this->ztdQuery('SELECT balance FROM accounts WHERE id = 1');
        $this->assertEquals(1500, (int) $rows[0]['balance']);
    }

    /**
     * Multiple computed UPDATEs accumulate correctly.
     */
    public function testMultipleComputedUpdatesAccumulate(): void
    {
        $this->pdo->exec("INSERT INTO accounts (id, name, balance, status) VALUES (1, 'Alice', 1000, 'active')");

        $this->pdo->exec("UPDATE accounts SET balance = balance + 100 WHERE id = 1");
        $this->pdo->exec("UPDATE accounts SET balance = balance + 200 WHERE id = 1");
        $this->pdo->exec("UPDATE accounts SET balance = balance - 50 WHERE id = 1");

        $rows = $this->ztdQuery('SELECT balance FROM accounts WHERE id = 1');
        $this->assertEquals(1250, (int) $rows[0]['balance']);
    }

    /**
     * Conditional UPDATE (update only if condition met).
     */
    public function testConditionalUpdate(): void
    {
        $this->pdo->exec("INSERT INTO accounts (id, name, balance, status) VALUES
            (1, 'Alice', 1000, 'active'),
            (2, 'Bob', 500, 'frozen')");

        // Only update active accounts
        $this->pdo->exec("UPDATE accounts SET balance = balance + 100 WHERE status = 'active'");

        $rows = $this->ztdQuery('SELECT name, balance FROM accounts ORDER BY id');
        $this->assertEquals(1100, (int) $rows[0]['balance']); // Alice updated
        $this->assertEquals(500, (int) $rows[1]['balance']);   // Bob unchanged
    }

    /**
     * UPDATE with CASE expression in SET.
     */
    public function testUpdateWithCaseInSet(): void
    {
        $this->pdo->exec("INSERT INTO accounts (id, name, balance, status) VALUES
            (1, 'Alice', 1000, 'active'),
            (2, 'Bob', 2000, 'active'),
            (3, 'Charlie', 500, 'active')");

        $this->pdo->exec("UPDATE accounts SET status = CASE
            WHEN balance >= 1000 THEN 'premium'
            ELSE 'basic'
            END WHERE status = 'active'");

        $rows = $this->ztdQuery('SELECT name, status FROM accounts ORDER BY id');
        $this->assertSame('premium', $rows[0]['status']); // Alice 1000
        $this->assertSame('premium', $rows[1]['status']); // Bob 2000
        $this->assertSame('basic', $rows[2]['status']);    // Charlie 500
    }

    /**
     * UPDATE with string function in SET.
     */
    public function testUpdateWithStringFunctionInSet(): void
    {
        $this->pdo->exec("INSERT INTO accounts (id, name, balance, status) VALUES (1, 'alice smith', 1000, 'active')");

        $this->pdo->exec("UPDATE accounts SET name = UPPER(name) WHERE id = 1");

        $rows = $this->ztdQuery('SELECT name FROM accounts WHERE id = 1');
        $this->assertSame('ALICE SMITH', $rows[0]['name']);
    }

    /**
     * Prepared UPDATE with computed SET then prepared SELECT.
     */
    public function testPreparedUpdateComputedSetThenSelect(): void
    {
        $this->pdo->exec("INSERT INTO accounts (id, name, balance, status) VALUES (1, 'Alice', 1000, 'active')");

        $update = $this->pdo->prepare('UPDATE accounts SET balance = balance + ? WHERE id = ?');
        $update->execute([250, 1]);

        $select = $this->pdo->prepare('SELECT balance FROM accounts WHERE id = ?');
        $select->execute([1]);
        $row = $select->fetch(PDO::FETCH_ASSOC);
        $this->assertEquals(1250, (int) $row['balance']);
    }

    /**
     * UPDATE affecting multiple rows then aggregate SELECT.
     */
    public function testUpdateMultipleRowsThenAggregate(): void
    {
        $this->pdo->exec("INSERT INTO accounts (id, name, balance, status) VALUES
            (1, 'A', 100, 'active'),
            (2, 'B', 200, 'active'),
            (3, 'C', 300, 'active')");

        $this->pdo->exec("UPDATE accounts SET balance = balance * 2 WHERE balance > 100");

        $rows = $this->ztdQuery('SELECT SUM(balance) AS total FROM accounts');
        $this->assertEquals(1100, (int) $rows[0]['total']); // 100 + 400 + 600
    }

    /**
     * UPDATE then JOIN SELECT to verify cross-table consistency.
     */
    public function testUpdateThenJoinSelect(): void
    {
        $this->pdo->exec("INSERT INTO accounts (id, name, balance, status) VALUES (1, 'Alice', 1000, 'active')");
        $this->pdo->exec("INSERT INTO txn_log (id, account_id, amount, type) VALUES (1, 1, 500, 'deposit')");

        $this->pdo->exec("UPDATE accounts SET balance = balance + 500 WHERE id = 1");

        $rows = $this->ztdQuery(
            "SELECT a.balance, t.amount FROM accounts a
             JOIN txn_log t ON t.account_id = a.id WHERE a.id = 1"
        );
        $this->assertCount(1, $rows);
        $this->assertEquals(1500, (int) $rows[0]['balance']);
        $this->assertEquals(500, (int) $rows[0]['amount']);
    }
}
