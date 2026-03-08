<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests that query rewriting occurs at prepare time, not execute time.
 * If ZTD mode is toggled between prepare() and execute(), the prepared
 * query retains its original rewritten (or non-rewritten) form.
 * @spec SPEC-2.1
 */
class SqlitePrepareTimeRewritingTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE ptr_items (id INTEGER PRIMARY KEY, name TEXT, price REAL)',
            'CREATE TABLE ptr_users (id INTEGER PRIMARY KEY, name TEXT)',
            'CREATE TABLE ptr_orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount REAL)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['ptr_items', 'ptr_users', 'ptr_orders'];
    }


    /**
     * Prepare SELECT with ZTD enabled, disable ZTD, then execute.
     * The prepared statement should still read from shadow store (CTE rewritten).
     */
    public function testSelectPreparedWithZtdEnabledDisabledBeforeExecute(): void
    {
        $this->pdo->exec("INSERT INTO ptr_items VALUES (10, 'Shadow X', 99.99)");

        $stmt = $this->pdo->prepare('SELECT * FROM ptr_items WHERE id = ?');

        $this->pdo->disableZtd();

        $stmt->execute([10]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        // Shadow data visible because query was rewritten at prepare time
        $this->assertCount(1, $rows);
        $this->assertSame('Shadow X', $rows[0]['name']);

        $this->pdo->enableZtd();
    }

    /**
     * Prepare SELECT with ZTD disabled, enable ZTD, then execute.
     * The prepared statement should read from physical table (not rewritten).
     */
    public function testSelectPreparedWithZtdDisabledEnabledBeforeExecute(): void
    {
        $this->pdo->disableZtd();

        $stmt = $this->pdo->prepare('SELECT * FROM ptr_items ORDER BY id');

        $this->pdo->enableZtd();

        $stmt->execute();
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        // Physical data visible because query was NOT rewritten at prepare time
        $this->assertCount(2, $rows);
        $this->assertSame('Physical A', $rows[0]['name']);
        $this->assertSame('Physical B', $rows[1]['name']);
    }

    /**
     * Prepare INSERT with ZTD enabled, disable ZTD, then execute.
     * The INSERT should go to shadow store (rewritten at prepare time).
     */
    public function testInsertPreparedWithZtdEnabledDisabledBeforeExecute(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO ptr_items VALUES (?, ?, ?)');

        $this->pdo->disableZtd();

        $stmt->execute([20, 'Prepared Insert', 55.00]);

        // Re-enable to query shadow
        $this->pdo->enableZtd();

        $result = $this->pdo->query("SELECT name FROM ptr_items WHERE id = 20");
        $row = $result->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Prepared Insert', $row['name']);

        // Physical table should NOT have this row
        $this->pdo->disableZtd();
        $result = $this->pdo->query("SELECT * FROM ptr_items WHERE id = 20");
        $row = $result->fetch(PDO::FETCH_ASSOC);
        $this->assertFalse($row);
    }

    /**
     * Prepare UPDATE with ZTD enabled, disable ZTD, then execute.
     * The UPDATE should affect shadow store only.
     */
    public function testUpdatePreparedWithZtdEnabledDisabledBeforeExecute(): void
    {
        $this->pdo->exec("INSERT INTO ptr_items VALUES (1, 'Shadow A', 10.00)");

        $stmt = $this->pdo->prepare('UPDATE ptr_items SET price = ? WHERE id = ?');

        $this->pdo->disableZtd();

        $stmt->execute([999.99, 1]);

        // Check shadow store
        $this->pdo->enableZtd();
        $result = $this->pdo->query("SELECT price FROM ptr_items WHERE id = 1");
        $row = $result->fetch(PDO::FETCH_ASSOC);
        $this->assertEqualsWithDelta(999.99, (float) $row['price'], 0.01);

        // Physical table has no data (INSERT was through ZTD shadow)
        $this->pdo->disableZtd();
        $result = $this->pdo->query("SELECT price FROM ptr_items WHERE id = 1");
        $row = $result->fetch(PDO::FETCH_ASSOC);
        $this->assertFalse($row, 'Physical table should be empty — INSERT was shadow-only');
    }

    /**
     * Re-execute a prepared statement after toggling ZTD multiple times.
     * The statement retains its prepare-time rewriting regardless of current ZTD state.
     */
    public function testReExecuteAcrossMultipleToggles(): void
    {
        $this->pdo->exec("INSERT INTO ptr_items VALUES (1, 'Shadow A', 10.00)");
        $this->pdo->exec("INSERT INTO ptr_items VALUES (2, 'Shadow B', 20.00)");

        $stmt = $this->pdo->prepare('SELECT COUNT(*) AS cnt FROM ptr_items');

        // Execute 1: ZTD enabled (same as prepare time)
        $stmt->execute();
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(2, (int) $row['cnt']);

        // Execute 2: ZTD disabled
        $this->pdo->disableZtd();
        $stmt->execute();
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(2, (int) $row['cnt']); // Still reads shadow (rewritten at prepare)

        // Execute 3: ZTD re-enabled
        $this->pdo->enableZtd();
        $stmt->execute();
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(2, (int) $row['cnt']);
    }

    /**
     * Two prepared statements — one prepared with ZTD on, one with ZTD off —
     * retain their respective behaviors when executed with the opposite setting.
     */
    public function testTwoPreparedStatementsOppositeToggle(): void
    {
        $this->pdo->exec("INSERT INTO ptr_items VALUES (10, 'Shadow Only', 50.00)");

        // Prepare with ZTD enabled (reads shadow)
        $stmtShadow = $this->pdo->prepare('SELECT COUNT(*) AS cnt FROM ptr_items');

        // Prepare with ZTD disabled (reads physical)
        $this->pdo->disableZtd();
        $stmtPhysical = $this->pdo->prepare('SELECT COUNT(*) AS cnt FROM ptr_items');

        // Execute both with ZTD enabled
        $this->pdo->enableZtd();

        $stmtShadow->execute();
        $shadowCount = (int) $stmtShadow->fetch(PDO::FETCH_ASSOC)['cnt'];

        $stmtPhysical->execute();
        $physicalCount = (int) $stmtPhysical->fetch(PDO::FETCH_ASSOC)['cnt'];

        // Shadow statement sees shadow data (1 row)
        $this->assertSame(1, $shadowCount);
        // Physical statement sees physical data (2 rows)
        $this->assertSame(2, $physicalCount);
    }

    /**
     * Prepared DELETE with ZTD enabled, disable before execute.
     * Deletion affects shadow store only.
     */
    public function testDeletePreparedWithZtdEnabledDisabledBeforeExecute(): void
    {
        $this->pdo->exec("INSERT INTO ptr_items VALUES (1, 'Shadow A', 10.00)");
        $this->pdo->exec("INSERT INTO ptr_items VALUES (2, 'Shadow B', 20.00)");

        $stmt = $this->pdo->prepare('DELETE FROM ptr_items WHERE id = ?');

        $this->pdo->disableZtd();
        $stmt->execute([1]);

        // Shadow store should have 1 row left
        $this->pdo->enableZtd();
        $result = $this->pdo->query("SELECT COUNT(*) AS cnt FROM ptr_items");
        $row = $result->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(1, (int) $row['cnt']);

        // Physical table should still have both original rows
        $this->pdo->disableZtd();
        $result = $this->pdo->query("SELECT COUNT(*) AS cnt FROM ptr_items");
        $row = $result->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(2, (int) $row['cnt']);
    }

    /**
     * Prepare with JOIN while ZTD enabled, execute with ZTD disabled.
     * Both tables should be read from shadow store.
     */
    public function testJoinPreparedWithZtdEnabledDisabledBeforeExecute(): void
    {
        $raw = new PDO('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        $raw->exec('CREATE TABLE ptr_users (id INTEGER PRIMARY KEY, name TEXT)');
        $raw->exec('CREATE TABLE ptr_orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount REAL)');
        $raw->exec("INSERT INTO ptr_users VALUES (1, 'Physical User')");

        $pdo = ZtdPdo::fromPdo($raw);

        $pdo->exec("INSERT INTO ptr_users VALUES (1, 'Shadow User')");
        $pdo->exec("INSERT INTO ptr_orders VALUES (1, 1, 100.00)");

        $stmt = $pdo->prepare('SELECT u.name, o.amount FROM ptr_users u JOIN ptr_orders o ON o.user_id = u.id WHERE u.id = ?');

        $pdo->disableZtd();
        $stmt->execute([1]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        // Should see shadow data (rewritten at prepare time)
        $this->assertCount(1, $rows);
        $this->assertSame('Shadow User', $rows[0]['name']);

        $pdo->enableZtd();
    }
}
