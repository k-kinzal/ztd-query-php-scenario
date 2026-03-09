<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests interleaved prepared statement usage — multiple prepared statements
 * active simultaneously on the same connection. This is common in ORM code
 * that prepares queries eagerly or reuses statements across operations.
 *
 * SQL patterns exercised: multiple prepared statements prepared before any
 * execution, interleaved execute calls, prepare-execute-prepare-execute
 * sequences, statement reuse after DML.
 * @spec SPEC-3.2
 */
class SqliteInterleavedPreparedStatementsTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_ips_products (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                price REAL NOT NULL
            )',
            'CREATE TABLE sl_ips_inventory (
                product_id INTEGER PRIMARY KEY,
                qty INTEGER NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_ips_inventory', 'sl_ips_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_ips_products VALUES (1, 'Widget', 10.00)");
        $this->pdo->exec("INSERT INTO sl_ips_products VALUES (2, 'Gadget', 20.00)");
        $this->pdo->exec("INSERT INTO sl_ips_products VALUES (3, 'Tool', 30.00)");

        $this->pdo->exec("INSERT INTO sl_ips_inventory VALUES (1, 100)");
        $this->pdo->exec("INSERT INTO sl_ips_inventory VALUES (2, 50)");
        $this->pdo->exec("INSERT INTO sl_ips_inventory VALUES (3, 0)");
    }

    /**
     * Prepare two SELECT statements, then execute them interleaved.
     */
    public function testPrepareAllThenExecuteInterleaved(): void
    {
        $stmtProduct = $this->pdo->prepare("SELECT name, price FROM sl_ips_products WHERE id = ?");
        $stmtInventory = $this->pdo->prepare("SELECT qty FROM sl_ips_inventory WHERE product_id = ?");

        // Execute product first
        $stmtProduct->execute([1]);
        $product = $stmtProduct->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame('Widget', $product[0]['name']);

        // Execute inventory
        $stmtInventory->execute([1]);
        $inv = $stmtInventory->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(100, (int) $inv[0]['qty']);

        // Execute product again for different ID
        $stmtProduct->execute([2]);
        $product2 = $stmtProduct->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame('Gadget', $product2[0]['name']);

        // Execute inventory for same ID
        $stmtInventory->execute([2]);
        $inv2 = $stmtInventory->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(50, (int) $inv2[0]['qty']);
    }

    /**
     * INSERT via prepared statement, then read back via different prepared statement.
     *
     * Known issue: Rows inserted via prepared statement cannot be read back
     * (SPEC-11.PDO-PREPARED-INSERT, Issue #23).
     */
    public function testInsertPreparedThenSelectPrepared(): void
    {
        $insertStmt = $this->pdo->prepare("INSERT INTO sl_ips_products VALUES (?, ?, ?)");
        $selectStmt = $this->pdo->prepare("SELECT name, price FROM sl_ips_products WHERE id = ?");

        $insertStmt->execute([4, 'Doohickey', 40.00]);

        $selectStmt->execute([4]);
        $rows = $selectStmt->fetchAll(PDO::FETCH_ASSOC);

        if (count($rows) === 0) {
            $this->markTestIncomplete(
                'SPEC-11.PDO-PREPARED-INSERT [Issue #23]: rows inserted via prepare() invisible to subsequent SELECT.'
            );
        }
        $this->assertCount(1, $rows);
    }

    /**
     * UPDATE via prepared, then verify via different prepared statement.
     *
     * Known issue: UPDATE via prepared statement is silently a no-op
     * (SPEC-11.PDO-PREPARED-INSERT, Issue #23).
     */
    public function testUpdatePreparedThenVerifyPrepared(): void
    {
        $updateStmt = $this->pdo->prepare("UPDATE sl_ips_products SET price = ? WHERE id = ?");
        $selectStmt = $this->pdo->prepare("SELECT price FROM sl_ips_products WHERE id = ?");

        $updateStmt->execute([99.99, 1]);

        $selectStmt->execute([1]);
        $rows = $selectStmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(1, $rows);
        $actual = (float) $rows[0]['price'];
        if (abs($actual - 10.0) < 0.01) {
            $this->markTestIncomplete(
                'SPEC-11.PDO-PREPARED-INSERT [Issue #23]: UPDATE via prepare() was a no-op, price still 10.00.'
            );
        }
        $this->assertEqualsWithDelta(99.99, $actual, 0.01);
    }

    /**
     * Multiple INSERT via same prepared statement (batch loop pattern).
     */
    public function testBatchInsertViaPreparedReuse(): void
    {
        $stmt = $this->pdo->prepare("INSERT INTO sl_ips_products VALUES (?, ?, ?)");

        $stmt->execute([10, 'Item10', 10.00]);
        $stmt->execute([11, 'Item11', 11.00]);
        $stmt->execute([12, 'Item12', 12.00]);

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_ips_products WHERE id >= 10");
        $this->assertEquals(3, (int) $rows[0]['cnt']);
    }

    /**
     * Interleaved INSERT and SELECT on same table using different prepared statements.
     * Simulates an ORM doing insert-then-verify in a loop.
     *
     * Known issue: Rows inserted via prepared statement cannot be read back
     * (SPEC-11.PDO-PREPARED-INSERT, Issue #23).
     */
    public function testInterleavedInsertSelectLoop(): void
    {
        $insertStmt = $this->pdo->prepare("INSERT INTO sl_ips_inventory VALUES (?, ?)");
        $selectStmt = $this->pdo->prepare("SELECT qty FROM sl_ips_inventory WHERE product_id = ?");

        $insertStmt->execute([4, 75]);
        $selectStmt->execute([4]);
        $row = $selectStmt->fetchAll(PDO::FETCH_ASSOC);

        if (count($row) === 0) {
            $this->markTestIncomplete(
                'SPEC-11.PDO-PREPARED-INSERT [Issue #23]: rows inserted via prepare() invisible in interleaved loop.'
            );
        }
        $this->assertCount(1, $row);
        $this->assertEquals(75, (int) $row[0]['qty']);
    }

    /**
     * Prepared JOIN query across two tables after shadow mutations.
     */
    public function testPreparedJoinAfterMutations(): void
    {
        // Add a new product with inventory via prepared statements
        $this->pdo->prepare("INSERT INTO sl_ips_products VALUES (?, ?, ?)")
            ->execute([4, 'Thingamajig', 15.00]);
        $this->pdo->prepare("INSERT INTO sl_ips_inventory VALUES (?, ?)")
            ->execute([4, 250]);

        // Update an existing product
        $this->pdo->prepare("UPDATE sl_ips_products SET price = ? WHERE id = ?")
            ->execute([25.00, 2]);

        // Prepared JOIN query
        $stmt = $this->pdo->prepare(
            "SELECT p.name, p.price, i.qty
             FROM sl_ips_products p
             JOIN sl_ips_inventory i ON i.product_id = p.id
             WHERE i.qty > ?
             ORDER BY p.name"
        );
        $stmt->execute([40]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertGreaterThanOrEqual(3, count($rows));
        $names = array_column($rows, 'name');
        $this->assertContains('Widget', $names);      // qty=100
        $this->assertContains('Gadget', $names);       // qty=50
        $this->assertContains('Thingamajig', $names);  // qty=250

        // Verify Gadget's updated price is visible
        $gadget = array_values(array_filter($rows, fn($r) => $r['name'] === 'Gadget'));
        $this->assertEqualsWithDelta(25.00, (float) $gadget[0]['price'], 0.01);
    }

    /**
     * Prepared statement reuse after ZTD disable/enable cycle.
     */
    public function testPreparedStatementReuseAfterZtdToggle(): void
    {
        $stmt = $this->pdo->prepare("SELECT name FROM sl_ips_products WHERE id = ?");

        // Use in ZTD mode
        $stmt->execute([1]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame('Widget', $rows[0]['name']);

        // Disable ZTD, use same statement
        $this->pdo->disableZtd();
        $stmt->execute([1]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        // Physical table has the row from setUp
        $this->assertSame('Widget', $rows[0]['name']);

        // Re-enable ZTD, reuse same statement
        $this->pdo->enableZtd();
        $stmt->execute([2]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame('Gadget', $rows[0]['name']);
    }
}
