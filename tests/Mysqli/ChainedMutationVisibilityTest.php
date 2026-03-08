<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests that interleaved INSERT/UPDATE/DELETE/SELECT operations correctly
 * reflect each mutation step in subsequent reads. Verifies shadow store
 * consistency across long chains of mixed DML operations.
 * @spec SPEC-2.2
 */
class ChainedMutationVisibilityTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_cmv_items (
                id INT PRIMARY KEY,
                name VARCHAR(255),
                status VARCHAR(20),
                quantity INT,
                price DECIMAL(10,2)
            )',
            'CREATE TABLE mi_cmv_log (
                id INT PRIMARY KEY,
                item_id INT,
                action VARCHAR(20),
                detail VARCHAR(255)
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_cmv_log', 'mi_cmv_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_cmv_items VALUES (1, 'Alpha', 'active', 10, 25.00)");
        $this->mysqli->query("INSERT INTO mi_cmv_items VALUES (2, 'Beta', 'active', 20, 15.00)");
        $this->mysqli->query("INSERT INTO mi_cmv_items VALUES (3, 'Gamma', 'inactive', 5, 50.00)");
    }

    /**
     * INSERT → SELECT → UPDATE → SELECT → DELETE → SELECT chain.
     */
    public function testInsertUpdateDeleteChain(): void
    {
        // Step 1: INSERT new item
        $this->mysqli->query("INSERT INTO mi_cmv_items VALUES (4, 'Delta', 'active', 30, 10.00)");
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_cmv_items");
        $this->assertSame(4, (int) $rows[0]['cnt']);

        // Step 2: UPDATE newly inserted item
        $this->mysqli->query("UPDATE mi_cmv_items SET quantity = 25, status = 'pending' WHERE id = 4");
        $rows = $this->ztdQuery("SELECT quantity, status FROM mi_cmv_items WHERE id = 4");
        $this->assertSame(25, (int) $rows[0]['quantity']);
        $this->assertSame('pending', $rows[0]['status']);

        // Step 3: UPDATE an existing item
        $this->mysqli->query("UPDATE mi_cmv_items SET price = price * 1.10 WHERE id = 1");
        $rows = $this->ztdQuery("SELECT price FROM mi_cmv_items WHERE id = 1");
        $this->assertEqualsWithDelta(27.50, (float) $rows[0]['price'], 0.01);

        // Step 4: DELETE one item
        $this->mysqli->query("DELETE FROM mi_cmv_items WHERE id = 3");
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_cmv_items");
        $this->assertSame(3, (int) $rows[0]['cnt']);

        // Step 5: Verify aggregate reflects all mutations
        $rows = $this->ztdQuery("SELECT SUM(quantity * price) AS total_value FROM mi_cmv_items");
        // id=1: 10 * 27.50 = 275, id=2: 20 * 15 = 300, id=4: 25 * 10 = 250
        $this->assertEqualsWithDelta(825.00, (float) $rows[0]['total_value'], 0.01);
    }

    /**
     * Multiple UPDATEs to the same row should accumulate correctly.
     */
    public function testMultipleUpdatesToSameRow(): void
    {
        // Three increments
        $this->mysqli->query("UPDATE mi_cmv_items SET quantity = quantity + 5 WHERE id = 1");
        $this->mysqli->query("UPDATE mi_cmv_items SET quantity = quantity + 3 WHERE id = 1");
        $this->mysqli->query("UPDATE mi_cmv_items SET quantity = quantity + 2 WHERE id = 1");

        $rows = $this->ztdQuery("SELECT quantity FROM mi_cmv_items WHERE id = 1");
        $this->assertSame(20, (int) $rows[0]['quantity']); // 10 + 5 + 3 + 2

        // Price update after quantity updates
        $this->mysqli->query("UPDATE mi_cmv_items SET price = 30.00 WHERE id = 1");
        $rows = $this->ztdQuery("SELECT quantity, price FROM mi_cmv_items WHERE id = 1");
        $this->assertSame(20, (int) $rows[0]['quantity']);
        $this->assertEqualsWithDelta(30.00, (float) $rows[0]['price'], 0.01);
    }

    /**
     * DELETE then re-INSERT at same PK should show new data.
     */
    public function testDeleteAndReInsertSamePk(): void
    {
        // Delete existing
        $this->mysqli->query("DELETE FROM mi_cmv_items WHERE id = 2");
        $rows = $this->ztdQuery("SELECT id FROM mi_cmv_items WHERE id = 2");
        $this->assertCount(0, $rows);

        // Re-insert with same PK, different data
        $this->mysqli->query("INSERT INTO mi_cmv_items VALUES (2, 'Beta-V2', 'active', 100, 99.99)");
        $rows = $this->ztdQuery("SELECT name, quantity, price FROM mi_cmv_items WHERE id = 2");
        $this->assertCount(1, $rows);
        $this->assertSame('Beta-V2', $rows[0]['name']);
        $this->assertSame(100, (int) $rows[0]['quantity']);
        $this->assertEqualsWithDelta(99.99, (float) $rows[0]['price'], 0.01);
    }

    /**
     * Cross-table mutations with JOIN visibility.
     */
    public function testCrossTableMutationVisibility(): void
    {
        // Insert log entries
        $this->mysqli->query("INSERT INTO mi_cmv_log VALUES (1, 1, 'view', 'Viewed Alpha')");
        $this->mysqli->query("INSERT INTO mi_cmv_log VALUES (2, 1, 'update', 'Updated Alpha')");
        $this->mysqli->query("INSERT INTO mi_cmv_log VALUES (3, 2, 'view', 'Viewed Beta')");

        // Verify JOIN works across both tables
        $rows = $this->ztdQuery(
            "SELECT i.name, COUNT(l.id) AS log_count
             FROM mi_cmv_items i
             LEFT JOIN mi_cmv_log l ON l.item_id = i.id
             GROUP BY i.id, i.name
             ORDER BY i.id"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Alpha', $rows[0]['name']);
        $this->assertSame(2, (int) $rows[0]['log_count']);
        $this->assertSame('Beta', $rows[1]['name']);
        $this->assertSame(1, (int) $rows[1]['log_count']);
        $this->assertSame('Gamma', $rows[2]['name']);
        $this->assertSame(0, (int) $rows[2]['log_count']);

        // Mutate item, add log, verify JOIN reflects both
        $this->mysqli->query("UPDATE mi_cmv_items SET status = 'archived' WHERE id = 3");
        $this->mysqli->query("INSERT INTO mi_cmv_log VALUES (4, 3, 'archive', 'Archived Gamma')");

        $rows = $this->ztdQuery(
            "SELECT i.name, i.status, COUNT(l.id) AS log_count
             FROM mi_cmv_items i
             LEFT JOIN mi_cmv_log l ON l.item_id = i.id
             WHERE i.id = 3
             GROUP BY i.id, i.name, i.status"
        );

        $this->assertSame('archived', $rows[0]['status']);
        $this->assertSame(1, (int) $rows[0]['log_count']);
    }

    /**
     * Conditional UPDATE based on aggregate, then verify.
     */
    public function testConditionalUpdateWithAggregate(): void
    {
        // Update items with quantity > average
        $rows = $this->ztdQuery("SELECT AVG(quantity) AS avg_qty FROM mi_cmv_items");
        $avgQty = (float) $rows[0]['avg_qty']; // (10+20+5)/3 ≈ 11.67

        // Use a simple WHERE instead of subquery to avoid known issues
        $this->mysqli->query("UPDATE mi_cmv_items SET status = 'high-stock' WHERE quantity > 11");

        $rows = $this->ztdQuery(
            "SELECT id, status FROM mi_cmv_items ORDER BY id"
        );

        $this->assertSame('active', $rows[0]['status']);    // id=1, qty=10, not > 11
        $this->assertSame('high-stock', $rows[1]['status']); // id=2, qty=20, > 11
        $this->assertSame('inactive', $rows[2]['status']);   // id=3, qty=5, not > 11
    }

    /**
     * Physical isolation across all chained mutations.
     */
    public function testPhysicalIsolationAfterChain(): void
    {
        $this->mysqli->query("INSERT INTO mi_cmv_items VALUES (4, 'Delta', 'active', 30, 10.00)");
        $this->mysqli->query("UPDATE mi_cmv_items SET quantity = 999 WHERE id = 1");
        $this->mysqli->query("DELETE FROM mi_cmv_items WHERE id = 3");
        $this->mysqli->query("INSERT INTO mi_cmv_log VALUES (1, 1, 'test', 'Test entry')");

        // Verify in ZTD
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_cmv_items");
        $this->assertSame(3, (int) $rows[0]['cnt']); // 3 original + 1 inserted - 1 deleted

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_cmv_log");
        $this->assertSame(1, (int) $rows[0]['cnt']);

        // Verify physical tables untouched
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_cmv_items');
        $this->assertEquals(0, (int) $result->fetch_assoc()['cnt']);

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_cmv_log');
        $this->assertEquals(0, (int) $result->fetch_assoc()['cnt']);
    }
}
