<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests a gift card redemption workflow through ZTD shadow store (MySQL PDO).
 * Covers prepare-once/execute-many pattern, self-referencing arithmetic in UPDATE,
 * transaction history, expiry logic, and physical isolation.
 * @spec SPEC-10.2.77
 */
class MysqlGiftCardRedemptionTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mp_gc_cards (
                id INT PRIMARY KEY,
                code VARCHAR(50),
                initial_balance DECIMAL(10,2),
                current_balance DECIMAL(10,2),
                status VARCHAR(20)
            )',
            'CREATE TABLE mp_gc_transactions (
                id INT PRIMARY KEY,
                card_id INT,
                amount DECIMAL(10,2),
                transaction_type VARCHAR(20),
                created_at DATETIME
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mp_gc_transactions', 'mp_gc_cards'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO mp_gc_cards VALUES (1, 'GC-100', 100.00, 75.50, 'active')");
        $this->pdo->exec("INSERT INTO mp_gc_cards VALUES (2, 'GC-200', 200.00, 200.00, 'active')");
        $this->pdo->exec("INSERT INTO mp_gc_cards VALUES (3, 'GC-050', 50.00, 0.00, 'depleted')");

        $this->pdo->exec("INSERT INTO mp_gc_transactions VALUES (1, 1, 10.00, 'redemption', '2026-01-15 10:00:00')");
        $this->pdo->exec("INSERT INTO mp_gc_transactions VALUES (2, 1, 14.50, 'redemption', '2026-02-01 14:30:00')");
        $this->pdo->exec("INSERT INTO mp_gc_transactions VALUES (3, 3, 50.00, 'redemption', '2026-01-20 09:00:00')");
    }

    /**
     * Check balance for multiple cards using the same prepared statement executed multiple times.
     */
    public function testCheckBalance(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT code, current_balance FROM mp_gc_cards WHERE code = ?",
            ['GC-100']
        );
        $this->assertCount(1, $rows);
        $this->assertEqualsWithDelta(75.50, (float) $rows[0]['current_balance'], 0.01);

        $rows = $this->ztdPrepareAndExecute(
            "SELECT code, current_balance FROM mp_gc_cards WHERE code = ?",
            ['GC-200']
        );
        $this->assertCount(1, $rows);
        $this->assertEqualsWithDelta(200.00, (float) $rows[0]['current_balance'], 0.01);

        $rows = $this->ztdPrepareAndExecute(
            "SELECT code, current_balance FROM mp_gc_cards WHERE code = ?",
            ['GC-050']
        );
        $this->assertCount(1, $rows);
        $this->assertEqualsWithDelta(0.00, (float) $rows[0]['current_balance'], 0.01);
    }

    /**
     * Redeem a card: subtract from balance using self-referencing UPDATE,
     * then INSERT a transaction record.
     */
    public function testRedeemCard(): void
    {
        $affected = $this->pdo->exec("UPDATE mp_gc_cards SET current_balance = current_balance - 25.50 WHERE id = 1 AND current_balance >= 25.50");
        $this->assertSame(1, $affected);

        $this->pdo->exec("INSERT INTO mp_gc_transactions VALUES (4, 1, 25.50, 'redemption', '2026-03-09 12:00:00')");

        $rows = $this->ztdQuery("SELECT current_balance FROM mp_gc_cards WHERE id = 1");
        $this->assertEqualsWithDelta(50.00, (float) $rows[0]['current_balance'], 0.01);

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mp_gc_transactions WHERE card_id = 1");
        $this->assertEquals(3, (int) $rows[0]['cnt']);
    }

    /**
     * Reload a card: add to balance using self-referencing UPDATE,
     * then INSERT a reload transaction.
     */
    public function testReloadCard(): void
    {
        $affected = $this->pdo->exec("UPDATE mp_gc_cards SET current_balance = current_balance + 50.00 WHERE id = 1");
        $this->assertSame(1, $affected);

        $this->pdo->exec("INSERT INTO mp_gc_transactions VALUES (4, 1, 50.00, 'reload', '2026-03-09 13:00:00')");

        $rows = $this->ztdQuery("SELECT current_balance FROM mp_gc_cards WHERE id = 1");
        $this->assertEqualsWithDelta(125.50, (float) $rows[0]['current_balance'], 0.01);

        $rows = $this->ztdQuery("SELECT transaction_type, amount FROM mp_gc_transactions WHERE id = 4");
        $this->assertSame('reload', $rows[0]['transaction_type']);
        $this->assertEqualsWithDelta(50.00, (float) $rows[0]['amount'], 0.01);
    }

    /**
     * Retrieve transaction history via JOIN, ordered by created_at DESC, using prepared card code.
     */
    public function testTransactionHistory(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT c.code, t.amount, t.transaction_type, t.created_at
             FROM mp_gc_transactions t
             JOIN mp_gc_cards c ON c.id = t.card_id
             WHERE c.code = ?
             ORDER BY t.created_at DESC",
            ['GC-100']
        );

        $this->assertCount(2, $rows);
        $this->assertSame('GC-100', $rows[0]['code']);
        $this->assertEqualsWithDelta(14.50, (float) $rows[0]['amount'], 0.01);
        $this->assertEqualsWithDelta(10.00, (float) $rows[1]['amount'], 0.01);
    }

    /**
     * Expire unused cards: UPDATE status to 'expired' where balance equals initial balance.
     */
    public function testExpireUnusedCards(): void
    {
        $affected = $this->pdo->exec("UPDATE mp_gc_cards SET status = 'expired' WHERE current_balance = initial_balance AND status = 'active'");
        $this->assertSame(1, $affected);

        $rows = $this->ztdQuery("SELECT code, status FROM mp_gc_cards WHERE id = 2");
        $this->assertSame('expired', $rows[0]['status']);

        $rows = $this->ztdQuery("SELECT status FROM mp_gc_cards WHERE id = 1");
        $this->assertSame('active', $rows[0]['status']);
    }

    /**
     * Balance summary report: SUM grouped by status with CASE for balance ranges.
     */
    public function testBalanceSummaryReport(): void
    {
        $rows = $this->ztdQuery(
            "SELECT status,
                    COUNT(*) AS card_count,
                    SUM(current_balance) AS total_balance,
                    SUM(CASE WHEN current_balance > 0 THEN 1 ELSE 0 END) AS cards_with_balance
             FROM mp_gc_cards
             GROUP BY status
             ORDER BY status"
        );

        $this->assertCount(2, $rows);

        $this->assertSame('active', $rows[0]['status']);
        $this->assertEquals(2, (int) $rows[0]['card_count']);
        $this->assertEqualsWithDelta(275.50, (float) $rows[0]['total_balance'], 0.01);
        $this->assertEquals(2, (int) $rows[0]['cards_with_balance']);

        $this->assertSame('depleted', $rows[1]['status']);
        $this->assertEquals(1, (int) $rows[1]['card_count']);
        $this->assertEqualsWithDelta(0.00, (float) $rows[1]['total_balance'], 0.01);
        $this->assertEquals(0, (int) $rows[1]['cards_with_balance']);
    }

    /**
     * Physical isolation: shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO mp_gc_transactions VALUES (4, 1, 5.00, 'redemption', '2026-03-09 15:00:00')");
        $this->pdo->exec("UPDATE mp_gc_cards SET current_balance = current_balance - 5.00 WHERE id = 1");

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mp_gc_transactions");
        $this->assertSame(4, (int) $rows[0]['cnt']);

        $rows = $this->ztdQuery("SELECT current_balance FROM mp_gc_cards WHERE id = 1");
        $this->assertEqualsWithDelta(70.50, (float) $rows[0]['current_balance'], 0.01);

        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM mp_gc_transactions")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt']);
    }
}
