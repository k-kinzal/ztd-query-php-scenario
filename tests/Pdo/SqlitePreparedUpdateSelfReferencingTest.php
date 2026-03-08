<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests prepared UPDATE with self-referencing arithmetic on SQLite.
 *
 * SET col = col + ? via prepared statements ensures parameter binding
 * works correctly with self-referencing expressions.
 */
class SqlitePreparedUpdateSelfReferencingTest extends TestCase
{
    private ZtdPdo $pdo;

    protected function setUp(): void
    {
        $raw = new PDO('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        $raw->exec('CREATE TABLE sl_pupd_test (id INTEGER PRIMARY KEY, name TEXT, counter INTEGER, balance REAL)');
        $this->pdo = ZtdPdo::fromPdo($raw);

        $this->pdo->exec("INSERT INTO sl_pupd_test VALUES (1, 'Alice', 10, 100.00)");
        $this->pdo->exec("INSERT INTO sl_pupd_test VALUES (2, 'Bob', 20, 200.00)");
        $this->pdo->exec("INSERT INTO sl_pupd_test VALUES (3, 'Charlie', 30, 300.00)");
    }

    /**
     * Prepared SET col = col + ? with parameter.
     */
    public function testPreparedIncrementWithParam(): void
    {
        $stmt = $this->pdo->prepare('UPDATE sl_pupd_test SET counter = counter + ? WHERE id = ?');
        $stmt->execute([5, 1]);

        $qstmt = $this->pdo->query('SELECT counter FROM sl_pupd_test WHERE id = 1');
        $this->assertSame(15, (int) $qstmt->fetchColumn());
    }

    /**
     * Prepared decrement.
     */
    public function testPreparedDecrementWithParam(): void
    {
        $stmt = $this->pdo->prepare('UPDATE sl_pupd_test SET balance = balance - ? WHERE id = ?');
        $stmt->execute([25.50, 2]);

        $qstmt = $this->pdo->query('SELECT balance FROM sl_pupd_test WHERE id = 2');
        $this->assertEqualsWithDelta(174.50, (float) $qstmt->fetchColumn(), 0.01);
    }

    /**
     * Prepared self-referencing update on all rows matching WHERE.
     */
    public function testPreparedUpdateAllMatching(): void
    {
        $stmt = $this->pdo->prepare('UPDATE sl_pupd_test SET counter = counter + ? WHERE counter >= ?');
        $stmt->execute([100, 20]);

        $qstmt = $this->pdo->query('SELECT counter FROM sl_pupd_test ORDER BY id');
        $rows = $qstmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertSame(10, (int) $rows[0]); // Alice: not >= 20
        $this->assertSame(120, (int) $rows[1]); // Bob: 20 + 100
        $this->assertSame(130, (int) $rows[2]); // Charlie: 30 + 100
    }

    /**
     * Sequential prepared updates accumulate.
     */
    public function testSequentialPreparedUpdates(): void
    {
        $stmt = $this->pdo->prepare('UPDATE sl_pupd_test SET counter = counter + ? WHERE id = ?');
        $stmt->execute([1, 1]);

        // New prepare call needed to see updated shadow state
        $stmt2 = $this->pdo->prepare('UPDATE sl_pupd_test SET counter = counter + ? WHERE id = ?');
        $stmt2->execute([1, 1]);

        $qstmt = $this->pdo->query('SELECT counter FROM sl_pupd_test WHERE id = 1');
        $this->assertSame(12, (int) $qstmt->fetchColumn()); // 10 + 1 + 1
    }

    /**
     * Prepared self-referencing after INSERT.
     */
    public function testPreparedUpdateAfterInsert(): void
    {
        $this->pdo->exec("INSERT INTO sl_pupd_test VALUES (4, 'Diana', 0, 0.00)");

        $stmt = $this->pdo->prepare('UPDATE sl_pupd_test SET counter = counter + ? WHERE id = ?');
        $stmt->execute([50, 4]);

        $qstmt = $this->pdo->query('SELECT counter FROM sl_pupd_test WHERE id = 4');
        $this->assertSame(50, (int) $qstmt->fetchColumn());
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $stmt = $this->pdo->prepare('UPDATE sl_pupd_test SET counter = counter + ? WHERE id = ?');
        $stmt->execute([999, 1]);

        $this->pdo->disableZtd();
        $qstmt = $this->pdo->query('SELECT COUNT(*) FROM sl_pupd_test');
        $this->assertSame(0, (int) $qstmt->fetchColumn());
    }
}
