<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests prepared pagination (LIMIT/OFFSET) combined with shadow mutations on SQLite.
 *
 * Ensures that prepared SELECT with parameterized LIMIT/OFFSET correctly
 * reflects shadow state changes across multiple pages.
 */
class SqlitePreparedPaginationAfterMutationTest extends TestCase
{
    private ZtdPdo $pdo;

    protected function setUp(): void
    {
        $raw = new PDO('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        $raw->exec('CREATE TABLE sl_ppag_test (id INTEGER PRIMARY KEY, name TEXT, category TEXT)');
        $this->pdo = ZtdPdo::fromPdo($raw);

        for ($i = 1; $i <= 10; $i++) {
            $cat = $i <= 5 ? 'A' : 'B';
            $this->pdo->exec("INSERT INTO sl_ppag_test VALUES ($i, 'Item$i', '$cat')");
        }
    }

    /**
     * Parameterized LIMIT and OFFSET.
     */
    public function testPreparedLimitOffset(): void
    {
        $stmt = $this->pdo->prepare('SELECT name FROM sl_ppag_test ORDER BY id LIMIT ? OFFSET ?');
        $stmt->execute([3, 0]);
        $page1 = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertCount(3, $page1);
        $this->assertSame('Item1', $page1[0]);
    }

    /**
     * Pagination after INSERT reflects new row.
     */
    public function testPaginationAfterInsert(): void
    {
        $this->pdo->exec("INSERT INTO sl_ppag_test VALUES (11, 'Item11', 'A')");

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM sl_ppag_test');
        $this->assertSame(11, (int) $stmt->fetchColumn());

        // Last page should include new row
        $stmt = $this->pdo->prepare('SELECT name FROM sl_ppag_test ORDER BY id LIMIT ? OFFSET ?');
        $stmt->execute([3, 9]);
        $page = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertCount(2, $page);
        $this->assertSame('Item10', $page[0]);
        $this->assertSame('Item11', $page[1]);
    }

    /**
     * Pagination after DELETE reduces total pages.
     */
    public function testPaginationAfterDelete(): void
    {
        $this->pdo->exec("DELETE FROM sl_ppag_test WHERE category = 'B'");

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM sl_ppag_test');
        $this->assertSame(5, (int) $stmt->fetchColumn());

        $stmt = $this->pdo->prepare('SELECT name FROM sl_ppag_test ORDER BY id LIMIT ? OFFSET ?');
        $stmt->execute([3, 3]);
        $page = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertCount(2, $page); // Only 2 remain on page 2
    }

    /**
     * Pagination with WHERE filter and mutations.
     */
    public function testPaginatedFilterAfterUpdate(): void
    {
        $this->pdo->exec("UPDATE sl_ppag_test SET category = 'A' WHERE id = 6");

        $stmt = $this->pdo->prepare("SELECT name FROM sl_ppag_test WHERE category = 'A' ORDER BY id LIMIT ? OFFSET ?");
        $stmt->execute([3, 0]);
        $page1 = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertCount(3, $page1);

        $stmt2 = $this->pdo->prepare("SELECT name FROM sl_ppag_test WHERE category = 'A' ORDER BY id LIMIT ? OFFSET ?");
        $stmt2->execute([3, 3]);
        $page2 = $stmt2->fetchAll(PDO::FETCH_COLUMN);
        $this->assertCount(3, $page2); // 5 original A + 1 updated = 6 total, 3 on page 2
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM sl_ppag_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
