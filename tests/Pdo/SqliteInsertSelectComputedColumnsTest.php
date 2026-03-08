<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests INSERT...SELECT with computed/aggregated columns on SQLite.
 *
 * On SQLite, computed columns become NULL in INSERT...SELECT.
 * Direct column references transfer correctly.
 * @spec pending
 */
class SqliteInsertSelectComputedColumnsTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE iscc_src (id INT PRIMARY KEY, name VARCHAR(50), price DECIMAL(10,2))',
            'CREATE TABLE iscc_dst (id INT PRIMARY KEY, name VARCHAR(50), price DECIMAL(10,2))',
            'CREATE TABLE iscc_agg (category VARCHAR(50) PRIMARY KEY, total DECIMAL(10,2))',
        ];
    }

    protected function getTableNames(): array
    {
        return ['iscc_src', 'iscc_dst', 'iscc_agg'];
    }


    /**
     * INSERT...SELECT with direct column references works.
     */
    public function testInsertSelectDirectColumns(): void
    {
        $this->pdo->exec('INSERT INTO iscc_dst SELECT id, name, price FROM iscc_src');

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM iscc_dst');
        $this->assertSame(3, (int) $stmt->fetchColumn());

        $stmt = $this->pdo->query('SELECT name FROM iscc_dst WHERE id = 1');
        $this->assertSame('Widget', $stmt->fetchColumn());
    }

    /**
     * INSERT...SELECT with computed column: price * 2 becomes NULL on SQLite.
     */
    public function testInsertSelectComputedColumnBecomesNull(): void
    {
        $this->pdo->exec('INSERT INTO iscc_dst SELECT id, name, price * 2 FROM iscc_src WHERE id = 1');

        $stmt = $this->pdo->query('SELECT price FROM iscc_dst WHERE id = 1');
        $price = $stmt->fetchColumn();
        // Computed column becomes NULL on SQLite
        $this->assertNull($price);
    }

    /**
     * INSERT...SELECT with filtered WHERE works (correct count).
     */
    public function testInsertSelectWithFilteredWhere(): void
    {
        $this->pdo->exec('INSERT INTO iscc_dst SELECT id, name, price FROM iscc_src WHERE price > 15');

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM iscc_dst');
        $this->assertSame(2, (int) $stmt->fetchColumn());
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec('INSERT INTO iscc_dst SELECT id, name, price FROM iscc_src');

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM iscc_dst');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
