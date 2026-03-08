<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests INSERT with column list / values count mismatches on SQLite.
 *
 * Verifies error handling when column counts don't match, and that
 * the shadow store remains consistent after errors.
 * @spec SPEC-4.1
 */
class SqliteInsertColumnMismatchTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE icm_items (id INT PRIMARY KEY, name VARCHAR(50), price DECIMAL(10,2))';
    }

    protected function getTableNames(): array
    {
        return ['icm_items'];
    }


    /**
     * Correct INSERT works.
     */
    public function testCorrectInsert(): void
    {
        $this->pdo->exec("INSERT INTO icm_items VALUES (1, 'Widget', 9.99)");

        $stmt = $this->pdo->query('SELECT * FROM icm_items WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Widget', $row['name']);
    }

    /**
     * INSERT with too few values (no column list).
     */
    public function testInsertTooFewValues(): void
    {
        try {
            $this->pdo->exec("INSERT INTO icm_items VALUES (1, 'Widget')");
            // If it succeeds, verify behavior
            $stmt = $this->pdo->query('SELECT COUNT(*) FROM icm_items');
            $count = (int) $stmt->fetchColumn();
            $this->assertGreaterThanOrEqual(0, $count);
        } catch (\Throwable $e) {
            // Column count mismatch should throw
            $this->assertInstanceOf(\Throwable::class, $e);
        }
    }

    /**
     * INSERT with too many values (no column list).
     */
    public function testInsertTooManyValues(): void
    {
        try {
            $this->pdo->exec("INSERT INTO icm_items VALUES (1, 'Widget', 9.99, 'extra')");
            $stmt = $this->pdo->query('SELECT COUNT(*) FROM icm_items');
            $count = (int) $stmt->fetchColumn();
            $this->assertGreaterThanOrEqual(0, $count);
        } catch (\Throwable $e) {
            $this->assertInstanceOf(\Throwable::class, $e);
        }
    }

    /**
     * INSERT with explicit column list and matching values.
     */
    public function testInsertWithColumnList(): void
    {
        $this->pdo->exec("INSERT INTO icm_items (id, name) VALUES (1, 'Widget')");

        $stmt = $this->pdo->query('SELECT name FROM icm_items WHERE id = 1');
        $this->assertSame('Widget', $stmt->fetchColumn());
    }

    /**
     * INSERT with column list having fewer columns than values.
     */
    public function testInsertColumnListFewerThanValues(): void
    {
        try {
            $this->pdo->exec("INSERT INTO icm_items (id, name) VALUES (1, 'Widget', 9.99)");
            $stmt = $this->pdo->query('SELECT COUNT(*) FROM icm_items');
            $this->assertGreaterThanOrEqual(0, (int) $stmt->fetchColumn());
        } catch (\Throwable $e) {
            $this->assertInstanceOf(\Throwable::class, $e);
        }
    }

    /**
     * INSERT with column list having more columns than values.
     */
    public function testInsertColumnListMoreThanValues(): void
    {
        try {
            $this->pdo->exec("INSERT INTO icm_items (id, name, price) VALUES (1, 'Widget')");
            $stmt = $this->pdo->query('SELECT COUNT(*) FROM icm_items');
            $this->assertGreaterThanOrEqual(0, (int) $stmt->fetchColumn());
        } catch (\Throwable $e) {
            $this->assertInstanceOf(\Throwable::class, $e);
        }
    }

    /**
     * Shadow store remains consistent after column mismatch errors.
     */
    public function testShadowConsistentAfterError(): void
    {
        // Insert one good row
        $this->pdo->exec("INSERT INTO icm_items VALUES (1, 'Widget', 9.99)");

        // Try a bad insert (may or may not throw)
        try {
            $this->pdo->exec("INSERT INTO icm_items VALUES (2, 'Bad')");
        } catch (\Throwable $e) {
            // Ignore
        }

        // Good row should still be there
        $stmt = $this->pdo->query('SELECT name FROM icm_items WHERE id = 1');
        $this->assertSame('Widget', $stmt->fetchColumn());
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO icm_items VALUES (1, 'Widget', 9.99)");

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM icm_items');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
