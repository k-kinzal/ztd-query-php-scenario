<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests CHECK constraint behavior with ZTD shadow store on SQLite.
 *
 * CHECK constraints are defined at the database level. Since ZTD
 * rewrites INSERT/UPDATE to CTE-based operations, CHECK constraints
 * are NOT enforced in the shadow store (the physical table is never
 * modified). This documents that behavior.
 * @spec SPEC-8.1
 */
class SqliteCheckConstraintBehaviorTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_check_test (
            id INTEGER PRIMARY KEY,
            age INTEGER CHECK(age >= 0 AND age <= 150),
            score INTEGER CHECK(score >= 0),
            status TEXT CHECK(status IN (\\\'active\\\', \\\'inactive\\\', \\\'pending\\\'))
        )';
    }

    protected function getTableNames(): array
    {
        return ['sl_check_test'];
    }


    /**
     * INSERT with valid CHECK constraint values succeeds.
     */
    public function testInsertWithValidValues(): void
    {
        $this->pdo->exec("INSERT INTO sl_check_test VALUES (1, 25, 100, 'active')");

        $stmt = $this->pdo->query('SELECT * FROM sl_check_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(25, (int) $row['age']);
        $this->assertSame(100, (int) $row['score']);
        $this->assertSame('active', $row['status']);
    }

    /**
     * INSERT violating CHECK constraint — shadow store does NOT enforce CHECK.
     *
     * Since the physical INSERT never happens, the CHECK constraint
     * is not triggered. The invalid value is stored in shadow.
     */
    public function testInsertViolatingCheckConstraintSucceedsInShadow(): void
    {
        // age = -1 violates CHECK(age >= 0), but shadow allows it
        $this->pdo->exec("INSERT INTO sl_check_test VALUES (1, -1, 100, 'active')");

        $stmt = $this->pdo->query('SELECT age FROM sl_check_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(-1, (int) $row['age']);
    }

    /**
     * INSERT with invalid status value — not enforced in shadow.
     */
    public function testInsertInvalidStatusSucceedsInShadow(): void
    {
        $this->pdo->exec("INSERT INTO sl_check_test VALUES (1, 25, 100, 'invalid_status')");

        $stmt = $this->pdo->query('SELECT status FROM sl_check_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('invalid_status', $row['status']);
    }

    /**
     * INSERT with negative score — not enforced in shadow.
     */
    public function testInsertNegativeScoreSucceedsInShadow(): void
    {
        $this->pdo->exec("INSERT INTO sl_check_test VALUES (1, 25, -999, 'active')");

        $stmt = $this->pdo->query('SELECT score FROM sl_check_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(-999, (int) $row['score']);
    }

    /**
     * UPDATE violating CHECK — not enforced in shadow.
     */
    public function testUpdateViolatingCheckSucceedsInShadow(): void
    {
        $this->pdo->exec("INSERT INTO sl_check_test VALUES (1, 25, 100, 'active')");
        $this->pdo->exec('UPDATE sl_check_test SET age = 200 WHERE id = 1'); // > 150

        $stmt = $this->pdo->query('SELECT age FROM sl_check_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(200, (int) $row['age']);
    }

    /**
     * Multiple rows with mixed valid/invalid CHECK values.
     */
    public function testMultipleRowsMixedCheckValidity(): void
    {
        $this->pdo->exec("INSERT INTO sl_check_test VALUES (1, 25, 100, 'active')");   // valid
        $this->pdo->exec("INSERT INTO sl_check_test VALUES (2, -5, -10, 'unknown')");  // all invalid
        $this->pdo->exec("INSERT INTO sl_check_test VALUES (3, 30, 50, 'pending')");   // valid

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM sl_check_test');
        $this->assertSame(3, (int) $stmt->fetchColumn());
    }

    /**
     * Physical isolation — invalid values never reach physical table.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO sl_check_test VALUES (1, -1, -999, 'bad')");

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM sl_check_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
