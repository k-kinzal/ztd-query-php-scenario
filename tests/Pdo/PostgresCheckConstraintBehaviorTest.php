<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests CHECK constraint behavior with ZTD shadow store on PostgreSQL.
 *
 * PostgreSQL has full CHECK constraint support. Since ZTD rewrites
 * operations to CTE-based queries, CHECK constraints are NOT enforced
 * in shadow.
 * @spec SPEC-8.1
 */
class PostgresCheckConstraintBehaviorTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pg_check_test (
            id INT PRIMARY KEY,
            age INT CHECK (age >= 0 AND age <= 150),
            score INT CHECK (score >= 0),
            status VARCHAR(20) CHECK (status IN (\'active\', \'inactive\', \'pending\'))
        )';
    }

    protected function getTableNames(): array
    {
        return ['pg_check_test'];
    }


    /**
     * INSERT with valid values succeeds.
     */
    public function testInsertWithValidValues(): void
    {
        $this->pdo->exec("INSERT INTO pg_check_test VALUES (1, 25, 100, 'active')");

        $stmt = $this->pdo->query('SELECT age, status FROM pg_check_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(25, (int) $row['age']);
        $this->assertSame('active', $row['status']);
    }

    /**
     * INSERT violating CHECK succeeds in shadow.
     */
    public function testInsertViolatingCheckSucceedsInShadow(): void
    {
        $this->pdo->exec("INSERT INTO pg_check_test VALUES (1, -1, 100, 'active')");

        $stmt = $this->pdo->query('SELECT age FROM pg_check_test WHERE id = 1');
        $this->assertSame(-1, (int) $stmt->fetchColumn());
    }

    /**
     * INSERT with invalid status succeeds in shadow.
     */
    public function testInsertInvalidStatusSucceeds(): void
    {
        $this->pdo->exec("INSERT INTO pg_check_test VALUES (1, 25, 100, 'invalid')");

        $stmt = $this->pdo->query('SELECT status FROM pg_check_test WHERE id = 1');
        $this->assertSame('invalid', $stmt->fetchColumn());
    }

    /**
     * UPDATE violating CHECK succeeds in shadow.
     */
    public function testUpdateViolatingCheckSucceeds(): void
    {
        $this->pdo->exec("INSERT INTO pg_check_test VALUES (1, 25, 100, 'active')");
        $this->pdo->exec('UPDATE pg_check_test SET age = 200 WHERE id = 1');

        $stmt = $this->pdo->query('SELECT age FROM pg_check_test WHERE id = 1');
        $this->assertSame(200, (int) $stmt->fetchColumn());
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO pg_check_test VALUES (1, -1, -999, 'bad')");

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_check_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
