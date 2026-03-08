<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests CHECK constraint behavior with ZTD shadow store on MySQL.
 *
 * MySQL 8.0.16+ supports CHECK constraints. Since ZTD rewrites operations
 * to CTE-based queries, CHECK constraints are NOT enforced in shadow.
 * @spec SPEC-8.1
 */
class MysqlCheckConstraintBehaviorTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pdo_check_test (
            id INT PRIMARY KEY,
            age INT,
            score INT,
            status VARCHAR(20),
            CONSTRAINT chk_age CHECK (age >= 0 AND age <= 150),
            CONSTRAINT chk_score CHECK (score >= 0),
            CONSTRAINT chk_status CHECK (status IN (\\\'active\\\', \\\'inactive\\\', \\\'pending\\\'))
        )';
    }

    protected function getTableNames(): array
    {
        return ['pdo_check_test'];
    }


    /**
     * INSERT with valid values succeeds.
     */
    public function testInsertWithValidValues(): void
    {
        $this->pdo->exec("INSERT INTO pdo_check_test VALUES (1, 25, 100, 'active')");

        $stmt = $this->pdo->query('SELECT * FROM pdo_check_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(25, (int) $row['age']);
        $this->assertSame('active', $row['status']);
    }

    /**
     * INSERT violating CHECK succeeds in shadow.
     */
    public function testInsertViolatingCheckSucceedsInShadow(): void
    {
        $this->pdo->exec("INSERT INTO pdo_check_test VALUES (1, -1, 100, 'active')");

        $stmt = $this->pdo->query('SELECT age FROM pdo_check_test WHERE id = 1');
        $this->assertSame(-1, (int) $stmt->fetchColumn());
    }

    /**
     * INSERT with invalid status succeeds in shadow.
     */
    public function testInsertInvalidStatusSucceeds(): void
    {
        $this->pdo->exec("INSERT INTO pdo_check_test VALUES (1, 25, 100, 'invalid')");

        $stmt = $this->pdo->query('SELECT status FROM pdo_check_test WHERE id = 1');
        $this->assertSame('invalid', $stmt->fetchColumn());
    }

    /**
     * UPDATE violating CHECK succeeds in shadow.
     */
    public function testUpdateViolatingCheckSucceeds(): void
    {
        $this->pdo->exec("INSERT INTO pdo_check_test VALUES (1, 25, 100, 'active')");
        $this->pdo->exec('UPDATE pdo_check_test SET age = 200 WHERE id = 1');

        $stmt = $this->pdo->query('SELECT age FROM pdo_check_test WHERE id = 1');
        $this->assertSame(200, (int) $stmt->fetchColumn());
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO pdo_check_test VALUES (1, -1, -999, 'bad')");

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_check_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
