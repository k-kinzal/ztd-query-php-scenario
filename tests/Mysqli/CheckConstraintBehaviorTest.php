<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests CHECK constraint behavior with ZTD shadow store via MySQLi.
 *
 * Cross-platform parity with MysqlCheckConstraintBehaviorTest (PDO).
 * CHECK constraints are NOT enforced in shadow.
 * @spec SPEC-8.1
 */
class CheckConstraintBehaviorTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_check_test (
            id INT PRIMARY KEY,
            age INT,
            score INT,
            status VARCHAR(20),
            CONSTRAINT mi_chk_age CHECK (age >= 0 AND age <= 150),
            CONSTRAINT mi_chk_score CHECK (score >= 0),
            CONSTRAINT mi_chk_status CHECK (status IN (\'active\', \'inactive\', \'pending\'))
        )';
    }

    protected function getTableNames(): array
    {
        return ['mi_check_test'];
    }


    /**
     * INSERT with valid values succeeds.
     */
    public function testInsertWithValidValues(): void
    {
        $this->mysqli->query("INSERT INTO mi_check_test VALUES (1, 25, 100, 'active')");

        $result = $this->mysqli->query('SELECT age, status FROM mi_check_test WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertSame(25, (int) $row['age']);
        $this->assertSame('active', $row['status']);
    }

    /**
     * INSERT violating CHECK succeeds in shadow.
     */
    public function testInsertViolatingCheckSucceeds(): void
    {
        $this->mysqli->query("INSERT INTO mi_check_test VALUES (1, -1, 100, 'active')");

        $result = $this->mysqli->query('SELECT age FROM mi_check_test WHERE id = 1');
        $this->assertSame(-1, (int) $result->fetch_assoc()['age']);
    }

    /**
     * UPDATE violating CHECK succeeds in shadow.
     */
    public function testUpdateViolatingCheckSucceeds(): void
    {
        $this->mysqli->query("INSERT INTO mi_check_test VALUES (1, 25, 100, 'active')");
        $this->mysqli->query('UPDATE mi_check_test SET age = 200 WHERE id = 1');

        $result = $this->mysqli->query('SELECT age FROM mi_check_test WHERE id = 1');
        $this->assertSame(200, (int) $result->fetch_assoc()['age']);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->query("INSERT INTO mi_check_test VALUES (1, -1, -999, 'bad')");

        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_check_test');
        $this->assertSame(0, (int) $result->fetch_assoc()['cnt']);
    }
}
