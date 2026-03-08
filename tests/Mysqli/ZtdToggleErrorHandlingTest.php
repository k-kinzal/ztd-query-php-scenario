<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests error handling across ZTD toggle boundaries on MySQLi:
 * - Errors when ZTD is enabled vs disabled
 * - State consistency after toggle + error
 * - Shadow data visibility vs physical data across toggles
 * @spec SPEC-2.1, SPEC-2.2, SPEC-2.3
 */
class ZtdToggleErrorHandlingTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_zte_users (id INT PRIMARY KEY, name VARCHAR(50))';
    }

    protected function getTableNames(): array
    {
        return ['mi_zte_users'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_zte_users VALUES (1, 'Alice')");
        $this->mysqli->query("INSERT INTO mi_zte_users VALUES (2, 'Bob')");
    }

    public function testErrorWithZtdEnabledDoesNotCorruptShadow(): void
    {
        try {
            $this->mysqli->query('SELECT * FROM nonexistent_xyz');
        } catch (\RuntimeException $e) {
            // Expected
        }

        $result = $this->mysqli->query('SELECT name FROM mi_zte_users WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertSame('Alice', $row['name']);
    }

    public function testErrorWithZtdDisabledThenReEnable(): void
    {
        $this->mysqli->disableZtd();

        try {
            $this->mysqli->query('SELECT * FROM nonexistent_xyz');
        } catch (\RuntimeException $e) {
            // Expected
        }

        $this->mysqli->enableZtd();

        $result = $this->mysqli->query('SELECT name FROM mi_zte_users WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertSame('Alice', $row['name']);
    }

    public function testToggleAfterErrorPreservesState(): void
    {
        $this->mysqli->query("INSERT INTO mi_zte_users VALUES (3, 'Charlie')");

        try {
            $this->mysqli->query('INSERT INTO nonexistent_xyz VALUES (1)');
        } catch (\RuntimeException $e) {
            // Expected
        }

        $this->mysqli->disableZtd();
        $this->mysqli->enableZtd();

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_zte_users');
        $row = $result->fetch_assoc();
        $this->assertSame(3, (int) $row['cnt']);
    }

    public function testShadowInsertNotVisibleWhenDisabled(): void
    {
        $this->mysqli->query("INSERT INTO mi_zte_users VALUES (4, 'ShadowUser')");

        // ZTD sees all shadow rows
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_zte_users');
        $row = $result->fetch_assoc();
        $this->assertSame(3, (int) $row['cnt']);

        // Disable ZTD — queries go to physical DB (no rows, all writes were shadow-only)
        $this->mysqli->disableZtd();

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_zte_users');
        $row = $result->fetch_assoc();
        $this->assertSame(0, (int) $row['cnt']);
    }

    public function testMultipleToggleCyclesAccumulateShadow(): void
    {
        // Cycle 1
        $this->mysqli->query("INSERT INTO mi_zte_users VALUES (3, 'Charlie')");
        $this->mysqli->disableZtd();
        $this->mysqli->enableZtd();

        // Cycle 2
        $this->mysqli->query("INSERT INTO mi_zte_users VALUES (4, 'Diana')");

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_zte_users');
        $row = $result->fetch_assoc();
        $this->assertSame(4, (int) $row['cnt']);

        // Cycle 3
        $this->mysqli->disableZtd();
        $this->mysqli->enableZtd();

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_zte_users');
        $row = $result->fetch_assoc();
        $this->assertSame(4, (int) $row['cnt']);
    }

    public function testPhysicalTableEmptyAfterDisable(): void
    {
        $this->mysqli->query("UPDATE mi_zte_users SET name = 'Modified' WHERE id = 1");

        // ZTD sees the modification
        $result = $this->mysqli->query('SELECT name FROM mi_zte_users WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertSame('Modified', $row['name']);

        // Disable ZTD — physical table has no rows (all writes were shadow-only)
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_zte_users');
        $row = $result->fetch_assoc();
        $this->assertSame(0, (int) $row['cnt']);
    }
}
