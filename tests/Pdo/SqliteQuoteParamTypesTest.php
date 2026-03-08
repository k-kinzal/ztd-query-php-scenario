<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests ZtdPdo::quote() with different PDO::PARAM_* types on SQLite.
 *
 * SQLite in-memory, no container needed.
 * @spec pending
 */
class SqliteQuoteParamTypesTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sq_qt_test (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)';
    }

    protected function getTableNames(): array
    {
        return ['sq_qt_test'];
    }


    /**
     * quote() with default PARAM_STR.
     */
    public function testQuoteString(): void
    {
        $quoted = $this->pdo->quote("it's a test");
        $this->assertIsString($quoted);
        $this->assertStringContainsString("it", $quoted);
    }

    /**
     * quote() with PARAM_INT.
     */
    public function testQuoteInt(): void
    {
        $quoted = $this->pdo->quote('42', PDO::PARAM_INT);
        $this->assertIsString($quoted);
        $this->assertStringContainsString('42', $quoted);
    }

    /**
     * quote() with special characters — SQLite double-quotes escaping.
     */
    public function testQuoteSpecialChars(): void
    {
        $quoted = $this->pdo->quote("O'Brien");
        $this->assertIsString($quoted);
        $this->assertStringContainsString('Brien', $quoted);
    }

    /**
     * quote() empty string.
     */
    public function testQuoteEmptyString(): void
    {
        $quoted = $this->pdo->quote('');
        $this->assertIsString($quoted);
        $this->assertSame("''", $quoted);
    }

    /**
     * Quoted value used in shadow query.
     */
    public function testQuotedValueInShadowQuery(): void
    {
        $this->pdo->exec("INSERT INTO sq_qt_test (id, name, score) VALUES (1, 'Alice', 90)");

        $quotedName = $this->pdo->quote('Alice');
        $stmt = $this->pdo->query("SELECT id FROM sq_qt_test WHERE name = $quotedName");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(1, (int) $row['id']);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO sq_qt_test (id, name, score) VALUES (1, 'Alice', 90)");

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM sq_qt_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
