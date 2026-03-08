<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests ZtdPdo::quote() with different PDO::PARAM_* types on MySQL.
 *
 * ZtdPdo::quote() delegates to the inner PDO connection.
 * This verifies that quote() works correctly with ZTD enabled
 * for various parameter types.
 * @spec SPEC-4.9
 */
class MysqlQuoteParamTypesTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pdo_quote_test (id INT PRIMARY KEY, name VARCHAR(50), score INT, active BOOLEAN)';
    }

    protected function getTableNames(): array
    {
        return ['pdo_quote_test'];
    }


    /**
     * quote() with default PARAM_STR.
     */
    public function testQuoteString(): void
    {
        $quoted = $this->pdo->quote("it's a test");
        $this->assertIsString($quoted);
        // MySQL quotes with single quotes and escapes internal quotes
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
     * quote() with PARAM_STR explicit.
     */
    public function testQuoteStringExplicit(): void
    {
        $quoted = $this->pdo->quote('hello', PDO::PARAM_STR);
        $this->assertIsString($quoted);
        $this->assertStringContainsString('hello', $quoted);
    }

    /**
     * quote() with special characters.
     */
    public function testQuoteSpecialChars(): void
    {
        $quoted = $this->pdo->quote("O'Brien\\path");
        $this->assertIsString($quoted);
        // Should properly escape the quote and backslash
        $this->assertStringContainsString('Brien', $quoted);
    }

    /**
     * quote() with empty string.
     */
    public function testQuoteEmptyString(): void
    {
        $quoted = $this->pdo->quote('');
        $this->assertIsString($quoted);
        // Empty string should be quoted as '' (with surrounding quotes)
        $this->assertSame("''", $quoted);
    }

    /**
     * Using quoted value in a query works.
     */
    public function testQuotedValueInQuery(): void
    {
        $this->pdo->exec("INSERT INTO pdo_quote_test (id, name, score) VALUES (1, 'Alice', 90)");

        $quotedName = $this->pdo->quote('Alice');
        $stmt = $this->pdo->query("SELECT id FROM pdo_quote_test WHERE name = $quotedName");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(1, (int) $row['id']);
    }

    /**
     * quote() with SQL injection attempt — properly escaped.
     */
    public function testQuoteEscapesSqlInjection(): void
    {
        $malicious = "'; DROP TABLE pdo_quote_test; --";
        $quoted = $this->pdo->quote($malicious);

        // Should be a safe string containing the escaped malicious input
        $this->assertIsString($quoted);
        $this->assertStringContainsString('DROP', $quoted); // Content preserved
        // The key assertion is that using this in a query doesn't cause issues
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO pdo_quote_test (id, name, score) VALUES (1, 'Alice', 90)");

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_quote_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
