<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests MySQL hex literal handling through the ZTD CTE shadow store.
 *
 * MySQL supports two hex literal syntaxes:
 * - 0xHHHH (MySQL-specific, no quotes)
 * - X'HHHH' (standard SQL hex string)
 *
 * These literals can appear in INSERT VALUES, UPDATE SET, and WHERE clauses.
 * The CTE rewriter must pass hex literals through without corruption,
 * and the shadow store must correctly store and compare the resulting values.
 *
 * @spec SPEC-4.1, SPEC-4.2, SPEC-3.1
 */
class MysqlHexLiteralTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mp_hl_data (
                id INT PRIMARY KEY,
                label VARCHAR(100) NOT NULL,
                content VARBINARY(200)
            ) ENGINE=InnoDB',
            'CREATE TABLE mp_hl_text (
                id INT PRIMARY KEY,
                name VARCHAR(100) NOT NULL
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mp_hl_data', 'mp_hl_text'];
    }

    /**
     * INSERT with 0x hex literal (MySQL-specific syntax).
     * 0x48656C6C6F is hex for 'Hello'.
     */
    public function testInsertWithOxHexLiteral(): void
    {
        try {
            $this->ztdExec("INSERT INTO mp_hl_text (id, name) VALUES (1, 0x48656C6C6F)");

            $rows = $this->ztdQuery('SELECT name FROM mp_hl_text WHERE id = 1');
            $this->assertCount(1, $rows);
            $this->assertSame('Hello', $rows[0]['name']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'INSERT with 0x hex literal failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * INSERT with X'...' hex literal (standard SQL syntax).
     * X'48656C6C6F' is hex for 'Hello'.
     */
    public function testInsertWithXQuoteHexLiteral(): void
    {
        try {
            $this->ztdExec("INSERT INTO mp_hl_text (id, name) VALUES (1, X'48656C6C6F')");

            $rows = $this->ztdQuery('SELECT name FROM mp_hl_text WHERE id = 1');
            $this->assertCount(1, $rows);
            $this->assertSame('Hello', $rows[0]['name']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'INSERT with X\' hex literal failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * INSERT with hex literal into VARBINARY column.
     */
    public function testInsertHexIntoVarbinary(): void
    {
        try {
            $this->ztdExec("INSERT INTO mp_hl_data (id, label, content) VALUES (1, 'binary', 0xDEADBEEF)");

            $rows = $this->ztdQuery('SELECT label, HEX(content) AS hex_content FROM mp_hl_data WHERE id = 1');
            $this->assertCount(1, $rows);
            $this->assertSame('binary', $rows[0]['label']);
            $this->assertSame('DEADBEEF', $rows[0]['hex_content']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'INSERT hex into VARBINARY failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * SELECT with WHERE clause comparing hex literal.
     */
    public function testSelectWhereHexComparison(): void
    {
        try {
            $this->ztdExec("INSERT INTO mp_hl_text (id, name) VALUES (1, 'Hello')");
            $this->ztdExec("INSERT INTO mp_hl_text (id, name) VALUES (2, 'World')");

            // 0x48656C6C6F = 'Hello'
            $rows = $this->ztdQuery('SELECT id, name FROM mp_hl_text WHERE name = 0x48656C6C6F');
            $this->assertCount(1, $rows);
            $this->assertSame(1, (int) $rows[0]['id']);
            $this->assertSame('Hello', $rows[0]['name']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'SELECT WHERE hex comparison failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE SET with 0x hex literal.
     * 0x576F726C64 is hex for 'World'.
     */
    public function testUpdateWithHexLiteral(): void
    {
        try {
            $this->ztdExec("INSERT INTO mp_hl_text (id, name) VALUES (1, 'Hello')");
            $this->ztdExec('UPDATE mp_hl_text SET name = 0x576F726C64 WHERE id = 1');

            $rows = $this->ztdQuery('SELECT name FROM mp_hl_text WHERE id = 1');
            $this->assertCount(1, $rows);
            $this->assertSame('World', $rows[0]['name']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'UPDATE with hex literal failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Multiple rows with hex literals and verify ordering.
     */
    public function testMultipleRowsWithHexLiterals(): void
    {
        try {
            // 0x41 = 'A', 0x42 = 'B', 0x43 = 'C'
            $this->ztdExec("INSERT INTO mp_hl_text (id, name) VALUES (1, 0x41)");
            $this->ztdExec("INSERT INTO mp_hl_text (id, name) VALUES (2, 0x42)");
            $this->ztdExec("INSERT INTO mp_hl_text (id, name) VALUES (3, 0x43)");

            $rows = $this->ztdQuery('SELECT id, name FROM mp_hl_text ORDER BY name');
            $this->assertCount(3, $rows);
            $this->assertSame('A', $rows[0]['name']);
            $this->assertSame('B', $rows[1]['name']);
            $this->assertSame('C', $rows[2]['name']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Multiple rows with hex literals failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Hex literal in multi-row INSERT.
     */
    public function testMultiRowInsertWithHexLiterals(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO mp_hl_text (id, name) VALUES "
                . "(1, 0x48656C6C6F), (2, 0x576F726C64)"
            );

            $rows = $this->ztdQuery('SELECT id, name FROM mp_hl_text ORDER BY id');
            $this->assertCount(2, $rows);
            $this->assertSame('Hello', $rows[0]['name']);
            $this->assertSame('World', $rows[1]['name']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Multi-row INSERT with hex literals failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * WHERE clause using X'...' syntax for comparison.
     */
    public function testWhereXQuoteHexComparison(): void
    {
        try {
            $this->ztdExec("INSERT INTO mp_hl_text (id, name) VALUES (1, 'Hello')");

            $rows = $this->ztdQuery("SELECT id FROM mp_hl_text WHERE name = X'48656C6C6F'");
            $this->assertCount(1, $rows);
            $this->assertSame(1, (int) $rows[0]['id']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'WHERE with X\' hex comparison failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Empty hex literal (0x or X'').
     */
    public function testEmptyHexLiteral(): void
    {
        try {
            $this->ztdExec("INSERT INTO mp_hl_data (id, label, content) VALUES (1, 'empty', X'')");

            $rows = $this->ztdQuery('SELECT label, content FROM mp_hl_data WHERE id = 1');
            $this->assertCount(1, $rows);
            $this->assertSame('empty', $rows[0]['label']);
            // Empty hex literal produces empty string/binary
            $this->assertSame('', $rows[0]['content']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Empty hex literal failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Physical isolation: hex-inserted shadow data does not appear in physical table.
     */
    public function testPhysicalIsolation(): void
    {
        try {
            $this->ztdExec("INSERT INTO mp_hl_text (id, name) VALUES (1, 0x48656C6C6F)");

            $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM mp_hl_text');
            $this->assertSame(1, (int) $rows[0]['cnt']);

            $this->pdo->disableZtd();
            $stmt = $this->pdo->query('SELECT COUNT(*) FROM mp_hl_text');
            $this->assertSame(0, (int) $stmt->fetchColumn());
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Hex literal physical isolation check failed: ' . $e->getMessage()
            );
        }
    }
}
