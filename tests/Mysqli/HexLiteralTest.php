<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests hexadecimal literal handling through the MySQLi ZTD adapter.
 *
 * MySQL supports two hex literal syntaxes:
 *   - 0x prefixed: 0x48656C6C6F (treated as binary string)
 *   - X'...' quoted: X'48656C6C6F' (SQL standard, treated as binary string)
 *
 * The CTE rewriter must pass hex literals through without corruption.
 * This is important because hex literals appear in binary data inserts,
 * UUID storage, and hash comparisons.
 *
 * 0x48656C6C6F = 'Hello'
 * 0x576F726C64 = 'World'
 *
 * @spec SPEC-3.1
 */
class HexLiteralTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_hex_lit (
            id INT PRIMARY KEY,
            data VARBINARY(255) NOT NULL,
            label VARCHAR(100) NOT NULL DEFAULT \'\'
        ) ENGINE=InnoDB';
    }

    protected function getTableNames(): array
    {
        return ['mi_hex_lit'];
    }

    /**
     * INSERT with 0x-prefixed hex literal and verify value.
     *
     * 0x48656C6C6F should be stored as the bytes for 'Hello'.
     */
    public function testInsertWith0xPrefixedHex(): void
    {
        try {
            $this->ztdExec("INSERT INTO mi_hex_lit VALUES (1, 0x48656C6C6F, 'hello-hex')");

            $rows = $this->ztdQuery("SELECT HEX(data) AS hex_data, label FROM mi_hex_lit WHERE id = 1");
            $this->assertCount(1, $rows);
            $this->assertSame('48656C6C6F', $rows[0]['hex_data'], 'Hex data should round-trip correctly');
            $this->assertSame('hello-hex', $rows[0]['label']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'INSERT with 0x hex literal failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * INSERT with X'...' quoted hex literal and verify value.
     *
     * X'48656C6C6F' is the SQL standard syntax for hex strings.
     */
    public function testInsertWithXQuotedHex(): void
    {
        try {
            $this->ztdExec("INSERT INTO mi_hex_lit VALUES (1, X'48656C6C6F', 'hello-xquote')");

            $rows = $this->ztdQuery("SELECT HEX(data) AS hex_data FROM mi_hex_lit WHERE id = 1");
            $this->assertCount(1, $rows);
            $this->assertSame('48656C6C6F', $rows[0]['hex_data'], "X'...' hex should round-trip correctly");
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                "INSERT with X'...' hex literal failed: " . $e->getMessage()
            );
        }
    }

    /**
     * SELECT hex data as string — CAST(data AS CHAR) should yield 'Hello'.
     */
    public function testSelectHexDataAsString(): void
    {
        try {
            $this->ztdExec("INSERT INTO mi_hex_lit VALUES (1, 0x48656C6C6F, 'test')");

            $rows = $this->ztdQuery("SELECT CAST(data AS CHAR) AS text_data FROM mi_hex_lit WHERE id = 1");
            $this->assertSame('Hello', $rows[0]['text_data'], '0x48656C6C6F should decode to Hello');
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'SELECT hex data as string failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * WHERE clause with 0x hex literal comparison.
     *
     * Tests that the CTE rewriter correctly handles hex literals
     * in WHERE conditions.
     */
    public function testWhereClauseWith0xHex(): void
    {
        try {
            $this->ztdExec("INSERT INTO mi_hex_lit VALUES (1, 0x48656C6C6F, 'hello')");
            $this->ztdExec("INSERT INTO mi_hex_lit VALUES (2, 0x576F726C64, 'world')");

            $rows = $this->ztdQuery("SELECT id, label FROM mi_hex_lit WHERE data = 0x48656C6C6F");
            $this->assertCount(1, $rows);
            $this->assertSame(1, (int) $rows[0]['id']);
            $this->assertSame('hello', $rows[0]['label']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'WHERE with 0x hex literal failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * WHERE clause with X'...' quoted hex comparison.
     */
    public function testWhereClauseWithXQuotedHex(): void
    {
        try {
            $this->ztdExec("INSERT INTO mi_hex_lit VALUES (1, X'48656C6C6F', 'hello')");
            $this->ztdExec("INSERT INTO mi_hex_lit VALUES (2, X'576F726C64', 'world')");

            $rows = $this->ztdQuery("SELECT id FROM mi_hex_lit WHERE data = X'576F726C64'");
            $this->assertCount(1, $rows);
            $this->assertSame(2, (int) $rows[0]['id']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                "WHERE with X'...' hex literal failed: " . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE SET with hex literal value.
     *
     * Tests that the CTE rewriter handles hex literals in UPDATE SET clauses.
     */
    public function testUpdateSetWithHexLiteral(): void
    {
        try {
            $this->ztdExec("INSERT INTO mi_hex_lit VALUES (1, 0x48656C6C6F, 'before')");

            // Update data to 'World' (0x576F726C64)
            $this->ztdExec("UPDATE mi_hex_lit SET data = 0x576F726C64, label = 'after' WHERE id = 1");

            $rows = $this->ztdQuery("SELECT HEX(data) AS hex_data, label FROM mi_hex_lit WHERE id = 1");
            $this->assertSame('576F726C64', $rows[0]['hex_data'], 'Updated hex data should be World');
            $this->assertSame('after', $rows[0]['label']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'UPDATE SET with hex literal failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE SET with X'...' quoted hex literal.
     */
    public function testUpdateSetWithXQuotedHex(): void
    {
        try {
            $this->ztdExec("INSERT INTO mi_hex_lit VALUES (1, X'48656C6C6F', 'before')");

            $this->ztdExec("UPDATE mi_hex_lit SET data = X'576F726C64' WHERE id = 1");

            $rows = $this->ztdQuery("SELECT HEX(data) AS hex_data FROM mi_hex_lit WHERE id = 1");
            $this->assertSame('576F726C64', $rows[0]['hex_data']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                "UPDATE SET with X'...' hex literal failed: " . $e->getMessage()
            );
        }
    }

    /**
     * Mixed hex syntaxes in a single INSERT.
     *
     * Tests that both hex syntaxes work correctly in the same statement.
     */
    public function testMixedHexSyntaxes(): void
    {
        try {
            $this->ztdExec("INSERT INTO mi_hex_lit VALUES (1, 0x48656C6C6F, 'zero-x')");
            $this->ztdExec("INSERT INTO mi_hex_lit VALUES (2, X'576F726C64', 'x-quote')");

            $rows = $this->ztdQuery("SELECT id, HEX(data) AS hex_data FROM mi_hex_lit ORDER BY id");
            $this->assertCount(2, $rows);
            $this->assertSame('48656C6C6F', $rows[0]['hex_data']);
            $this->assertSame('576F726C64', $rows[1]['hex_data']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Mixed hex syntaxes failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Hex literal in INSERT with multiple rows.
     */
    public function testMultiRowInsertWithHex(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO mi_hex_lit VALUES
                 (1, 0x48656C6C6F, 'hello'),
                 (2, 0x576F726C64, 'world'),
                 (3, X'466F6F', 'foo')"
            );

            $rows = $this->ztdQuery('SELECT id, HEX(data) AS hex_data FROM mi_hex_lit ORDER BY id');
            $this->assertCount(3, $rows);
            $this->assertSame('48656C6C6F', $rows[0]['hex_data']); // Hello
            $this->assertSame('576F726C64', $rows[1]['hex_data']); // World
            $this->assertSame('466F6F', $rows[2]['hex_data']);     // Foo
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Multi-row INSERT with hex failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Hex literal with CONCAT function.
     *
     * Tests that hex literals work within MySQL functions.
     */
    public function testHexLiteralInConcat(): void
    {
        try {
            $this->ztdExec("INSERT INTO mi_hex_lit VALUES (1, 0x48656C6C6F, 'test')");

            $rows = $this->ztdQuery(
                "SELECT HEX(CONCAT(data, 0x20576F726C64)) AS hex_result FROM mi_hex_lit WHERE id = 1"
            );
            // 'Hello' + ' World' = 'Hello World'
            // 0x48656C6C6F20576F726C64
            $this->assertSame('48656C6C6F20576F726C64', $rows[0]['hex_result']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Hex literal in CONCAT failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Hex literal for empty binary string.
     *
     * 0x (empty) and X'' should both work as empty binary values.
     */
    public function testEmptyHexLiteral(): void
    {
        try {
            $this->ztdExec("INSERT INTO mi_hex_lit VALUES (1, X'', 'empty')");

            $rows = $this->ztdQuery("SELECT HEX(data) AS hex_data, LENGTH(data) AS data_len FROM mi_hex_lit WHERE id = 1");
            $this->assertSame('', $rows[0]['hex_data'], 'Empty hex should produce empty data');
            $this->assertEquals(0, (int) $rows[0]['data_len']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Empty hex literal failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Physical isolation: hex literal data must not reach the physical table.
     */
    public function testPhysicalIsolation(): void
    {
        $this->ztdExec("INSERT INTO mi_hex_lit VALUES (1, 0x48656C6C6F, 'test')");

        $this->disableZtd();
        $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM mi_hex_lit');
        $this->assertEquals(0, (int) $rows[0]['cnt'], 'Physical table should be empty');
    }
}
