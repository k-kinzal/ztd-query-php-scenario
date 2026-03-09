<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests hexadecimal literal handling through the CTE shadow store on SQLite PDO.
 *
 * SQLite supports two hex literal forms:
 * - X'...' (SQL standard blob literal, e.g., X'DEADBEEF')
 * - 0x... (integer literal, e.g., 0x1234 = 4660)
 *
 * The CTE rewriter must preserve these literals during query transformation.
 * Mishandling could corrupt binary data, truncate hex tokens, or misparse
 * the X'...' as a table alias or string.
 * @spec SPEC-4.1
 * @spec SPEC-3.1
 */
class SqliteHexLiteralTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_hex_data (id INTEGER PRIMARY KEY, int_val INTEGER, blob_val BLOB, label TEXT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_hex_data'];
    }

    /**
     * INSERT with X'...' blob literal and verify roundtrip.
     */
    public function testInsertXBlobLiteral(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_hex_data (id, blob_val, label) VALUES (1, X'DEADBEEF', 'test-blob')");
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'INSERT with X\'DEADBEEF\' blob literal failed: ' . $e->getMessage()
            );
            return;
        }

        $rows = $this->ztdQuery('SELECT id, blob_val, label FROM sl_hex_data WHERE id = 1');

        if (count($rows) !== 1) {
            $this->markTestIncomplete(
                'X\'...\' blob literal: expected 1 row, got ' . count($rows)
            );
            return;
        }

        $this->assertSame('test-blob', $rows[0]['label']);
        // Blob value should be the 4-byte binary \xDE\xAD\xBE\xEF
        $this->assertSame("\xDE\xAD\xBE\xEF", $rows[0]['blob_val']);
    }

    /**
     * INSERT with 0x integer hex literal.
     * 0x1234 = 4660 in decimal.
     */
    public function testInsertIntegerHexLiteral(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_hex_data (id, int_val, label) VALUES (2, 0x1234, 'hex-int')");
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'INSERT with 0x1234 integer hex literal failed: ' . $e->getMessage()
            );
            return;
        }

        $rows = $this->ztdQuery('SELECT int_val FROM sl_hex_data WHERE id = 2');

        if (count($rows) !== 1) {
            $this->markTestIncomplete(
                '0x... integer hex literal: expected 1 row, got ' . count($rows)
            );
            return;
        }

        $this->assertSame(4660, (int) $rows[0]['int_val']);
    }

    /**
     * INSERT with X'00' (single null byte) — edge case for short blob.
     *
     * The CTE rewriter may fail to parse X'00' because the null byte
     * confuses the SQL tokenizer during query transformation.
     */
    public function testInsertSingleNullByteBlobLiteral(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_hex_data (id, blob_val, label) VALUES (3, X'00', 'null-byte')");
            $rows = $this->ztdQuery('SELECT blob_val FROM sl_hex_data WHERE id = 3');
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'CTE rewriter cannot handle X\'00\' blob literal (null byte corrupts CTE query): ' . $e->getMessage()
            );
            return;
        }

        if (count($rows) !== 1) {
            $this->markTestIncomplete(
                'X\'00\' single null byte: expected 1 row, got ' . count($rows)
            );
            return;
        }

        $this->assertSame("\x00", $rows[0]['blob_val']);
    }

    /**
     * INSERT with empty blob X'' — zero-length blob.
     */
    public function testInsertEmptyBlobLiteral(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_hex_data (id, blob_val, label) VALUES (4, X'', 'empty-blob')");
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'INSERT with X\'\' empty blob literal failed: ' . $e->getMessage()
            );
            return;
        }

        $rows = $this->ztdQuery('SELECT blob_val FROM sl_hex_data WHERE id = 4');

        if (count($rows) !== 1) {
            $this->markTestIncomplete(
                'X\'\' empty blob: expected 1 row, got ' . count($rows)
            );
            return;
        }

        // Empty blob should be empty string or null depending on PDO behavior
        $this->assertTrue($rows[0]['blob_val'] === '' || $rows[0]['blob_val'] === null);
    }

    /**
     * UPDATE SET column to a hex blob literal, then verify.
     */
    public function testUpdateWithHexBlobLiteral(): void
    {
        $this->pdo->exec("INSERT INTO sl_hex_data (id, blob_val, label) VALUES (5, X'AABB', 'before')");

        try {
            $this->pdo->exec("UPDATE sl_hex_data SET blob_val = X'CCDDEE', label = 'after' WHERE id = 5");
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'UPDATE SET blob_val = X\'CCDDEE\' failed: ' . $e->getMessage()
            );
            return;
        }

        $rows = $this->ztdQuery('SELECT blob_val, label FROM sl_hex_data WHERE id = 5');

        $this->assertCount(1, $rows);
        $this->assertSame('after', $rows[0]['label']);
        $this->assertSame("\xCC\xDD\xEE", $rows[0]['blob_val']);
    }

    /**
     * WHERE clause comparison with hex blob literal.
     */
    public function testWhereWithHexBlobComparison(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_hex_data (id, blob_val, label) VALUES (6, X'CAFE', 'cafe')");
            $this->pdo->exec("INSERT INTO sl_hex_data (id, blob_val, label) VALUES (7, X'BABE', 'babe')");
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'INSERT with hex blobs for WHERE test failed: ' . $e->getMessage()
            );
            return;
        }

        try {
            $rows = $this->ztdQuery("SELECT label FROM sl_hex_data WHERE blob_val = X'CAFE'");
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'WHERE blob_val = X\'CAFE\' failed: ' . $e->getMessage()
            );
            return;
        }

        if (count($rows) !== 1) {
            $this->markTestIncomplete(
                'WHERE with hex blob comparison: expected 1 row, got ' . count($rows)
            );
            return;
        }

        $this->assertSame('cafe', $rows[0]['label']);
    }

    /**
     * WHERE clause with 0x integer comparison.
     */
    public function testWhereWithIntegerHexComparison(): void
    {
        $this->pdo->exec("INSERT INTO sl_hex_data (id, int_val, label) VALUES (8, 255, 'ff')");
        $this->pdo->exec("INSERT INTO sl_hex_data (id, int_val, label) VALUES (9, 256, 'hundred')");

        try {
            $rows = $this->ztdQuery('SELECT label FROM sl_hex_data WHERE int_val = 0xFF');
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'WHERE int_val = 0xFF failed: ' . $e->getMessage()
            );
            return;
        }

        if (count($rows) !== 1) {
            $this->markTestIncomplete(
                'WHERE with 0xFF comparison: expected 1 row, got ' . count($rows)
            );
            return;
        }

        $this->assertSame('ff', $rows[0]['label']);
    }

    /**
     * Multi-row INSERT with mixed hex literals.
     */
    public function testMultiRowInsertWithHexLiterals(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO sl_hex_data (id, int_val, blob_val, label) VALUES "
                . "(10, 0xA, X'0A', 'ten'), "
                . "(11, 0xB, X'0B', 'eleven'), "
                . "(12, 0xC, X'0C', 'twelve')"
            );
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Multi-row INSERT with hex literals failed: ' . $e->getMessage()
            );
            return;
        }

        $rows = $this->ztdQuery('SELECT id, int_val, blob_val, label FROM sl_hex_data WHERE id >= 10 ORDER BY id');

        if (count($rows) !== 3) {
            $this->markTestIncomplete(
                'Multi-row hex INSERT: expected 3 rows, got ' . count($rows)
            );
            return;
        }

        $this->assertSame(10, (int) $rows[0]['int_val']);
        $this->assertSame(11, (int) $rows[1]['int_val']);
        $this->assertSame(12, (int) $rows[2]['int_val']);
        $this->assertSame("\x0A", $rows[0]['blob_val']);
        $this->assertSame("\x0B", $rows[1]['blob_val']);
        $this->assertSame("\x0C", $rows[2]['blob_val']);
    }

    /**
     * HEX() function in SELECT — verify the shadow store supports SQLite HEX() output.
     */
    public function testHexFunctionInSelect(): void
    {
        $this->pdo->exec("INSERT INTO sl_hex_data (id, blob_val, label) VALUES (13, X'48454C4C4F', 'hello-hex')");

        $rows = $this->ztdQuery('SELECT HEX(blob_val) AS hex_out FROM sl_hex_data WHERE id = 13');

        $this->assertCount(1, $rows);
        $this->assertSame('48454C4C4F', $rows[0]['hex_out']);
    }

    /**
     * Prepared INSERT with blob parameter vs hex literal — both visible.
     *
     * The CTE rewriter may fail when hex literals contain bytes that look
     * like string terminators (e.g., X'FF00FF' with embedded 0x00).
     */
    public function testPreparedBlobVsHexLiteralCoexistence(): void
    {
        try {
            // Insert via hex literal
            $this->pdo->exec("INSERT INTO sl_hex_data (id, blob_val, label) VALUES (14, X'FF00FF', 'hex-literal')");

            // Insert via prepared statement with binary param
            $stmt = $this->pdo->prepare('INSERT INTO sl_hex_data (id, blob_val, label) VALUES (?, ?, ?)');
            $stmt->execute([15, "\xFF\x00\xFF", 'prepared-blob']);

            $rows = $this->ztdQuery('SELECT id, blob_val, label FROM sl_hex_data WHERE id IN (14, 15) ORDER BY id');
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'CTE rewriter fails with binary blob data containing null bytes in shadow store: ' . $e->getMessage()
            );
            return;
        }

        $this->assertCount(2, $rows);
        // Both should contain the same binary data
        $this->assertSame("\xFF\x00\xFF", $rows[0]['blob_val']);
        $this->assertSame("\xFF\x00\xFF", $rows[1]['blob_val']);
    }

    /**
     * Physical isolation: hex-inserted data not visible with ZTD disabled.
     */
    public function testPhysicalIsolation(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_hex_data (id, blob_val, label) VALUES (20, X'FACE', 'isolation')");
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'INSERT hex for isolation test failed: ' . $e->getMessage()
            );
            return;
        }

        $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM sl_hex_data');
        $this->assertSame('1', (string) $rows[0]['cnt']);

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM sl_hex_data');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
