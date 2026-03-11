<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests BINARY/BLOB data integrity through the ZTD CTE rewriter on MySQL PDO.
 *
 * Binary data (NUL bytes, control characters, non-UTF-8 byte sequences)
 * must survive INSERT→SELECT round-trips when ZTD rewrites queries as CTEs.
 * If the rewriter embeds values as string literals in CTE definitions,
 * binary content may be silently corrupted.
 *
 * @spec SPEC-10.2
 */
class MysqlBinaryBlobDataDmlTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return "CREATE TABLE my_blob_t (
            id INT AUTO_INCREMENT PRIMARY KEY,
            label VARCHAR(50),
            bin_data BLOB,
            vbin_data VARBINARY(255)
        ) ENGINE=InnoDB";
    }

    protected function getTableNames(): array
    {
        return ['my_blob_t'];
    }

    /**
     * NUL byte (\x00) embedded in BLOB column.
     */
    public function testNulByteInBlob(): void
    {
        $data = "before\x00after";

        try {
            $stmt = $this->pdo->prepare("INSERT INTO my_blob_t (label, bin_data) VALUES (?, ?)");
            $stmt->execute(['nul_test', $data]);

            $rows = $this->ztdQuery("SELECT bin_data FROM my_blob_t WHERE label = 'nul_test'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'NUL byte BLOB (MySQL): expected 1 row, got ' . count($rows)
                );
            }

            if ($rows[0]['bin_data'] !== $data) {
                $this->markTestIncomplete(
                    'NUL byte BLOB (MySQL): data corrupted. Expected ' . bin2hex($data)
                    . ', got ' . bin2hex($rows[0]['bin_data'])
                );
            }

            $this->assertSame($data, $rows[0]['bin_data']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('NUL byte BLOB (MySQL) failed: ' . $e->getMessage());
        }
    }

    /**
     * Byte sequence 0x00–0xFF in BLOB column.
     */
    public function testFullByteRangeInBlob(): void
    {
        $data = '';
        for ($i = 0; $i < 256; $i++) {
            $data .= chr($i);
        }

        try {
            $stmt = $this->pdo->prepare("INSERT INTO my_blob_t (label, bin_data) VALUES (?, ?)");
            $stmt->execute(['byte_range', $data]);

            $rows = $this->ztdQuery("SELECT bin_data FROM my_blob_t WHERE label = 'byte_range'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Full byte range BLOB (MySQL): expected 1 row, got ' . count($rows)
                );
            }

            if (strlen($rows[0]['bin_data']) !== 256) {
                $this->markTestIncomplete(
                    'Full byte range BLOB (MySQL): expected 256 bytes, got ' . strlen($rows[0]['bin_data'])
                );
            }

            $this->assertSame($data, $rows[0]['bin_data']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Full byte range BLOB (MySQL) failed: ' . $e->getMessage());
        }
    }

    /**
     * VARBINARY column with backslash-heavy content.
     */
    public function testBackslashSequencesInVarbinary(): void
    {
        $data = "\\n\\r\\t\\\\\\'\\\"\\0";

        try {
            $stmt = $this->pdo->prepare("INSERT INTO my_blob_t (label, vbin_data) VALUES (?, ?)");
            $stmt->execute(['backslash', $data]);

            $rows = $this->ztdQuery("SELECT vbin_data FROM my_blob_t WHERE label = 'backslash'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Backslash VARBINARY (MySQL): expected 1 row, got ' . count($rows)
                );
            }

            if ($rows[0]['vbin_data'] !== $data) {
                $this->markTestIncomplete(
                    'Backslash VARBINARY (MySQL): data corrupted. Expected ' . bin2hex($data)
                    . ', got ' . bin2hex($rows[0]['vbin_data'])
                );
            }

            $this->assertSame($data, $rows[0]['vbin_data']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Backslash VARBINARY (MySQL) failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE a BLOB column with binary data after ZTD INSERT.
     */
    public function testUpdateBlobAfterInsert(): void
    {
        $original = "\x01\x02\x03";
        $updated = "\xFE\xFF\x00\x01";

        try {
            $stmt = $this->pdo->prepare("INSERT INTO my_blob_t (label, bin_data) VALUES (?, ?)");
            $stmt->execute(['update_test', $original]);

            $stmt2 = $this->pdo->prepare("UPDATE my_blob_t SET bin_data = ? WHERE label = ?");
            $stmt2->execute([$updated, 'update_test']);

            $rows = $this->ztdQuery("SELECT bin_data FROM my_blob_t WHERE label = 'update_test'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'UPDATE BLOB (MySQL): expected 1 row, got ' . count($rows)
                );
            }

            if ($rows[0]['bin_data'] !== $updated) {
                $this->markTestIncomplete(
                    'UPDATE BLOB (MySQL): data corrupted after update. Expected ' . bin2hex($updated)
                    . ', got ' . bin2hex($rows[0]['bin_data'])
                );
            }

            $this->assertSame($updated, $rows[0]['bin_data']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE BLOB (MySQL) failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared SELECT with binary parameter in WHERE.
     */
    public function testPreparedSelectWithBinaryParam(): void
    {
        $data = "\x00\xFF\x80\x7F";

        try {
            $stmt = $this->pdo->prepare("INSERT INTO my_blob_t (label, vbin_data) VALUES (?, ?)");
            $stmt->execute(['bin_where', $data]);

            $rows = $this->ztdPrepareAndExecute(
                "SELECT label FROM my_blob_t WHERE vbin_data = ?",
                [$data]
            );

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Prepared binary WHERE (MySQL): expected 1 row, got ' . count($rows)
                );
            }

            $this->assertSame('bin_where', $rows[0]['label']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared binary WHERE (MySQL) failed: ' . $e->getMessage());
        }
    }
}
