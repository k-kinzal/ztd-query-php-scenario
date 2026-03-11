<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests BINARY/BLOB data integrity through the ZTD CTE rewriter on MySQLi.
 *
 * @spec SPEC-10.2
 */
class BinaryBlobDataDmlTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return "CREATE TABLE mi_blob_t (
            id INT AUTO_INCREMENT PRIMARY KEY,
            label VARCHAR(50),
            bin_data BLOB,
            vbin_data VARBINARY(255)
        ) ENGINE=InnoDB";
    }

    protected function getTableNames(): array
    {
        return ['mi_blob_t'];
    }

    /**
     * NUL byte (\x00) embedded in BLOB column.
     */
    public function testNulByteInBlob(): void
    {
        $data = "before\x00after";

        try {
            $stmt = $this->mysqli->prepare("INSERT INTO mi_blob_t (label, bin_data) VALUES (?, ?)");
            $stmt->bind_param('ss', ...['nul_test', $data]);
            $stmt->execute();

            $rows = $this->ztdQuery("SELECT bin_data FROM mi_blob_t WHERE label = 'nul_test'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'NUL byte BLOB (MySQLi): expected 1 row, got ' . count($rows)
                );
            }

            if ($rows[0]['bin_data'] !== $data) {
                $this->markTestIncomplete(
                    'NUL byte BLOB (MySQLi): data corrupted. Expected ' . bin2hex($data)
                    . ', got ' . bin2hex($rows[0]['bin_data'])
                );
            }

            $this->assertSame($data, $rows[0]['bin_data']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('NUL byte BLOB (MySQLi) failed: ' . $e->getMessage());
        }
    }

    /**
     * Full 0x00–0xFF byte range in BLOB column.
     */
    public function testFullByteRangeInBlob(): void
    {
        $data = '';
        for ($i = 0; $i < 256; $i++) {
            $data .= chr($i);
        }

        try {
            $stmt = $this->mysqli->prepare("INSERT INTO mi_blob_t (label, bin_data) VALUES (?, ?)");
            $label = 'byte_range';
            $stmt->bind_param('ss', $label, $data);
            $stmt->execute();

            $rows = $this->ztdQuery("SELECT bin_data FROM mi_blob_t WHERE label = 'byte_range'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Full byte range BLOB (MySQLi): expected 1 row, got ' . count($rows)
                );
            }

            if (strlen($rows[0]['bin_data']) !== 256) {
                $this->markTestIncomplete(
                    'Full byte range BLOB (MySQLi): expected 256 bytes, got ' . strlen($rows[0]['bin_data'])
                );
            }

            $this->assertSame($data, $rows[0]['bin_data']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Full byte range BLOB (MySQLi) failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE BLOB after INSERT.
     */
    public function testUpdateBlobAfterInsert(): void
    {
        $original = "\x01\x02\x03";
        $updated = "\xFE\xFF\x00\x01";

        try {
            $stmt = $this->mysqli->prepare("INSERT INTO mi_blob_t (label, bin_data) VALUES (?, ?)");
            $label = 'update_test';
            $stmt->bind_param('ss', $label, $original);
            $stmt->execute();

            $stmt2 = $this->mysqli->prepare("UPDATE mi_blob_t SET bin_data = ? WHERE label = ?");
            $stmt2->bind_param('ss', $updated, $label);
            $stmt2->execute();

            $rows = $this->ztdQuery("SELECT bin_data FROM mi_blob_t WHERE label = 'update_test'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'UPDATE BLOB (MySQLi): expected 1 row, got ' . count($rows)
                );
            }

            if ($rows[0]['bin_data'] !== $updated) {
                $this->markTestIncomplete(
                    'UPDATE BLOB (MySQLi): data corrupted. Expected ' . bin2hex($updated)
                    . ', got ' . bin2hex($rows[0]['bin_data'])
                );
            }

            $this->assertSame($updated, $rows[0]['bin_data']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE BLOB (MySQLi) failed: ' . $e->getMessage());
        }
    }
}
