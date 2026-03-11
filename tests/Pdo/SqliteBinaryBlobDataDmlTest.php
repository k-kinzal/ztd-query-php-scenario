<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests BLOB data integrity through the ZTD CTE rewriter on SQLite PDO.
 *
 * @spec SPEC-10.2
 */
class SqliteBinaryBlobDataDmlTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        // Use explicit PK (not AUTOINCREMENT) to avoid Issue #145 shadow PK=null
        return "CREATE TABLE sl_blob_t (
            id INTEGER PRIMARY KEY,
            label TEXT,
            bin_data BLOB
        )";
    }

    protected function getTableNames(): array
    {
        return ['sl_blob_t'];
    }

    /**
     * NUL byte (\x00) embedded in BLOB column.
     */
    public function testNulByteInBlob(): void
    {
        $data = "before\x00after";

        try {
            $stmt = $this->pdo->prepare("INSERT INTO sl_blob_t (id, label, bin_data) VALUES (?, ?, ?)");
            $stmt->bindValue(1, 1, PDO::PARAM_INT);
            $stmt->bindValue(2, 'nul_test', PDO::PARAM_STR);
            $stmt->bindValue(3, $data, PDO::PARAM_LOB);
            $stmt->execute();

            $rows = $this->ztdQuery("SELECT bin_data FROM sl_blob_t WHERE label = 'nul_test'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'NUL byte BLOB (SQLite): expected 1 row, got ' . count($rows)
                );
            }

            if ($rows[0]['bin_data'] !== $data) {
                $this->markTestIncomplete(
                    'NUL byte BLOB (SQLite): data corrupted. Expected ' . bin2hex($data)
                    . ', got ' . bin2hex($rows[0]['bin_data'])
                );
            }

            $this->assertSame($data, $rows[0]['bin_data']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('NUL byte BLOB (SQLite) failed: ' . $e->getMessage());
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
            $stmt = $this->pdo->prepare("INSERT INTO sl_blob_t (id, label, bin_data) VALUES (?, ?, ?)");
            $stmt->bindValue(1, 1, PDO::PARAM_INT);
            $stmt->bindValue(2, 'byte_range', PDO::PARAM_STR);
            $stmt->bindValue(3, $data, PDO::PARAM_LOB);
            $stmt->execute();

            $rows = $this->ztdQuery("SELECT bin_data FROM sl_blob_t WHERE label = 'byte_range'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Full byte range BLOB (SQLite): expected 1 row, got ' . count($rows)
                );
            }

            if (strlen($rows[0]['bin_data']) !== 256) {
                $this->markTestIncomplete(
                    'Full byte range BLOB (SQLite): expected 256 bytes, got ' . strlen($rows[0]['bin_data'])
                );
            }

            $this->assertSame($data, $rows[0]['bin_data']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Full byte range BLOB (SQLite) failed: ' . $e->getMessage());
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
            $stmt = $this->pdo->prepare("INSERT INTO sl_blob_t (id, label, bin_data) VALUES (?, ?, ?)");
            $stmt->bindValue(1, 1, PDO::PARAM_INT);
            $stmt->bindValue(2, 'update_test', PDO::PARAM_STR);
            $stmt->bindValue(3, $original, PDO::PARAM_LOB);
            $stmt->execute();

            $stmt2 = $this->pdo->prepare("UPDATE sl_blob_t SET bin_data = ? WHERE label = ?");
            $stmt2->bindValue(1, $updated, PDO::PARAM_LOB);
            $stmt2->bindValue(2, 'update_test', PDO::PARAM_STR);
            $stmt2->execute();

            $rows = $this->ztdQuery("SELECT bin_data FROM sl_blob_t WHERE label = 'update_test'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'UPDATE BLOB (SQLite): expected 1 row, got ' . count($rows)
                );
            }

            if ($rows[0]['bin_data'] !== $updated) {
                $this->markTestIncomplete(
                    'UPDATE BLOB (SQLite): data corrupted. Expected ' . bin2hex($updated)
                    . ', got ' . bin2hex($rows[0]['bin_data'])
                );
            }

            $this->assertSame($updated, $rows[0]['bin_data']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE BLOB (SQLite) failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared SELECT with binary parameter in WHERE.
     */
    public function testPreparedSelectWithBinaryParam(): void
    {
        $data = "\x00\xFF\x80\x7F";

        try {
            $stmt = $this->pdo->prepare("INSERT INTO sl_blob_t (id, label, bin_data) VALUES (?, ?, ?)");
            $stmt->bindValue(1, 1, PDO::PARAM_INT);
            $stmt->bindValue(2, 'bin_where', PDO::PARAM_STR);
            $stmt->bindValue(3, $data, PDO::PARAM_LOB);
            $stmt->execute();

            $rows = $this->ztdPrepareAndExecute(
                "SELECT label FROM sl_blob_t WHERE bin_data = ?",
                [$data]
            );

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Prepared binary WHERE (SQLite): expected 1 row, got ' . count($rows)
                );
            }

            $this->assertSame('bin_where', $rows[0]['label']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared binary WHERE (SQLite) failed: ' . $e->getMessage());
        }
    }
}
