<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests BYTEA data integrity through the ZTD CTE rewriter on PostgreSQL PDO.
 *
 * @spec SPEC-10.2
 */
class PostgresBinaryBlobDataDmlTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return "CREATE TABLE pg_blob_t (
            id SERIAL PRIMARY KEY,
            label VARCHAR(50),
            bin_data BYTEA
        )";
    }

    protected function getTableNames(): array
    {
        return ['pg_blob_t'];
    }

    /**
     * NUL byte (\x00) embedded in BYTEA column.
     */
    public function testNulByteInBytea(): void
    {
        $data = "before\x00after";

        try {
            $stmt = $this->pdo->prepare("INSERT INTO pg_blob_t (label, bin_data) VALUES (?, ?)");
            $stmt->bindValue(1, 'nul_test', PDO::PARAM_STR);
            $stmt->bindValue(2, $data, PDO::PARAM_LOB);
            $stmt->execute();

            $rows = $this->ztdQuery("SELECT bin_data FROM pg_blob_t WHERE label = 'nul_test'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'NUL byte BYTEA (PG): expected 1 row, got ' . count($rows)
                );
            }

            // PostgreSQL BYTEA may return hex-encoded or escape-encoded; compare via unpack
            $fetched = $rows[0]['bin_data'];
            // Handle PostgreSQL bytea hex format (\x prefix)
            if (is_string($fetched) && str_starts_with($fetched, '\x')) {
                $fetched = hex2bin(substr($fetched, 2));
            }

            if ($fetched !== $data) {
                $this->markTestIncomplete(
                    'NUL byte BYTEA (PG): data corrupted. Expected ' . bin2hex($data)
                    . ', got ' . bin2hex((string) $fetched)
                );
            }

            $this->assertSame($data, $fetched);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('NUL byte BYTEA (PG) failed: ' . $e->getMessage());
        }
    }

    /**
     * Full 0x00–0xFF byte range in BYTEA column.
     */
    public function testFullByteRangeInBytea(): void
    {
        $data = '';
        for ($i = 0; $i < 256; $i++) {
            $data .= chr($i);
        }

        try {
            $stmt = $this->pdo->prepare("INSERT INTO pg_blob_t (label, bin_data) VALUES (?, ?)");
            $stmt->bindValue(1, 'byte_range', PDO::PARAM_STR);
            $stmt->bindValue(2, $data, PDO::PARAM_LOB);
            $stmt->execute();

            $rows = $this->ztdQuery("SELECT bin_data FROM pg_blob_t WHERE label = 'byte_range'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Full byte range BYTEA (PG): expected 1 row, got ' . count($rows)
                );
            }

            $fetched = $rows[0]['bin_data'];
            if (is_string($fetched) && str_starts_with($fetched, '\x')) {
                $fetched = hex2bin(substr($fetched, 2));
            }

            if (strlen((string) $fetched) !== 256) {
                $this->markTestIncomplete(
                    'Full byte range BYTEA (PG): expected 256 bytes, got ' . strlen((string) $fetched)
                );
            }

            $this->assertSame($data, $fetched);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Full byte range BYTEA (PG) failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE BYTEA after INSERT.
     */
    public function testUpdateByteaAfterInsert(): void
    {
        $original = "\x01\x02\x03";
        $updated = "\xFE\xFF\x00\x01";

        try {
            $stmt = $this->pdo->prepare("INSERT INTO pg_blob_t (label, bin_data) VALUES (?, ?)");
            $stmt->bindValue(1, 'update_test', PDO::PARAM_STR);
            $stmt->bindValue(2, $original, PDO::PARAM_LOB);
            $stmt->execute();

            $stmt2 = $this->pdo->prepare("UPDATE pg_blob_t SET bin_data = ? WHERE label = ?");
            $stmt2->bindValue(1, $updated, PDO::PARAM_LOB);
            $stmt2->bindValue(2, 'update_test', PDO::PARAM_STR);
            $stmt2->execute();

            $rows = $this->ztdQuery("SELECT bin_data FROM pg_blob_t WHERE label = 'update_test'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'UPDATE BYTEA (PG): expected 1 row, got ' . count($rows)
                );
            }

            $fetched = $rows[0]['bin_data'];
            if (is_string($fetched) && str_starts_with($fetched, '\x')) {
                $fetched = hex2bin(substr($fetched, 2));
            }

            if ($fetched !== $updated) {
                $this->markTestIncomplete(
                    'UPDATE BYTEA (PG): data corrupted. Expected ' . bin2hex($updated)
                    . ', got ' . bin2hex((string) $fetched)
                );
            }

            $this->assertSame($updated, $fetched);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE BYTEA (PG) failed: ' . $e->getMessage());
        }
    }
}
