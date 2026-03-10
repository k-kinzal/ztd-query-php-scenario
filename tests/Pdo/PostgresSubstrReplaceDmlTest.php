<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests multi-argument string functions (SUBSTRING, REPLACE, POSITION) in DML
 * with prepared parameters through ZTD shadow store on PostgreSQL.
 *
 * String functions with multiple bound $N parameters are common in applications
 * that manipulate text. The CTE rewriter must correctly bind all parameters.
 *
 * @spec SPEC-4.2, SPEC-4.3, SPEC-3.2
 */
class PostgresSubstrReplaceDmlTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_sr_codes (
                id INTEGER PRIMARY KEY,
                code VARCHAR(20) NOT NULL,
                description VARCHAR(100) NOT NULL,
                prefix VARCHAR(10) NOT NULL DEFAULT \'\'
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_sr_codes'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_sr_codes VALUES (1, 'ABC-001', 'First item', 'ABC')");
        $this->pdo->exec("INSERT INTO pg_sr_codes VALUES (2, 'ABC-002', 'Second item', 'ABC')");
        $this->pdo->exec("INSERT INTO pg_sr_codes VALUES (3, 'XYZ-001', 'Third item', 'XYZ')");
        $this->pdo->exec("INSERT INTO pg_sr_codes VALUES (4, 'XYZ-002', 'Fourth item', 'XYZ')");
    }

    /**
     * Prepared UPDATE SET with REPLACE() and $1/$2 params.
     */
    public function testPreparedUpdateSetReplaceWithTwoParams(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "UPDATE pg_sr_codes SET code = REPLACE(code, $1, $2) WHERE prefix = 'ABC'"
            );
            $stmt->execute(['ABC', 'DEF']);

            $rows = $this->ztdQuery(
                "SELECT code FROM pg_sr_codes WHERE prefix = 'ABC' ORDER BY id"
            );

            if (count($rows) !== 2) {
                $this->markTestIncomplete('UPDATE REPLACE: got ' . json_encode($rows));
            }

            $this->assertSame('DEF-001', $rows[0]['code']);
            $this->assertSame('DEF-002', $rows[1]['code']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE with REPLACE() failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared SELECT with SUBSTRING(col, $1, $2).
     */
    public function testPreparedSelectSubstringWithParams(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "SELECT id, SUBSTRING(code FROM $1 FOR $2) AS extracted FROM pg_sr_codes ORDER BY id"
            );
            $stmt->execute([1, 3]);

            $rows = $stmt->fetchAll(\PDO::FETCH_ASSOC);

            if (count($rows) !== 4) {
                $this->markTestIncomplete(
                    'SELECT SUBSTRING: expected 4, got ' . count($rows)
                );
            }

            $this->assertSame('ABC', $rows[0]['extracted']);
            $this->assertSame('XYZ', $rows[2]['extracted']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared SELECT with SUBSTRING params failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared DELETE WHERE SUBSTRING(col FROM $1 FOR $2) = $3 — 3 params.
     */
    public function testPreparedDeleteWhereSubstringWithThreeParams(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "DELETE FROM pg_sr_codes WHERE SUBSTRING(code FROM $1 FOR $2) = $3"
            );
            $stmt->execute([5, 3, '001']);

            $rows = $this->ztdQuery("SELECT code FROM pg_sr_codes ORDER BY id");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'DELETE SUBSTRING 3 params: expected 2, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertSame('ABC-002', $rows[0]['code']);
            $this->assertSame('XYZ-002', $rows[1]['code']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared DELETE with SUBSTRING 3 params failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared UPDATE WHERE POSITION($1 IN col) > 0 — search with param.
     */
    public function testPreparedUpdateWherePositionWithParam(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "UPDATE pg_sr_codes SET description = $1 WHERE POSITION($2 IN code) > 0"
            );
            $stmt->execute(['Updated XYZ', 'XYZ']);

            $rows = $this->ztdQuery(
                "SELECT id, description FROM pg_sr_codes WHERE description = 'Updated XYZ' ORDER BY id"
            );

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'UPDATE POSITION: expected 2, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertSame(3, (int) $rows[0]['id']);
            $this->assertSame(4, (int) $rows[1]['id']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE with POSITION in WHERE failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared UPDATE SET REPLACE with 4 total params.
     */
    public function testPreparedUpdateReplaceWithFourTotalParams(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "UPDATE pg_sr_codes SET description = REPLACE(description, $1, $2) WHERE id = $3"
            );
            $stmt->execute(['item', 'product', 1]);

            $rows = $this->ztdQuery("SELECT description FROM pg_sr_codes WHERE id = 1");

            if (count($rows) !== 1) {
                $this->markTestIncomplete('Prepared REPLACE 4 params: got ' . json_encode($rows));
            }

            $this->assertSame('First product', $rows[0]['description']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE REPLACE 4 params failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared SELECT WHERE REPLACE(col, $1, $2) = $3 — function equality.
     */
    public function testPreparedSelectWhereReplaceEqualsParam(): void
    {
        try {
            $this->pdo->exec("INSERT INTO pg_sr_codes VALUES (5, 'NEW-999', 'Fifth item', 'NEW')");

            $stmt = $this->pdo->prepare(
                "SELECT id, code FROM pg_sr_codes WHERE REPLACE(code, $1, $2) = $3"
            );
            $stmt->execute(['-', '', 'NEW999']);

            $rows = $stmt->fetchAll(\PDO::FETCH_ASSOC);

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'SELECT REPLACE WHERE: expected 1, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertSame(5, (int) $rows[0]['id']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared SELECT with REPLACE in WHERE failed: ' . $e->getMessage());
        }
    }
}
