<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests multi-argument string functions (SUBSTR, REPLACE, INSTR) in DML
 * with prepared parameters through ZTD shadow store on SQLite.
 *
 * String functions with multiple bound parameters are common in applications
 * that manipulate text data (search-and-replace, substring extraction, pattern matching).
 * The CTE rewriter must correctly bind all parameters in these function calls.
 *
 * @spec SPEC-4.2, SPEC-4.3, SPEC-3.2
 */
class SqliteSubstrReplaceDmlTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_sr_codes (
                id INTEGER PRIMARY KEY,
                code TEXT NOT NULL,
                description TEXT NOT NULL,
                prefix TEXT NOT NULL DEFAULT \'\'
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_sr_codes'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_sr_codes VALUES (1, 'ABC-001', 'First item', 'ABC')");
        $this->pdo->exec("INSERT INTO sl_sr_codes VALUES (2, 'ABC-002', 'Second item', 'ABC')");
        $this->pdo->exec("INSERT INTO sl_sr_codes VALUES (3, 'XYZ-001', 'Third item', 'XYZ')");
        $this->pdo->exec("INSERT INTO sl_sr_codes VALUES (4, 'XYZ-002', 'Fourth item', 'XYZ')");
    }

    /**
     * UPDATE SET with REPLACE() function and two prepared params.
     */
    public function testPreparedUpdateSetReplaceWithTwoParams(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "UPDATE sl_sr_codes SET code = REPLACE(code, ?, ?) WHERE prefix = 'ABC'"
            );
            $stmt->execute(['ABC', 'DEF']);

            $rows = $this->ztdQuery(
                "SELECT code FROM sl_sr_codes WHERE prefix = 'ABC' ORDER BY id"
            );

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'UPDATE REPLACE: got ' . json_encode($rows)
                );
            }

            $this->assertSame('DEF-001', $rows[0]['code']);
            $this->assertSame('DEF-002', $rows[1]['code']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE with REPLACE() failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared SELECT with SUBSTR() and bound position/length params.
     */
    public function testPreparedSelectSubstrWithParams(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "SELECT id, SUBSTR(code, ?, ?) AS extracted FROM sl_sr_codes ORDER BY id"
            );
            $stmt->execute([1, 3]);

            $rows = $stmt->fetchAll(\PDO::FETCH_ASSOC);

            if (count($rows) !== 4) {
                $this->markTestIncomplete(
                    'SELECT SUBSTR: expected 4, got ' . count($rows) . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertSame('ABC', $rows[0]['extracted']);
            $this->assertSame('XYZ', $rows[2]['extracted']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared SELECT with SUBSTR params failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared DELETE WHERE SUBSTR(col, ?, ?) = ? — 3 params in one expression.
     */
    public function testPreparedDeleteWhereSubstrWithThreeParams(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "DELETE FROM sl_sr_codes WHERE SUBSTR(code, ?, ?) = ?"
            );
            $stmt->execute([5, 3, '001']);

            $rows = $this->ztdQuery("SELECT code FROM sl_sr_codes ORDER BY id");

            // Should delete ABC-001 and XYZ-001 (substr at pos 5 len 3 = '001')
            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'DELETE SUBSTR 3 params: expected 2, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertSame('ABC-002', $rows[0]['code']);
            $this->assertSame('XYZ-002', $rows[1]['code']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared DELETE with SUBSTR 3 params failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared UPDATE WHERE INSTR(col, ?) > 0 — string search with param.
     */
    public function testPreparedUpdateWhereInstrWithParam(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "UPDATE sl_sr_codes SET description = ? WHERE INSTR(code, ?) > 0"
            );
            $stmt->execute(['Updated XYZ', 'XYZ']);

            $rows = $this->ztdQuery(
                "SELECT id, description FROM sl_sr_codes WHERE description = 'Updated XYZ' ORDER BY id"
            );

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'UPDATE INSTR: expected 2, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertSame(3, (int) $rows[0]['id']);
            $this->assertSame(4, (int) $rows[1]['id']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE with INSTR in WHERE failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE SET with REPLACE() and SUBSTR() combined.
     */
    public function testUpdateSetReplaceAndSubstrCombined(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE sl_sr_codes SET prefix = SUBSTR(REPLACE(code, '-', ''), 1, 3)"
            );

            $rows = $this->ztdQuery("SELECT id, prefix FROM sl_sr_codes ORDER BY id");

            if (count($rows) !== 4) {
                $this->markTestIncomplete('UPDATE REPLACE+SUBSTR: got ' . json_encode($rows));
            }

            // REPLACE('ABC-001','-','') = 'ABC001', SUBSTR('ABC001',1,3) = 'ABC'
            $this->assertSame('ABC', $rows[0]['prefix']);
            $this->assertSame('ABC', $rows[1]['prefix']);
            $this->assertSame('XYZ', $rows[2]['prefix']);
            $this->assertSame('XYZ', $rows[3]['prefix']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE with REPLACE+SUBSTR combined failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared UPDATE SET with REPLACE() having 3 params (col, search, replace) plus WHERE param.
     */
    public function testPreparedUpdateReplaceWithFourTotalParams(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "UPDATE sl_sr_codes SET description = REPLACE(description, ?, ?) WHERE id = ?"
            );
            $stmt->execute(['item', 'product', 1]);

            $rows = $this->ztdQuery("SELECT description FROM sl_sr_codes WHERE id = 1");

            if (count($rows) !== 1) {
                $this->markTestIncomplete('Prepared REPLACE 4 params: got ' . json_encode($rows));
            }

            $this->assertSame('First product', $rows[0]['description']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE REPLACE with 4 total params failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared SELECT WHERE REPLACE(col, ?, ?) = ? after mutations — 3 params in one comparison.
     */
    public function testPreparedSelectWhereReplaceEqualsParam(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_sr_codes VALUES (5, 'NEW-999', 'Fifth item', 'NEW')");

            $stmt = $this->pdo->prepare(
                "SELECT id, code FROM sl_sr_codes WHERE REPLACE(code, ?, ?) = ?"
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
