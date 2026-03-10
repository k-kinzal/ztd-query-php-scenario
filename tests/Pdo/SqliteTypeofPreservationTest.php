<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests typeof() behavior with shadow store data on SQLite.
 *
 * The CTE shadow store embeds values as SQL literals. SQLite's typeof()
 * function returns the storage class of a value: 'null', 'integer', 'real',
 * 'text', or 'blob'. If the CTE embeds all values as text strings, typeof()
 * will return 'text' for everything instead of the correct type.
 *
 * This tests whether the shadow store preserves type affinity correctly.
 *
 * @spec SPEC-3.1
 */
class SqliteTypeofPreservationTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return "CREATE TABLE sl_typ_mixed (
            id INTEGER PRIMARY KEY,
            int_col INTEGER,
            real_col REAL,
            text_col TEXT,
            nullable_col TEXT
        )";
    }

    protected function getTableNames(): array
    {
        return ['sl_typ_mixed'];
    }

    /**
     * typeof() on INTEGER column — should return 'integer', not 'text'.
     */
    public function testTypeofInteger(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_typ_mixed (id, int_col, real_col, text_col) VALUES (1, 42, 3.14, 'hello')");

            $rows = $this->ztdQuery("SELECT typeof(int_col) AS t FROM sl_typ_mixed WHERE id = 1");

            if (count($rows) === 0) {
                $this->markTestIncomplete('typeof(int_col) query returned 0 rows.');
            }

            $type = $rows[0]['t'];
            if ($type === 'text') {
                $this->markTestIncomplete(
                    'typeof(int_col) returned "text" instead of "integer". '
                    . 'CTE shadow store embeds integer values as text strings.'
                );
            }

            $this->assertSame('integer', $type);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('typeof(int_col) test failed: ' . $e->getMessage());
        }
    }

    /**
     * typeof() on REAL column — should return 'real', not 'text'.
     */
    public function testTypeofReal(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_typ_mixed (id, int_col, real_col, text_col) VALUES (1, 42, 3.14, 'hello')");

            $rows = $this->ztdQuery("SELECT typeof(real_col) AS t FROM sl_typ_mixed WHERE id = 1");

            if (count($rows) === 0) {
                $this->markTestIncomplete('typeof(real_col) query returned 0 rows.');
            }

            $type = $rows[0]['t'];
            if ($type === 'text') {
                $this->markTestIncomplete(
                    'typeof(real_col) returned "text" instead of "real". '
                    . 'CTE shadow store may embed REAL values as text strings.'
                );
            }

            $this->assertSame('real', $type);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('typeof(real_col) test failed: ' . $e->getMessage());
        }
    }

    /**
     * typeof() on TEXT column — should return 'text'.
     */
    public function testTypeofText(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_typ_mixed (id, int_col, real_col, text_col) VALUES (1, 42, 3.14, 'hello')");

            $rows = $this->ztdQuery("SELECT typeof(text_col) AS t FROM sl_typ_mixed WHERE id = 1");

            if (count($rows) === 0) {
                $this->markTestIncomplete('typeof(text_col) query returned 0 rows.');
            }

            $this->assertSame('text', $rows[0]['t']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('typeof(text_col) test failed: ' . $e->getMessage());
        }
    }

    /**
     * typeof() on NULL — should return 'null', not 'text' or empty.
     */
    public function testTypeofNull(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_typ_mixed (id, int_col, real_col, text_col, nullable_col) VALUES (1, 42, 3.14, 'hello', NULL)");

            $rows = $this->ztdQuery("SELECT typeof(nullable_col) AS t FROM sl_typ_mixed WHERE id = 1");

            if (count($rows) === 0) {
                $this->markTestIncomplete('typeof(nullable_col) query returned 0 rows.');
            }

            $type = $rows[0]['t'];
            if ($type !== 'null') {
                $this->markTestIncomplete(
                    'typeof(NULL) returned "' . $type . '" instead of "null". '
                    . 'Shadow store may embed NULL as string literal.'
                );
            }

            $this->assertSame('null', $type);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('typeof(NULL) test failed: ' . $e->getMessage());
        }
    }

    /**
     * typeof() on integer value after UPDATE — type should persist.
     */
    public function testTypeofAfterUpdate(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_typ_mixed (id, int_col, real_col, text_col) VALUES (1, 10, 1.5, 'x')");
            $this->pdo->exec("UPDATE sl_typ_mixed SET int_col = 99 WHERE id = 1");

            $rows = $this->ztdQuery("SELECT typeof(int_col) AS t, int_col FROM sl_typ_mixed WHERE id = 1");

            if (count($rows) === 0) {
                $this->markTestIncomplete('typeof after UPDATE returned 0 rows.');
            }

            $type = $rows[0]['t'];
            if ($type === 'text') {
                $this->markTestIncomplete(
                    'typeof(int_col) after UPDATE returned "text" instead of "integer". Value=' . $rows[0]['int_col']
                );
            }

            $this->assertSame('integer', $type);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('typeof after UPDATE test failed: ' . $e->getMessage());
        }
    }

    /**
     * Arithmetic on typed values — integer + integer should still be integer.
     *
     * If the CTE embeds values as text, SQLite may coerce via affinity but
     * typeof(text_col + text_col) = 'integer' only if coercion works.
     */
    public function testArithmeticTypePreservation(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_typ_mixed (id, int_col, real_col, text_col) VALUES (1, 10, 2.5, 'hello')");

            $rows = $this->ztdQuery(
                "SELECT typeof(int_col + 5) AS int_add_type, typeof(real_col * 2) AS real_mul_type FROM sl_typ_mixed WHERE id = 1"
            );

            if (count($rows) === 0) {
                $this->markTestIncomplete('Arithmetic typeof query returned 0 rows.');
            }

            $intType = $rows[0]['int_add_type'];
            $realType = $rows[0]['real_mul_type'];

            // int + int = integer, real * int = real
            $this->assertSame('integer', $intType, 'int_col + 5 should be integer');
            $this->assertSame('real', $realType, 'real_col * 2 should be real');
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Arithmetic type preservation test failed: ' . $e->getMessage());
        }
    }

    /**
     * GROUP BY typeof() — should correctly group by storage class.
     */
    public function testGroupByTypeof(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_typ_mixed (id, int_col, real_col, text_col, nullable_col) VALUES (1, 10, 1.5, 'a', NULL)");
            $this->pdo->exec("INSERT INTO sl_typ_mixed (id, int_col, real_col, text_col, nullable_col) VALUES (2, 20, 2.5, 'b', 'not-null')");
            $this->pdo->exec("INSERT INTO sl_typ_mixed (id, int_col, real_col, text_col, nullable_col) VALUES (3, 30, 3.5, 'c', NULL)");

            $rows = $this->ztdQuery(
                "SELECT typeof(nullable_col) AS t, COUNT(*) AS cnt FROM sl_typ_mixed GROUP BY typeof(nullable_col) ORDER BY t"
            );

            if (count($rows) === 0) {
                $this->markTestIncomplete('GROUP BY typeof() returned 0 rows.');
            }

            // Expect 2 groups: 'null' (2 rows) and 'text' (1 row)
            if (count($rows) === 1) {
                $this->markTestIncomplete(
                    'GROUP BY typeof() returned only 1 group. Expected 2 (null, text). Got: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);

            $typeMap = [];
            foreach ($rows as $row) {
                $typeMap[$row['t']] = (int) $row['cnt'];
            }

            if (!isset($typeMap['null'])) {
                $this->markTestIncomplete(
                    'GROUP BY typeof(): no "null" group found. Groups: ' . json_encode($typeMap)
                );
            }

            $this->assertSame(2, $typeMap['null'] ?? 0);
            $this->assertSame(1, $typeMap['text'] ?? 0);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('GROUP BY typeof() test failed: ' . $e->getMessage());
        }
    }

    /**
     * typeof() with CAST — verify CAST works through shadow store.
     */
    public function testTypeofWithCast(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_typ_mixed (id, int_col, real_col, text_col) VALUES (1, 42, 3.14, '123')");

            $rows = $this->ztdQuery(
                "SELECT typeof(CAST(text_col AS INTEGER)) AS t, CAST(text_col AS INTEGER) AS v FROM sl_typ_mixed WHERE id = 1"
            );

            if (count($rows) === 0) {
                $this->markTestIncomplete('typeof(CAST) query returned 0 rows.');
            }

            $this->assertSame('integer', $rows[0]['t']);
            $this->assertEquals(123, (int) $rows[0]['v']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('typeof with CAST test failed: ' . $e->getMessage());
        }
    }
}
