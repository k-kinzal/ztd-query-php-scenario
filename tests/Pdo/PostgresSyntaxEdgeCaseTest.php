<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests PostgreSQL-specific syntax patterns that may stress the PgSqlParser's
 * regex-based parsing. These patterns embed SQL keywords (FROM, IN, FOR) inside
 * function arguments, or use operators (::, ||, ?) that could confuse a
 * regex-based SQL parser.
 *
 * Related known issue: TRIM(x FROM y) treats FROM as the SQL FROM keyword
 * (SPEC-11.PG-UPDATE-SET-FROM-KEYWORD / upstream issue #47).
 *
 * @spec SPEC-11.PG-SYNTAX-EDGE
 */
class PostgresSyntaxEdgeCaseTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pg_syn_data (
            id SERIAL PRIMARY KEY,
            label TEXT,
            value TEXT,
            num INT,
            created_at TIMESTAMP
        )';
    }

    protected function getTableNames(): array
    {
        return ['pg_syn_data'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_syn_data (id, label, value, num, created_at) VALUES (1, 'alpha', 'first',  10, '2025-01-15 09:00:00')");
        $this->pdo->exec("INSERT INTO pg_syn_data (id, label, value, num, created_at) VALUES (2, 'beta',  'second', 20, '2025-02-20 10:30:00')");
        $this->pdo->exec("INSERT INTO pg_syn_data (id, label, value, num, created_at) VALUES (3, 'gamma', '',       30, '2025-03-10 14:00:00')");
        $this->pdo->exec("INSERT INTO pg_syn_data (id, label, value, num, created_at) VALUES (4, 'delta', NULL,     40, '2025-04-05 16:45:00')");
        $this->pdo->exec("INSERT INTO pg_syn_data (id, label, value, num, created_at) VALUES (5, 'epsilon', 'fifth', 50, '2025-05-25 08:15:00')");
        $this->pdo->exec("INSERT INTO pg_syn_data (id, label, value, num, created_at) VALUES (6, 'zeta',  'sixth',  15, '2025-06-30 12:00:00')");
    }

    /**
     * PostgreSQL-style cast using :: operator.
     *
     * The :: operator is PostgreSQL-specific syntax for type casting.
     * A regex-based parser might not recognize :: and could misparse the query.
     *
     * @spec SPEC-11.PG-SYNTAX-EDGE.1
     */
    public function testCastWithDoubleColon(): void
    {
        $rows = $this->ztdQuery(
            "SELECT label, num::TEXT AS num_text FROM pg_syn_data WHERE id = 1"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('alpha', $rows[0]['label']);
        $this->assertSame('10', $rows[0]['num_text']);
    }

    /**
     * Array literal with ANY() construct.
     *
     * ARRAY[...] is PostgreSQL-specific. The parser must not be confused
     * by the brackets or the ANY keyword.
     *
     * @spec SPEC-11.PG-SYNTAX-EDGE.2
     */
    public function testArrayLiteralWithAny(): void
    {
        $rows = $this->ztdQuery(
            "SELECT label FROM pg_syn_data WHERE num = ANY(ARRAY[10, 30, 50]) ORDER BY id"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('alpha', $rows[0]['label']);
        $this->assertSame('gamma', $rows[1]['label']);
        $this->assertSame('epsilon', $rows[2]['label']);
    }

    /**
     * String concatenation with || operator.
     *
     * The || operator could be confused with logical OR in some parsers.
     *
     * @spec SPEC-11.PG-SYNTAX-EDGE.3
     */
    public function testStringConcatenationWithPipeOperator(): void
    {
        $rows = $this->ztdQuery(
            "SELECT label || ' - ' || value AS combined FROM pg_syn_data WHERE id = 1"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('alpha - first', $rows[0]['combined']);
    }

    /**
     * COALESCE with multiple arguments.
     *
     * Tests that the parser handles multi-argument function calls correctly,
     * including when some arguments are NULL.
     *
     * @spec SPEC-11.PG-SYNTAX-EDGE.4
     */
    public function testCoalesceWithMultipleArgs(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, COALESCE(value, label, 'default') AS resolved FROM pg_syn_data WHERE id IN (1, 3, 4) ORDER BY id"
        );

        $this->assertCount(3, $rows);
        // id=1: value='first' -> 'first'
        $this->assertSame('first', $rows[0]['resolved']);
        // id=3: value='' -> '' (empty string is not NULL)
        $this->assertSame('', $rows[1]['resolved']);
        // id=4: value=NULL -> falls through to label='delta'
        $this->assertSame('delta', $rows[2]['resolved']);
    }

    /**
     * NULLIF function.
     *
     * NULLIF(value, '') returns NULL when value equals the empty string.
     * Tests that the parser handles this two-argument function correctly.
     *
     * @spec SPEC-11.PG-SYNTAX-EDGE.5
     */
    public function testNullif(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, NULLIF(value, '') AS cleaned FROM pg_syn_data WHERE id IN (1, 3) ORDER BY id"
        );

        $this->assertCount(2, $rows);
        // id=1: value='first' != '' -> 'first'
        $this->assertSame('first', $rows[0]['cleaned']);
        // id=3: value='' == '' -> NULL
        $this->assertNull($rows[1]['cleaned']);
    }

    /**
     * POSITION(substring IN string) function.
     *
     * The POSITION function uses the IN keyword inside its arguments:
     *   POSITION('x' IN label)
     *
     * A regex-based parser might treat IN as the SQL IN operator and
     * misparse the query — similar to the TRIM(FROM) bug.
     *
     * @spec SPEC-11.PG-SYNTAX-EDGE.6
     */
    public function testPositionFunctionWithInKeyword(): void
    {
        $rows = $this->ztdQuery(
            "SELECT label, POSITION('a' IN label) AS pos FROM pg_syn_data WHERE id IN (1, 2) ORDER BY id"
        );

        $this->assertCount(2, $rows);
        // 'alpha': 'a' is at position 1
        $this->assertSame('alpha', $rows[0]['label']);
        $this->assertEquals(1, (int) $rows[0]['pos']);
        // 'beta': 'a' is at position 4
        $this->assertSame('beta', $rows[1]['label']);
        $this->assertEquals(4, (int) $rows[1]['pos']);
    }

    /**
     * OVERLAY(string PLACING replacement FROM start FOR count) function.
     *
     * The OVERLAY function contains the FROM keyword and FOR keyword inside
     * its arguments:
     *   OVERLAY(label PLACING 'X' FROM 1 FOR 1)
     *
     * This is the same class of bug as TRIM(x FROM y): the regex parser
     * may treat FROM as the SQL FROM clause keyword.
     *
     * @spec SPEC-11.PG-SYNTAX-EDGE.7
     */
    public function testOverlayFunctionWithFromKeyword(): void
    {
        $rows = $this->ztdQuery(
            "SELECT OVERLAY(label PLACING 'X' FROM 1 FOR 1) AS modified FROM pg_syn_data WHERE id = 1"
        );

        $this->assertCount(1, $rows);
        // 'alpha' with first character replaced by 'X' -> 'Xlpha'
        $this->assertSame('Xlpha', $rows[0]['modified']);
    }

    /**
     * Physical isolation: underlying table has no rows while ZTD sees all.
     *
     * @spec SPEC-11.PG-SYNTAX-EDGE.8
     */
    public function testPhysicalIsolation(): void
    {
        // ZTD sees all 6 seeded rows
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM pg_syn_data");
        $this->assertEquals(6, (int) $rows[0]['cnt']);

        // Physical table is untouched
        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM pg_syn_data")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
