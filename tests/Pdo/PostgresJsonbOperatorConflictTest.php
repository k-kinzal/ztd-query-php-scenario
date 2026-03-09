<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests PostgreSQL JSONB operators that contain ? character through CTE shadow store.
 *
 * PostgreSQL JSONB has several operators that use the ? character:
 *   ?   — key exists
 *   ?|  — any key exists
 *   ?&  — all keys exist
 *
 * The CTE rewriter treats ? as a prepared-statement parameter placeholder and
 * converts it to $N, producing invalid SQL. This affects both query() and prepare().
 *
 * Workarounds: use jsonb_exists(), jsonb_exists_any(), jsonb_exists_all() functions.
 *
 * @spec SPEC-11.PG-JSONB-QUESTION-MARK
 */
class PostgresJsonbOperatorConflictTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "CREATE TABLE pg_jop_docs (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                meta JSONB NOT NULL
            )",
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_jop_docs'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_jop_docs VALUES (1, 'Doc A', '{\"author\": \"Alice\", \"reviewed\": true, \"tags\": [\"draft\", \"urgent\"]}')");
        $this->pdo->exec("INSERT INTO pg_jop_docs VALUES (2, 'Doc B', '{\"author\": \"Bob\", \"tags\": [\"final\"]}')");
        $this->pdo->exec("INSERT INTO pg_jop_docs VALUES (3, 'Doc C', '{\"author\": \"Carol\", \"reviewed\": false, \"priority\": \"high\", \"tags\": [\"draft\"]}')");
    }

    /**
     * JSONB ? operator (key exists) — fails through CTE rewriter.
     * The ? is converted to $1 parameter placeholder.
     */
    public function testQuestionMarkOperatorFails(): void
    {
        $this->expectException(\PDOException::class);
        $this->expectExceptionMessageMatches('/syntax error/');

        $this->ztdQuery(
            "SELECT name FROM pg_jop_docs WHERE meta ? 'reviewed' ORDER BY id"
        );
    }

    /**
     * Workaround for ? operator: jsonb_exists() function.
     */
    public function testJsonbExistsWorkaround(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name FROM pg_jop_docs WHERE jsonb_exists(meta, 'reviewed') ORDER BY id"
        );

        $this->assertCount(2, $rows);
        $this->assertSame('Doc A', $rows[0]['name']);
        $this->assertSame('Doc C', $rows[1]['name']);
    }

    /**
     * JSONB ?| operator (any key exists) — fails through CTE rewriter.
     */
    public function testQuestionPipeOperatorFails(): void
    {
        $this->expectException(\PDOException::class);

        $this->ztdQuery(
            "SELECT name FROM pg_jop_docs WHERE meta ?| array['reviewed', 'priority'] ORDER BY id"
        );
    }

    /**
     * Workaround for ?| operator: jsonb_exists_any() function.
     */
    public function testJsonbExistsAnyWorkaround(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name FROM pg_jop_docs WHERE jsonb_exists_any(meta, array['reviewed', 'priority']) ORDER BY id"
        );

        $this->assertCount(2, $rows);
        $this->assertSame('Doc A', $rows[0]['name']);
        $this->assertSame('Doc C', $rows[1]['name']);
    }

    /**
     * JSONB ?& operator (all keys exist) — fails through CTE rewriter.
     */
    public function testQuestionAmpOperatorFails(): void
    {
        $this->expectException(\PDOException::class);

        $this->ztdQuery(
            "SELECT name FROM pg_jop_docs WHERE meta ?& array['author', 'reviewed'] ORDER BY id"
        );
    }

    /**
     * Workaround for ?& operator: jsonb_exists_all() function.
     */
    public function testJsonbExistsAllWorkaround(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name FROM pg_jop_docs WHERE jsonb_exists_all(meta, array['author', 'reviewed']) ORDER BY id"
        );

        $this->assertCount(2, $rows);
        $this->assertSame('Doc A', $rows[0]['name']);
        $this->assertSame('Doc C', $rows[1]['name']);
    }

    /**
     * Non-? JSONB operators work correctly: ->, ->>, @>, <@.
     */
    public function testNonQuestionMarkOperatorsWork(): void
    {
        // ->> operator
        $rows = $this->ztdQuery(
            "SELECT name FROM pg_jop_docs WHERE meta->>'author' = 'Alice'"
        );
        $this->assertCount(1, $rows);
        $this->assertSame('Doc A', $rows[0]['name']);

        // @> containment
        $rows = $this->ztdQuery(
            "SELECT name FROM pg_jop_docs WHERE meta @> '{\"author\": \"Bob\"}'"
        );
        $this->assertCount(1, $rows);
        $this->assertSame('Doc B', $rows[0]['name']);

        // <@ contained-by
        $rows = $this->ztdQuery(
            "SELECT name FROM pg_jop_docs
             WHERE meta <@ '{\"author\": \"Bob\", \"tags\": [\"final\"], \"extra\": true}'
             ORDER BY id"
        );
        $this->assertCount(1, $rows);
        $this->assertSame('Doc B', $rows[0]['name']);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) FROM pg_jop_docs")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['count']);
    }
}
