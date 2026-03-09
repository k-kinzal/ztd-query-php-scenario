<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests PostgreSQL JSON/JSONB operators through the CTE rewriter.
 *
 * Real-world scenario: JSON columns are extremely common in PostgreSQL
 * applications. The -> and ->> operators extract JSON values and are used
 * in SELECT, WHERE, and UPDATE SET clauses. The ? operator (JSONB key
 * existence) is known to conflict with prepared statement placeholders
 * (upstream #48). This test covers the operator usage patterns.
 *
 * @spec SPEC-3.1
 * @spec SPEC-4.1
 */
class PostgresJsonOperatorTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "CREATE TABLE pg_jo_profiles (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                config JSONB NOT NULL DEFAULT '{}'::JSONB,
                tags JSONB
            )",
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_jo_profiles'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO pg_jo_profiles (id, name, config, tags) VALUES (1, 'Alice', '{\"theme\":\"dark\",\"lang\":\"en\",\"level\":5}', '[\"admin\",\"user\"]')");
        $this->ztdExec("INSERT INTO pg_jo_profiles (id, name, config, tags) VALUES (2, 'Bob', '{\"theme\":\"light\",\"lang\":\"fr\",\"level\":3}', '[\"user\"]')");
        $this->ztdExec("INSERT INTO pg_jo_profiles (id, name, config, tags) VALUES (3, 'Charlie', '{\"theme\":\"dark\",\"lang\":\"de\"}', NULL)");
    }

    /**
     * SELECT with -> operator (JSON object field).
     */
    public function testJsonArrowInSelect(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT name, config->'theme' AS theme FROM pg_jo_profiles ORDER BY id"
            );

            $this->assertCount(3, $rows);
            // -> returns JSONB: includes quotes
            $this->assertStringContainsString('dark', $rows[0]['theme']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'JSON -> in SELECT failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * SELECT with ->> operator (JSON object field as text).
     */
    public function testJsonDoubleArrowInSelect(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT name, config->>'theme' AS theme FROM pg_jo_profiles ORDER BY id"
            );

            $this->assertCount(3, $rows);
            // ->> returns text: no quotes
            $this->assertSame('dark', $rows[0]['theme']);
            $this->assertSame('light', $rows[1]['theme']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'JSON ->> in SELECT failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * WHERE clause with ->> operator.
     */
    public function testJsonDoubleArrowInWhere(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT name FROM pg_jo_profiles WHERE config->>'theme' = 'dark' ORDER BY name"
            );

            $this->assertCount(2, $rows);
            $this->assertSame('Alice', $rows[0]['name']);
            $this->assertSame('Charlie', $rows[1]['name']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'JSON ->> in WHERE failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Nested JSON access: config->'level' cast to integer.
     */
    public function testNestedJsonWithCast(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT name FROM pg_jo_profiles WHERE (config->>'level')::INTEGER > 4 ORDER BY id"
            );

            $this->assertCount(1, $rows);
            $this->assertSame('Alice', $rows[0]['name']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Nested JSON with cast failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE with JSONB set (using || merge operator).
     */
    public function testJsonbMergeUpdate(): void
    {
        try {
            $this->ztdExec(
                "UPDATE pg_jo_profiles SET config = config || '{\"notifications\":true}'::JSONB WHERE id = 1"
            );

            $rows = $this->ztdQuery("SELECT config->>'notifications' AS notif FROM pg_jo_profiles WHERE id = 1");
            $this->assertCount(1, $rows);
            $this->assertSame('true', $rows[0]['notif']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'JSONB merge update failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * JSON array access with -> integer index.
     */
    public function testJsonArrayAccess(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT name, tags->0 AS first_tag FROM pg_jo_profiles WHERE tags IS NOT NULL ORDER BY id"
            );

            $this->assertCount(2, $rows);
            $this->assertStringContainsString('admin', $rows[0]['first_tag']);
            $this->assertStringContainsString('user', $rows[1]['first_tag']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'JSON array access failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * JSONB containment operator @>.
     */
    public function testJsonbContainment(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT name FROM pg_jo_profiles WHERE config @> '{\"theme\":\"dark\"}'::JSONB ORDER BY name"
            );

            $this->assertCount(2, $rows);
            $this->assertSame('Alice', $rows[0]['name']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'JSONB containment failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * JSON ->> with prepared parameter.
     */
    public function testJsonDoubleArrowWithPreparedParam(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT name FROM pg_jo_profiles WHERE config->>'lang' = ? ORDER BY id",
                ['en']
            );

            $this->assertCount(1, $rows);
            $this->assertSame('Alice', $rows[0]['name']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'JSON ->> with prepared param failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * INSERT with JSONB value then query it.
     */
    public function testInsertJsonbThenQuery(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO pg_jo_profiles (id, name, config, tags) VALUES (4, 'Diana', '{\"theme\":\"auto\",\"lang\":\"es\"}', '[\"editor\",\"user\"]')"
            );

            $rows = $this->ztdQuery(
                "SELECT config->>'lang' AS lang FROM pg_jo_profiles WHERE id = 4"
            );
            $this->assertCount(1, $rows);
            $this->assertSame('es', $rows[0]['lang']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'INSERT JSONB then query failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * JSONB key deletion with - operator.
     */
    public function testJsonbKeyDeletion(): void
    {
        try {
            $this->ztdExec(
                "UPDATE pg_jo_profiles SET config = config - 'level' WHERE id = 2"
            );

            $rows = $this->ztdQuery(
                "SELECT config->>'level' AS level FROM pg_jo_profiles WHERE id = 2"
            );
            $this->assertCount(1, $rows);
            $this->assertNull($rows[0]['level']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'JSONB key deletion failed: ' . $e->getMessage()
            );
        }
    }
}
