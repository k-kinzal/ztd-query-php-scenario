<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests schema-qualified table names (e.g., public.tablename) on PostgreSQL ZTD.
 *
 * The PgSqlParser strips schema prefixes via stripSchemaPrefix(),
 * so `public.tablename` resolves to `tablename` in the shadow store for mutations.
 *
 * Limitation: The CTE rewriter does NOT recognize schema-qualified table names
 * in SELECT queries. INSERT/UPDATE/DELETE work because the mutation resolver
 * strips the schema prefix, but SELECT FROM public.tablename returns empty
 * because the CTE shadowing only matches unqualified table names.
 * @spec SPEC-1.6
 */
class PostgresSchemaQualifiedTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pg_sq_test (id INT PRIMARY KEY, name VARCHAR(50), score INT)';
    }

    protected function getTableNames(): array
    {
        return ['pg_sq_test'];
    }


    public function testInsertWithSchemaQualifiedName(): void
    {
        $this->pdo->exec("INSERT INTO public.pg_sq_test VALUES (1, 'Alice', 90)");

        $stmt = $this->pdo->query('SELECT name FROM pg_sq_test WHERE id = 1');
        $this->assertSame('Alice', $stmt->fetchColumn());
    }

    /**
     * SELECT with schema-qualified name does NOT return shadow data.
     * The CTE rewriter does not recognize public.tablename as a shadow table.
     */
    public function testSelectWithSchemaQualifiedNameReturnsEmpty(): void
    {
        $this->pdo->exec("INSERT INTO pg_sq_test VALUES (1, 'Alice', 90)");

        $stmt = $this->pdo->query('SELECT name FROM public.pg_sq_test WHERE id = 1');
        $this->assertFalse($stmt->fetchColumn());
    }

    public function testUpdateWithSchemaQualifiedName(): void
    {
        $this->pdo->exec("INSERT INTO pg_sq_test VALUES (1, 'Alice', 90)");
        $this->pdo->exec("UPDATE public.pg_sq_test SET score = 95 WHERE id = 1");

        $stmt = $this->pdo->query('SELECT score FROM pg_sq_test WHERE id = 1');
        $this->assertEquals(95, $stmt->fetchColumn());
    }

    public function testDeleteWithSchemaQualifiedName(): void
    {
        $this->pdo->exec("INSERT INTO pg_sq_test VALUES (1, 'Alice', 90)");
        $this->pdo->exec("INSERT INTO pg_sq_test VALUES (2, 'Bob', 80)");
        $this->pdo->exec("DELETE FROM public.pg_sq_test WHERE id = 1");

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_sq_test');
        $this->assertSame(1, (int) $stmt->fetchColumn());
    }

    /**
     * Mixed qualified/unqualified: data is shared in shadow store (mutations strip prefix),
     * but SELECT with qualified name cannot see it (CTE rewriter limitation).
     */
    public function testMixedQualifiedAndUnqualifiedAccess(): void
    {
        $this->pdo->exec("INSERT INTO public.pg_sq_test VALUES (1, 'Alice', 90)");
        $this->pdo->exec("INSERT INTO pg_sq_test VALUES (2, 'Bob', 80)");

        // Unqualified SELECT sees both rows
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_sq_test');
        $this->assertSame(2, (int) $stmt->fetchColumn());

        // Schema-qualified SELECT returns empty (CTE rewriter limitation)
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM public.pg_sq_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }

    /**
     * Prepared SELECT with schema-qualified name also returns empty (same CTE limitation).
     */
    public function testPreparedWithSchemaQualifiedNameReturnsEmpty(): void
    {
        $this->pdo->exec("INSERT INTO pg_sq_test VALUES (1, 'Alice', 90)");

        $stmt = $this->pdo->prepare('SELECT name FROM public.pg_sq_test WHERE id = ?');
        $stmt->execute([1]);
        $this->assertFalse($stmt->fetchColumn());
    }

    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO public.pg_sq_test VALUES (1, 'Alice', 90)");

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM public.pg_sq_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
