<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests PostgreSQL TRUNCATE TABLE with various options.
 *
 * PostgreSQL supports: TRUNCATE [TABLE] [ONLY] name [RESTART IDENTITY | CONTINUE IDENTITY] [CASCADE | RESTRICT]
 * The PgSqlParser::extractTruncateTable() extracts the table name and the
 * TruncateMutation simply clears all rows regardless of options.
 * @spec pending
 */
class PostgresTruncateOptionsTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pg_trunc_test (id SERIAL PRIMARY KEY, name VARCHAR(50), score INT)';
    }

    protected function getTableNames(): array
    {
        return ['pg_trunc_test'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_trunc_test (id, name, score) VALUES (1, 'Alice', 90)");
        $this->pdo->exec("INSERT INTO pg_trunc_test (id, name, score) VALUES (2, 'Bob', 80)");
        $this->pdo->exec("INSERT INTO pg_trunc_test (id, name, score) VALUES (3, 'Charlie', 70)");
    }

    /**
     * Basic TRUNCATE TABLE.
     */
    public function testBasicTruncate(): void
    {
        $this->pdo->exec('TRUNCATE TABLE pg_trunc_test');

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_trunc_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }

    /**
     * TRUNCATE without TABLE keyword.
     */
    public function testTruncateWithoutTableKeyword(): void
    {
        $this->pdo->exec('TRUNCATE pg_trunc_test');

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_trunc_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }

    /**
     * TRUNCATE TABLE ONLY.
     */
    public function testTruncateOnly(): void
    {
        $this->pdo->exec('TRUNCATE TABLE ONLY pg_trunc_test');

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_trunc_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }

    /**
     * TRUNCATE with RESTART IDENTITY.
     * The option is parsed by PgSqlParser but has no effect on shadow behavior.
     */
    public function testTruncateRestartIdentity(): void
    {
        $this->pdo->exec('TRUNCATE TABLE pg_trunc_test RESTART IDENTITY');

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_trunc_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }

    /**
     * TRUNCATE with CONTINUE IDENTITY.
     */
    public function testTruncateContinueIdentity(): void
    {
        $this->pdo->exec('TRUNCATE TABLE pg_trunc_test CONTINUE IDENTITY');

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_trunc_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }

    /**
     * TRUNCATE with CASCADE.
     */
    public function testTruncateCascade(): void
    {
        $this->pdo->exec('TRUNCATE TABLE pg_trunc_test CASCADE');

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_trunc_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }

    /**
     * TRUNCATE then INSERT — shadow should be empty then have new data.
     */
    public function testTruncateThenInsert(): void
    {
        $this->pdo->exec('TRUNCATE TABLE pg_trunc_test');
        $this->pdo->exec("INSERT INTO pg_trunc_test (id, name, score) VALUES (10, 'New', 100)");

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_trunc_test');
        $this->assertSame(1, (int) $stmt->fetchColumn());

        $stmt = $this->pdo->query('SELECT name FROM pg_trunc_test WHERE id = 10');
        $this->assertSame('New', $stmt->fetchColumn());
    }

    /**
     * Physical isolation: TRUNCATE stays in shadow.
     */
    public function testTruncatePhysicalIsolation(): void
    {
        $this->pdo->exec('TRUNCATE TABLE pg_trunc_test');

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_trunc_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
