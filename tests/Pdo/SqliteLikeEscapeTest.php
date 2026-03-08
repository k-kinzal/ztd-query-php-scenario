<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests LIKE pattern matching with special characters and ESCAPE clause on SQLite.
 * @spec SPEC-3.1
 */
class SqliteLikeEscapeTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE le_test (id INT PRIMARY KEY, val VARCHAR(100))';
    }

    protected function getTableNames(): array
    {
        return ['le_test'];
    }



    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO le_test VALUES (1, 'hello world')");
        $this->pdo->exec("INSERT INTO le_test VALUES (2, 'hello_world')");
        $this->pdo->exec("INSERT INTO le_test VALUES (3, '100% complete')");
        $this->pdo->exec("INSERT INTO le_test VALUES (4, '50% done')");
        $this->pdo->exec("INSERT INTO le_test VALUES (5, 'no match')");
    }
    /**
     * Basic LIKE with % wildcard.
     */
    public function testBasicLikeWildcard(): void
    {
        $stmt = $this->pdo->query("SELECT COUNT(*) FROM le_test WHERE val LIKE 'hello%'");
        $this->assertSame(2, (int) $stmt->fetchColumn());
    }

    /**
     * LIKE with _ wildcard.
     */
    public function testLikeUnderscoreWildcard(): void
    {
        $stmt = $this->pdo->query("SELECT COUNT(*) FROM le_test WHERE val LIKE 'hello_world'");
        // Both 'hello world' (space) and 'hello_world' match _ wildcard
        $this->assertSame(2, (int) $stmt->fetchColumn());
    }

    /**
     * LIKE with ESCAPE clause to match literal %.
     */
    public function testLikeEscapePercent(): void
    {
        $stmt = $this->pdo->query("SELECT COUNT(*) FROM le_test WHERE val LIKE '%!%%' ESCAPE '!'");
        // Matches values containing literal %
        $this->assertSame(2, (int) $stmt->fetchColumn());
    }

    /**
     * LIKE with ESCAPE clause to match literal _.
     */
    public function testLikeEscapeUnderscore(): void
    {
        $stmt = $this->pdo->query("SELECT COUNT(*) FROM le_test WHERE val LIKE '%!_%' ESCAPE '!'");
        // Matches 'hello_world' containing literal _
        $this->assertSame(1, (int) $stmt->fetchColumn());
    }

    /**
     * LIKE with prepared statement.
     */
    public function testLikePrepared(): void
    {
        $stmt = $this->pdo->prepare('SELECT COUNT(*) FROM le_test WHERE val LIKE ?');
        $stmt->execute(['%world']);
        $this->assertSame(2, (int) $stmt->fetchColumn());
    }
}
