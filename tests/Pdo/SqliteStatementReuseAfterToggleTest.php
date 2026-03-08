<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests prepared statement reuse after ZTD mode toggle.
 *
 * Queries are rewritten at prepare() time. Toggling ZTD mode after prepare()
 * does not change the prepared query's rewriting.
 * @spec SPEC-2.1
 */
class SqliteStatementReuseAfterToggleTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE srat_test (id INT PRIMARY KEY, name VARCHAR(50))';
    }

    protected function getTableNames(): array
    {
        return ['srat_test'];
    }



    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO srat_test VALUES (1, 'Alice')");
        $this->pdo->exec("INSERT INTO srat_test VALUES (2, 'Bob')");
    }
    /**
     * Prepared in ZTD mode, executed after disabling ZTD.
     * Query retains CTE rewriting from prepare time.
     */
    public function testPreparedInZtdExecutedAfterDisable(): void
    {
        $stmt = $this->pdo->prepare('SELECT COUNT(*) FROM srat_test');

        $this->pdo->disableZtd();

        $stmt->execute();
        // Still sees shadow data (2 rows), not physical (0 rows)
        $this->assertSame(2, (int) $stmt->fetchColumn());
    }

    /**
     * Prepared with ZTD disabled, executed after enabling ZTD.
     * Query retains non-rewritten form from prepare time.
     */
    public function testPreparedWithoutZtdExecutedAfterEnable(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->prepare('SELECT COUNT(*) FROM srat_test');

        $this->pdo->enableZtd();

        $stmt->execute();
        // Sees physical data (0 rows), not shadow
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }

    /**
     * Multiple toggles don't affect already-prepared statements.
     */
    public function testMultipleToggles(): void
    {
        $stmt = $this->pdo->prepare('SELECT name FROM srat_test WHERE id = ?');

        $this->pdo->disableZtd();
        $this->pdo->enableZtd();
        $this->pdo->disableZtd();

        $stmt->execute([1]);
        $this->assertSame('Alice', $stmt->fetchColumn());
    }
}
