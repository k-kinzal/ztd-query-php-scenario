<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests SQLite GLOB operator through CTE shadow.
 *
 * GLOB is a SQLite-specific case-sensitive pattern matching operator
 * using * and ? wildcards (unlike LIKE which uses % and _).
 * @spec pending
 */
class SqliteGlobOperatorTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE glob_files (id INT PRIMARY KEY, path VARCHAR(200), type VARCHAR(20))';
    }

    protected function getTableNames(): array
    {
        return ['glob_files'];
    }


    /**
     * GLOB with * wildcard.
     */
    public function testGlobStarWildcard(): void
    {
        $stmt = $this->pdo->query("SELECT COUNT(*) FROM glob_files WHERE path GLOB '/home/*'");
        $this->assertSame(3, (int) $stmt->fetchColumn());
    }

    /**
     * GLOB with ? single-character wildcard.
     */
    public function testGlobQuestionMarkWildcard(): void
    {
        $stmt = $this->pdo->query("SELECT COUNT(*) FROM glob_files WHERE path GLOB '/home/user/docs/?????.txt'");
        $this->assertSame(1, (int) $stmt->fetchColumn());
    }

    /**
     * GLOB is case-sensitive (unlike LIKE).
     */
    public function testGlobIsCaseSensitive(): void
    {
        // 'Photos' with capital P
        $stmt = $this->pdo->query("SELECT COUNT(*) FROM glob_files WHERE path GLOB '*Photos*'");
        $this->assertSame(1, (int) $stmt->fetchColumn());

        // 'photos' with lowercase p — should not match
        $stmt = $this->pdo->query("SELECT COUNT(*) FROM glob_files WHERE path GLOB '*photos*'");
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }

    /**
     * GLOB with file extension pattern.
     */
    public function testGlobFileExtension(): void
    {
        $stmt = $this->pdo->query("SELECT path FROM glob_files WHERE path GLOB '*.md'");
        $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertCount(1, $rows);
        $this->assertStringEndsWith('README.md', $rows[0]);
    }

    /**
     * GLOB after shadow mutation.
     */
    public function testGlobAfterMutation(): void
    {
        $this->pdo->exec("INSERT INTO glob_files VALUES (6, '/home/user/docs/todo.md', 'doc')");

        $stmt = $this->pdo->query("SELECT COUNT(*) FROM glob_files WHERE path GLOB '*.md'");
        $this->assertSame(2, (int) $stmt->fetchColumn());
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM glob_files');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
