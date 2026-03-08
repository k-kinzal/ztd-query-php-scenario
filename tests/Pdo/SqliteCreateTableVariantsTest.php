<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests CREATE TABLE LIKE and CREATE TABLE AS SELECT on SQLite.
 *
 * CREATE TABLE LIKE works correctly on SQLite.
 * CREATE TABLE AS SELECT (CTAS) has a limitation: the exec succeeds and
 * INSERT into the created table works, but SELECT from it fails because
 * the CTE rewriter cannot find the physical table for the shadow-only table.
 * @spec SPEC-5.1b
 */
class SqliteCreateTableVariantsTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE source_table (id INTEGER PRIMARY KEY, val TEXT)',
            'CREATE TABLE target_like LIKE source_table',
            'CREATE TABLE target_ctas AS SELECT * FROM source_table',
        ];
    }

    protected function getTableNames(): array
    {
        return ['LIKE', 'AS', 'source_table', 'target_like', 'target_ctas'];
    }


    public function testCreateTableLike(): void
    {
        $this->pdo->exec('CREATE TABLE target_like LIKE source_table');

        $this->pdo->exec("INSERT INTO target_like (id, val) VALUES (1, 'hello')");

        $stmt = $this->pdo->query('SELECT * FROM target_like WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('hello', $rows[0]['val']);
    }

    public function testCreateTableAsSelectExecSucceeds(): void
    {
        $this->pdo->exec("INSERT INTO source_table (id, val) VALUES (1, 'hello')");

        // CTAS exec succeeds without error
        $result = $this->pdo->exec('CREATE TABLE target_ctas AS SELECT * FROM source_table');
        $this->assertIsInt($result);
    }

    public function testCreateTableAsSelectQueryFails(): void
    {
        // CTAS creates shadow data, but SELECT from the created table fails
        // because the physical table doesn't exist and the CTE rewriter
        // can't build the CTE for a non-physical table.
        $this->pdo->exec("INSERT INTO source_table (id, val) VALUES (1, 'hello')");
        $this->pdo->exec('CREATE TABLE target_ctas AS SELECT * FROM source_table');

        $this->expectException(\PDOException::class);
        $this->expectExceptionMessageMatches('/no such table/');
        $this->pdo->query('SELECT * FROM target_ctas');
    }

    public function testCreateTableAsSelectInsertMakesQueryable(): void
    {
        // INSERT into a CTAS-created table populates the shadow store
        $this->pdo->exec("INSERT INTO source_table (id, val) VALUES (1, 'hello')");
        $this->pdo->exec('CREATE TABLE target_ctas AS SELECT * FROM source_table');

        // INSERT succeeds
        $this->pdo->exec("INSERT INTO target_ctas (id, val) VALUES (2, 'world')");

        // After INSERT, SELECT works — but only returns the INSERTed row,
        // not the original CTAS data (which is lost).
        $stmt = $this->pdo->query('SELECT * FROM target_ctas');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('world', $rows[0]['val']);
    }

    public function testCreateTableLikeIsolation(): void
    {
        $this->pdo->exec('CREATE TABLE target_like LIKE source_table');
        $this->pdo->exec("INSERT INTO target_like (id, val) VALUES (1, 'hello')");

        $this->pdo->disableZtd();
        // Physical table should not exist
        try {
            $stmt = $this->pdo->query('SELECT * FROM target_like');
            $this->assertCount(0, $stmt->fetchAll(PDO::FETCH_ASSOC));
        } catch (\PDOException $e) {
            $this->assertStringContainsString('target_like', $e->getMessage());
        }
    }
}
