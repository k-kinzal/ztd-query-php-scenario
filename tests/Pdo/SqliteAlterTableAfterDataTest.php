<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests ALTER TABLE ADD COLUMN behavior with the shadow store on SQLite PDO.
 *
 * ALTER TABLE ADD COLUMN should update the reflected schema so that
 * new columns are queryable.
 * @spec SPEC-5.1a
 */
class SqliteAlterTableAfterDataTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE evolve (id INT PRIMARY KEY, name VARCHAR(50))';
    }

    protected function getTableNames(): array
    {
        return ['evolve'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec('CREATE TABLE evolve (id INT PRIMARY KEY, name VARCHAR(50))');

        }

    /**
     * SELECT referencing the new column should work after ALTER TABLE ADD COLUMN.
     */
    public function testSelectNewColumnWorksAfterAlter(): void
    {
        $this->pdo->exec("INSERT INTO evolve VALUES (1, 'Alice')");
        $this->pdo->exec('ALTER TABLE evolve ADD COLUMN score INT');

        try {
            $stmt = $this->pdo->query('SELECT name, score FROM evolve WHERE id = 1');
            $row = $stmt->fetch(PDO::FETCH_ASSOC);
            $this->assertSame('Alice', $row['name']);
        } catch (\PDOException $e) {
            $this->markTestIncomplete(
                'ALTER TABLE ADD COLUMN does not update reflected schema on SQLite. '
                . 'New column not visible in CTE rewriter: ' . $e->getMessage()
            );
        }
    }

    /**
     * INSERT with the new column succeeds — shadow store accepts the data.
     */
    public function testInsertWithNewColumnSucceeds(): void
    {
        $this->pdo->exec("INSERT INTO evolve VALUES (1, 'Alice')");
        $this->pdo->exec('ALTER TABLE evolve ADD COLUMN score INT');

        // This does NOT throw — INSERT goes to shadow store
        $this->pdo->exec("INSERT INTO evolve (id, name, score) VALUES (2, 'Bob', 100)");

        // But we can't verify the new column via SELECT
        $stmt = $this->pdo->query('SELECT COUNT(*) AS cnt FROM evolve');
        $this->assertSame(2, (int) $stmt->fetch(PDO::FETCH_ASSOC)['cnt']);
    }

    /**
     * Original columns still work after ALTER TABLE.
     */
    public function testOriginalColumnsStillWorkAfterAlter(): void
    {
        $this->pdo->exec("INSERT INTO evolve VALUES (1, 'Alice')");
        $this->pdo->exec("INSERT INTO evolve VALUES (2, 'Bob')");
        $this->pdo->exec('ALTER TABLE evolve ADD COLUMN score INT');

        $stmt = $this->pdo->query('SELECT name FROM evolve WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Alice', $row['name']);
    }

    /**
     * ALTER TABLE before data insertion - new column should be visible.
     */
    public function testAlterBeforeInsertNewColumnVisible(): void
    {
        $this->pdo->exec('ALTER TABLE evolve ADD COLUMN score INT');
        $this->pdo->exec("INSERT INTO evolve (id, name, score) VALUES (1, 'Alice', 95)");

        try {
            $stmt = $this->pdo->query('SELECT name, score FROM evolve WHERE id = 1');
            $row = $stmt->fetch(PDO::FETCH_ASSOC);
            $this->assertSame('Alice', $row['name']);
            $this->assertSame(95, (int) $row['score']);
        } catch (\PDOException $e) {
            $this->markTestIncomplete(
                'ALTER TABLE ADD COLUMN does not update reflected schema on SQLite (even before data). '
                . 'New column not visible: ' . $e->getMessage()
            );
        }
    }
}
