<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests ALTER TABLE behavior in ZTD mode on PostgreSQL via PDO.
 *
 * PostgreSQL does NOT support ALTER TABLE in ZTD mode — it throws
 * ZtdPdoException ("ALTER TABLE not yet supported for PostgreSQL").
 * @spec SPEC-5.1a
 */
class PostgresAlterTableTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pg_alter_test (id INT PRIMARY KEY, name VARCHAR(255))';
    }

    protected function getTableNames(): array
    {
        return ['pg_alter_test'];
    }


    public function testAlterTableAddColumnThrows(): void
    {
        $this->expectException(ZtdPdoException::class);
        $this->pdo->exec('ALTER TABLE pg_alter_test ADD COLUMN age INT');
    }

    public function testAlterTableDropColumnThrows(): void
    {
        $this->expectException(ZtdPdoException::class);
        $this->pdo->exec('ALTER TABLE pg_alter_test DROP COLUMN name');
    }

    public function testAlterTableRenameColumnThrows(): void
    {
        $this->expectException(ZtdPdoException::class);
        $this->pdo->exec('ALTER TABLE pg_alter_test RENAME COLUMN name TO full_name');
    }

    public function testOriginalTableStillWorksAfterFailedAlter(): void
    {
        // ALTER TABLE throws, but shadow store remains functional
        try {
            $this->pdo->exec('ALTER TABLE pg_alter_test ADD COLUMN age INT');
        } catch (ZtdPdoException $e) {
            // expected
        }

        $this->pdo->exec("INSERT INTO pg_alter_test (id, name) VALUES (1, 'Alice')");
        $stmt = $this->pdo->query('SELECT * FROM pg_alter_test WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
    }
}
