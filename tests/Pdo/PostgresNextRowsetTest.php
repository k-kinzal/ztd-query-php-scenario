<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests PDOStatement::nextRowset() behavior with ZTD on PostgreSQL.
 *
 * Discovery: PostgreSQL PDO driver does NOT support nextRowset() — same as SQLite.
 * Throws PDOException "Driver does not support this function".
 * Only MySQL supports nextRowset() (returns false for CTE queries).
 * @spec pending
 */
class PostgresNextRowsetTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE nr_test_pg (id INT PRIMARY KEY, name VARCHAR(50))';
    }

    protected function getTableNames(): array
    {
        return ['nr_test_pg'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO nr_test_pg VALUES (1, 'Alice')");
    }

    /**
     * PostgreSQL does not support multiple rowsets — nextRowset() throws.
     */
    public function testNextRowsetThrowsOnPostgres(): void
    {
        $stmt = $this->pdo->query('SELECT * FROM nr_test_pg WHERE id = 1');
        $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->expectException(\PDOException::class);
        $this->expectExceptionMessage('does not support');
        $stmt->nextRowset();
    }

    public function testNextRowsetThrowsOnPrepared(): void
    {
        $stmt = $this->pdo->prepare('SELECT name FROM nr_test_pg WHERE id = ?');
        $stmt->execute([1]);
        $stmt->fetch(PDO::FETCH_ASSOC);

        $this->expectException(\PDOException::class);
        $this->expectExceptionMessage('does not support');
        $stmt->nextRowset();
    }
}
