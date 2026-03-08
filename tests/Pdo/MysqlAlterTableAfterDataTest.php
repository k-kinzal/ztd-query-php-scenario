<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests ALTER TABLE ADD COLUMN behavior with the shadow store on MySQL PDO.
 *
 * Discovery: On MySQL, ALTER TABLE modifies the physical table directly.
 * Unlike SQLite where new columns fail with "no such column" in CTE,
 * MySQL can successfully query the new column because the physical table has it.
 * However, the CTE rewriter schema is NOT updated.
 * @spec SPEC-5.1a
 */
class MysqlAlterTableAfterDataTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE evolve_m (id INT PRIMARY KEY, name VARCHAR(50))';
    }

    protected function getTableNames(): array
    {
        return ['evolve_m'];
    }


    /**
     * On MySQL, SELECT with new column works because physical table has it.
     * Unlike SQLite where CTE rewrite causes "no such column".
     */
    public function testSelectNewColumnWorksOnMysql(): void
    {
        $this->pdo->exec("INSERT INTO evolve_m VALUES (1, 'Alice')");
        $this->pdo->exec('ALTER TABLE evolve_m ADD COLUMN score INT');

        // On MySQL, the query falls through to physical table where the column exists
        $stmt = $this->pdo->query('SELECT name, score FROM evolve_m WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Alice', $row['name']);
    }

    public function testInsertWithNewColumnSucceeds(): void
    {
        $this->pdo->exec("INSERT INTO evolve_m VALUES (1, 'Alice')");
        $this->pdo->exec('ALTER TABLE evolve_m ADD COLUMN score INT');

        $this->pdo->exec("INSERT INTO evolve_m (id, name, score) VALUES (2, 'Bob', 100)");

        $stmt = $this->pdo->query('SELECT COUNT(*) AS cnt FROM evolve_m');
        $this->assertSame(2, (int) $stmt->fetch(PDO::FETCH_ASSOC)['cnt']);
    }

    public function testOriginalColumnsStillWorkAfterAlter(): void
    {
        $this->pdo->exec("INSERT INTO evolve_m VALUES (1, 'Alice')");
        $this->pdo->exec("INSERT INTO evolve_m VALUES (2, 'Bob')");
        $this->pdo->exec('ALTER TABLE evolve_m ADD COLUMN score INT');

        $stmt = $this->pdo->query('SELECT name FROM evolve_m WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Alice', $row['name']);
    }
}
