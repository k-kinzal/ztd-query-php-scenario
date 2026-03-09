<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests MySQL REPLACE INTO with prepared statements through CTE shadow.
 *
 * REPLACE works like INSERT, but if a row with the same PK/unique key exists,
 * it DELETEs the old row and INSERTs a new one. This interacts with the shadow
 * store differently from ON DUPLICATE KEY UPDATE.
 *
 * Known Issue: REPLACE INTO via PDO prepared statements does not delete the
 * existing row in the shadow store. This creates duplicate primary keys and
 * the original values persist. The exec() path works correctly for REPLACE,
 * but prepared statements bypass the replace/upsert mutation path.
 *
 * Related: Issue #42 (MySQLi execute_query REPLACE), Issue #17 (PDO prepared upsert)
 *
 * @spec SPEC-4.4
 * @see SPEC-11.PDO-REPLACE-PREPARED
 */
class MysqlReplaceIntoPreparedTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pdo_replace_prep (id INT PRIMARY KEY, name VARCHAR(50), value INT)';
    }

    protected function getTableNames(): array
    {
        return ['pdo_replace_prep'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pdo_replace_prep VALUES (1, 'original', 100)");
        $this->pdo->exec("INSERT INTO pdo_replace_prep VALUES (2, 'keeper', 200)");
    }

    /**
     * REPLACE INTO with prepared statement - new row (works).
     */
    public function testReplacePreparedNewRow(): void
    {
        $stmt = $this->pdo->prepare('REPLACE INTO pdo_replace_prep (id, name, value) VALUES (?, ?, ?)');
        $stmt->execute([3, 'new', 300]);

        $rows = $this->ztdQuery('SELECT * FROM pdo_replace_prep ORDER BY id');
        $this->assertCount(3, $rows);
        $this->assertSame('new', $rows[2]['name']);
        $this->assertEquals(300, (int) $rows[2]['value']);
    }

    /**
     * REPLACE INTO with prepared statement - existing PK creates duplicate (Known Issue).
     *
     * Expected: 1 row with name='replaced', value=999
     * Actual: 2 rows with id=1 (original + new), shadow store has duplicate PK
     */
    public function testReplacePreparedExistingRowCreatesDuplicate(): void
    {
        $stmt = $this->pdo->prepare('REPLACE INTO pdo_replace_prep (id, name, value) VALUES (?, ?, ?)');
        $stmt->execute([1, 'replaced', 999]);

        $rows = $this->ztdQuery('SELECT name, value FROM pdo_replace_prep WHERE id = 1');
        // Known Issue: Returns 2 rows instead of 1 (duplicate PK in shadow store)
        $this->assertCount(2, $rows, 'Known Issue: REPLACE prepared creates duplicate PK');
    }

    /**
     * REPLACE prepared - row count increases instead of staying constant (Known Issue).
     *
     * Expected: 2 rows (replace existing)
     * Actual: 3 rows (original not deleted, new inserted)
     */
    public function testReplacePreparedRowCountIncreases(): void
    {
        $stmt = $this->pdo->prepare('REPLACE INTO pdo_replace_prep (id, name, value) VALUES (?, ?, ?)');
        $stmt->execute([1, 'replaced', 999]);

        $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM pdo_replace_prep');
        // Known Issue: 3 instead of expected 2
        $this->assertEquals(3, (int) $rows[0]['cnt'], 'Known Issue: REPLACE prepared adds instead of replacing');
    }

    /**
     * Multiple REPLACE prepared executions create many duplicates (Known Issue).
     */
    public function testReplacePreparedMultipleCreatesDuplicates(): void
    {
        $stmt = $this->pdo->prepare('REPLACE INTO pdo_replace_prep (id, name, value) VALUES (?, ?, ?)');
        $stmt->execute([1, 'v1', 10]);
        $stmt->execute([1, 'v2', 20]);
        $stmt->execute([1, 'v3', 30]);

        $rows = $this->ztdQuery('SELECT name, value FROM pdo_replace_prep WHERE id = 1');
        // Known Issue: Each REPLACE adds a row, so 4 rows with id=1
        $this->assertGreaterThan(1, count($rows), 'Known Issue: REPLACE prepared creates duplicates');
    }

    /**
     * REPLACE with exec() works correctly (for comparison).
     */
    public function testReplaceExecWorksCorrectly(): void
    {
        $this->pdo->exec("REPLACE INTO pdo_replace_prep VALUES (1, 'exec-replaced', 777)");

        $rows = $this->ztdQuery('SELECT name, value FROM pdo_replace_prep WHERE id = 1');
        $this->assertCount(1, $rows, 'REPLACE via exec() should work correctly');
        $this->assertSame('exec-replaced', $rows[0]['name']);
        $this->assertEquals(777, (int) $rows[0]['value']);
    }

    /**
     * REPLACE with named prepared parameters also fails (Known Issue).
     */
    public function testReplacePreparedNamedParamsFails(): void
    {
        $stmt = $this->pdo->prepare('REPLACE INTO pdo_replace_prep (id, name, value) VALUES (:id, :name, :value)');
        $stmt->execute([':id' => 1, ':name' => 'named', ':value' => 777]);

        $rows = $this->ztdQuery('SELECT name FROM pdo_replace_prep WHERE id = 1');
        // Known Issue: original value persists alongside new value
        $this->assertGreaterThan(1, count($rows), 'Known Issue: Named params REPLACE also creates duplicates');
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_replace_prep');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
