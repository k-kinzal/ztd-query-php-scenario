<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests prepared statement CTE snapshot behavior on MySQL PDO.
 *
 * Cross-platform parity with SqlitePreparedSnapshotBehaviorTest.
 * @spec pending
 */
class MysqlPreparedSnapshotBehaviorTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pdo_mpsb_test (id INT PRIMARY KEY, name VARCHAR(50), score INT)';
    }

    protected function getTableNames(): array
    {
        return ['pdo_mpsb_test'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pdo_mpsb_test VALUES (1, 'Alice', 100)");
        $this->pdo->exec("INSERT INTO pdo_mpsb_test VALUES (2, 'Bob', 80)");
    }

    /**
     * INSERT after prepare() is NOT visible.
     */
    public function testInsertAfterPrepareNotVisible(): void
    {
        $stmt = $this->pdo->prepare('SELECT COUNT(*) FROM pdo_mpsb_test');

        $this->pdo->exec("INSERT INTO pdo_mpsb_test VALUES (3, 'Charlie', 90)");

        $stmt->execute();
        $this->assertSame(2, (int) $stmt->fetchColumn());
    }

    /**
     * Fresh prepare() after INSERT sees new data.
     */
    public function testFreshPrepareSeesNewData(): void
    {
        $this->pdo->exec("INSERT INTO pdo_mpsb_test VALUES (3, 'Charlie', 90)");

        $stmt = $this->pdo->prepare('SELECT COUNT(*) FROM pdo_mpsb_test');
        $stmt->execute();
        $this->assertSame(3, (int) $stmt->fetchColumn());
    }

    /**
     * Re-execution uses stale snapshot.
     */
    public function testReExecutionUsesStaleSnapshot(): void
    {
        $stmt = $this->pdo->prepare('SELECT COUNT(*) FROM pdo_mpsb_test');

        $stmt->execute();
        $this->assertSame(2, (int) $stmt->fetchColumn());

        $this->pdo->exec("INSERT INTO pdo_mpsb_test VALUES (3, 'Charlie', 90)");

        $stmt->execute();
        $this->assertSame(2, (int) $stmt->fetchColumn());
    }
}
