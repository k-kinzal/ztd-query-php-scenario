<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests prepared statement CTE snapshot behavior via MySQLi.
 *
 * Cross-platform parity with SqlitePreparedSnapshotBehaviorTest.
 * @spec pending
 */
class PreparedSnapshotBehaviorTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_psb_test (id INT PRIMARY KEY, name VARCHAR(50), score INT)';
    }

    protected function getTableNames(): array
    {
        return ['mi_psb_test'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_psb_test VALUES (1, 'Alice', 100)");
        $this->mysqli->query("INSERT INTO mi_psb_test VALUES (2, 'Bob', 80)");
    }

    /**
     * INSERT after prepare() is NOT visible.
     */
    public function testInsertAfterPrepareNotVisible(): void
    {
        $stmt = $this->mysqli->prepare('SELECT COUNT(*) AS cnt FROM mi_psb_test');

        $this->mysqli->query("INSERT INTO mi_psb_test VALUES (3, 'Charlie', 90)");

        $stmt->execute();
        $result = $stmt->get_result();
        $this->assertSame(2, (int) $result->fetch_assoc()['cnt']);
    }

    /**
     * Fresh prepare() after INSERT sees new data.
     */
    public function testFreshPrepareSeesNewData(): void
    {
        $this->mysqli->query("INSERT INTO mi_psb_test VALUES (3, 'Charlie', 90)");

        $stmt = $this->mysqli->prepare('SELECT COUNT(*) AS cnt FROM mi_psb_test');
        $stmt->execute();
        $result = $stmt->get_result();
        $this->assertSame(3, (int) $result->fetch_assoc()['cnt']);
    }

    /**
     * Re-execution uses stale snapshot.
     */
    public function testReExecutionUsesStaleSnapshot(): void
    {
        $stmt = $this->mysqli->prepare('SELECT COUNT(*) AS cnt FROM mi_psb_test');

        $stmt->execute();
        $result = $stmt->get_result();
        $this->assertSame(2, (int) $result->fetch_assoc()['cnt']);

        $this->mysqli->query("INSERT INTO mi_psb_test VALUES (3, 'Charlie', 90)");

        $stmt->execute();
        $result = $stmt->get_result();
        $this->assertSame(2, (int) $result->fetch_assoc()['cnt']);
    }
}
