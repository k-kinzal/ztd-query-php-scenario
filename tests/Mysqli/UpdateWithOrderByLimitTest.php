<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests UPDATE with ORDER BY and LIMIT on MySQL ZTD.
 *
 * MySQL supports: UPDATE t SET ... WHERE ... ORDER BY ... LIMIT n
 * This allows updating only the first N rows matching a condition,
 * sorted by the specified order.
 * @spec pending
 */
class UpdateWithOrderByLimitTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_uol_test (id INT PRIMARY KEY, name VARCHAR(50), score INT, status VARCHAR(20))';
    }

    protected function getTableNames(): array
    {
        return ['mi_uol_test'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_uol_test (id, name, score, status) VALUES (1, 'Alice', 90, 'active')");
        $this->mysqli->query("INSERT INTO mi_uol_test (id, name, score, status) VALUES (2, 'Bob', 80, 'active')");
        $this->mysqli->query("INSERT INTO mi_uol_test (id, name, score, status) VALUES (3, 'Charlie', 70, 'active')");
        $this->mysqli->query("INSERT INTO mi_uol_test (id, name, score, status) VALUES (4, 'Dave', 60, 'active')");
        $this->mysqli->query("INSERT INTO mi_uol_test (id, name, score, status) VALUES (5, 'Eve', 50, 'active')");
    }

    /**
     * UPDATE with LIMIT only.
     */
    public function testUpdateWithLimit(): void
    {
        $this->mysqli->query("UPDATE mi_uol_test SET status = 'updated' LIMIT 2");

        $result = $this->mysqli->query("SELECT COUNT(*) AS cnt FROM mi_uol_test WHERE status = 'updated'");
        $this->assertEquals(2, (int) $result->fetch_assoc()['cnt']);
    }

    /**
     * UPDATE with ORDER BY and LIMIT updates only the first N matching rows.
     */
    public function testUpdateWithOrderByAndLimit(): void
    {
        $this->mysqli->query("UPDATE mi_uol_test SET status = 'low' ORDER BY score ASC LIMIT 2");

        // Two lowest-scoring should be marked 'low'
        $result = $this->mysqli->query("SELECT COUNT(*) AS cnt FROM mi_uol_test WHERE status = 'low'");
        $this->assertEquals(2, (int) $result->fetch_assoc()['cnt']);

        // Eve (50) and Dave (60) should be updated
        $result = $this->mysqli->query("SELECT status FROM mi_uol_test WHERE id = 5");
        $this->assertSame('low', $result->fetch_assoc()['status']);

        $result = $this->mysqli->query("SELECT status FROM mi_uol_test WHERE id = 4");
        $this->assertSame('low', $result->fetch_assoc()['status']);

        // Alice should remain active
        $result = $this->mysqli->query("SELECT status FROM mi_uol_test WHERE id = 1");
        $this->assertSame('active', $result->fetch_assoc()['status']);
    }

    /**
     * UPDATE with WHERE + ORDER BY + LIMIT.
     */
    public function testUpdateWithWhereOrderByLimit(): void
    {
        $this->mysqli->query("UPDATE mi_uol_test SET status = 'top' WHERE score > 60 ORDER BY score DESC LIMIT 2");

        // Top 2 of those with score > 60: Alice (90) and Bob (80)
        $result = $this->mysqli->query("SELECT COUNT(*) AS cnt FROM mi_uol_test WHERE status = 'top'");
        $this->assertEquals(2, (int) $result->fetch_assoc()['cnt']);

        $result = $this->mysqli->query("SELECT status FROM mi_uol_test WHERE id = 1");
        $this->assertSame('top', $result->fetch_assoc()['status']);

        $result = $this->mysqli->query("SELECT status FROM mi_uol_test WHERE id = 2");
        $this->assertSame('top', $result->fetch_assoc()['status']);
    }

    /**
     * Physical isolation for UPDATE with ORDER BY + LIMIT.
     */
    public function testUpdateWithOrderByLimitPhysicalIsolation(): void
    {
        $this->mysqli->query("UPDATE mi_uol_test SET status = 'changed' ORDER BY score ASC LIMIT 3");

        $this->mysqli->disableZtd();
        $result = $this->mysqli->query("SELECT COUNT(*) AS cnt FROM mi_uol_test WHERE status = 'changed'");
        $this->assertEquals(0, (int) $result->fetch_assoc()['cnt']);
    }
}
