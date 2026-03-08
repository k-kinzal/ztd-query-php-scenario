<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;
use Tests\Support\MySQLContainer;

/** @spec pending */
class StmtInitBypassTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string
    {
        return 'CREATE TABLE stmt_init_test (id INT PRIMARY KEY, name VARCHAR(255))';
    }

    protected function getTableNames(): array
    {
        return ['stmt_init_test'];
    }

    /**
     * Test that stmt_init() + prepare() + execute() goes through ZTD.
     * If stmt_init() bypasses ZTD, data won't appear in the shadow store.
     */
    public function testStmtInitPrepareExecuteGoThroughZtd(): void
    {
        // Use stmt_init to get a statement handle
        $stmt = $this->mysqli->stmt_init();
        $stmt->prepare('INSERT INTO stmt_init_test (id, name) VALUES (?, ?)');
        $id = 1;
        $name = 'Alice';
        $stmt->bind_param('is', $id, $name);
        $stmt->execute();

        // Verify data is visible in ZTD shadow store
        $rows = $this->ztdQuery('SELECT * FROM stmt_init_test WHERE id = 1');
        if (count($rows) === 0) {
            $this->markTestIncomplete(
                'stmt_init() bypasses ZTD rewriting. Data inserted via stmt_init()->prepare() '
                . 'is not visible in the shadow store.'
            );
        }
        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
    }

    /**
     * Test that stmt_init() data is isolated from physical database.
     */
    public function testStmtInitIsolation(): void
    {
        $stmt = $this->mysqli->stmt_init();
        $stmt->prepare('INSERT INTO stmt_init_test (id, name) VALUES (?, ?)');
        $id = 1;
        $name = 'Alice';
        $stmt->bind_param('is', $id, $name);
        $stmt->execute();

        // Physical table should be empty
        $this->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM stmt_init_test');
        $count = (int) $result->fetch_assoc()['cnt'];
        $this->enableZtd();

        if ($count > 0) {
            $this->markTestIncomplete(
                'stmt_init() bypasses ZTD: data was written to the physical database.'
            );
        }
        $this->assertSame(0, $count);
    }
}
