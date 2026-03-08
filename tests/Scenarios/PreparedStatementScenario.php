<?php

declare(strict_types=1);

namespace Tests\Scenarios;

/**
 * Shared prepared statement scenario for all platforms.
 *
 * @spec SPEC-3.2, SPEC-4.1, SPEC-4.2, SPEC-4.3, SPEC-4.4
 *
 * Requires table: prep_test (id INT/INTEGER PRIMARY KEY, name VARCHAR/TEXT, score INT/INTEGER)
 * Provided by the concrete test class via getTableDDL().
 */
trait PreparedStatementScenario
{
    abstract protected function ztdExec(string $sql): int|false;
    abstract protected function ztdQuery(string $sql): array;
    abstract protected function ztdPrepareAndExecute(string $sql, array $params): array;

    /** @spec SPEC-3.2, SPEC-4.1 */
    public function testPreparedInsertAndSelect(): void
    {
        $this->ztdPrepareAndExecute(
            'INSERT INTO prep_test (id, name, score) VALUES (?, ?, ?)',
            [1, 'Alice', 100]
        );

        $rows = $this->ztdQuery('SELECT * FROM prep_test WHERE id = 1');

        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame(100, (int) $rows[0]['score']);
    }

    /** @spec SPEC-3.2 */
    public function testPreparedSelectWithParameter(): void
    {
        $this->ztdExec("INSERT INTO prep_test (id, name, score) VALUES (1, 'Alice', 100)");
        $this->ztdExec("INSERT INTO prep_test (id, name, score) VALUES (2, 'Bob', 85)");

        $rows = $this->ztdPrepareAndExecute(
            'SELECT name, score FROM prep_test WHERE score > ? ORDER BY name',
            [80]
        );

        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Bob', $rows[1]['name']);
    }

    /** @spec SPEC-4.2, SPEC-4.4 */
    public function testPreparedUpdateRowCount(): void
    {
        $this->ztdExec("INSERT INTO prep_test (id, name, score) VALUES (1, 'Alice', 100)");
        $this->ztdExec("INSERT INTO prep_test (id, name, score) VALUES (2, 'Bob', 85)");
        $this->ztdExec("INSERT INTO prep_test (id, name, score) VALUES (3, 'Charlie', 70)");

        // Update rows with score < 90 (Bob and Charlie)
        $this->ztdPrepareAndExecute('UPDATE prep_test SET score = ? WHERE score < ?', [0, 90]);

        $rows = $this->ztdQuery('SELECT * FROM prep_test WHERE score = 0 ORDER BY id');
        $this->assertCount(2, $rows);
    }

    /** @spec SPEC-4.3 */
    public function testPreparedDeleteAndVerify(): void
    {
        $this->ztdExec("INSERT INTO prep_test (id, name, score) VALUES (1, 'Alice', 100)");
        $this->ztdExec("INSERT INTO prep_test (id, name, score) VALUES (2, 'Bob', 85)");

        $this->ztdPrepareAndExecute('DELETE FROM prep_test WHERE id = ?', [1]);

        $rows = $this->ztdQuery('SELECT * FROM prep_test');
        $this->assertCount(1, $rows);
        $this->assertSame('Bob', $rows[0]['name']);
    }

    /** @spec SPEC-3.2, SPEC-4.1 */
    public function testReExecutePreparedStatement(): void
    {
        $this->ztdPrepareAndExecute(
            'INSERT INTO prep_test (id, name, score) VALUES (?, ?, ?)',
            [1, 'Alice', 100]
        );
        $this->ztdPrepareAndExecute(
            'INSERT INTO prep_test (id, name, score) VALUES (?, ?, ?)',
            [2, 'Bob', 85]
        );

        $rows = $this->ztdQuery('SELECT * FROM prep_test ORDER BY id');

        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Bob', $rows[1]['name']);
    }
}
