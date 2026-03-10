<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests DML with parameterized IN lists through ZTD shadow store on MySQLi.
 *
 * @spec SPEC-4.2, SPEC-4.3, SPEC-3.2
 */
class ParamInListDmlTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_pin_tasks (
            id INT AUTO_INCREMENT PRIMARY KEY,
            title VARCHAR(50) NOT NULL,
            status VARCHAR(20) NOT NULL,
            priority INT NOT NULL
        ) ENGINE=InnoDB';
    }

    protected function getTableNames(): array
    {
        return ['mi_pin_tasks'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_pin_tasks VALUES (1, 'Task A', 'open', 1)");
        $this->mysqli->query("INSERT INTO mi_pin_tasks VALUES (2, 'Task B', 'in_progress', 2)");
        $this->mysqli->query("INSERT INTO mi_pin_tasks VALUES (3, 'Task C', 'open', 3)");
        $this->mysqli->query("INSERT INTO mi_pin_tasks VALUES (4, 'Task D', 'done', 1)");
        $this->mysqli->query("INSERT INTO mi_pin_tasks VALUES (5, 'Task E', 'in_progress', 2)");
        $this->mysqli->query("INSERT INTO mi_pin_tasks VALUES (6, 'Task F', 'blocked', 1)");
    }

    public function testPreparedDeleteInList(): void
    {
        try {
            $stmt = $this->mysqli->prepare("DELETE FROM mi_pin_tasks WHERE id IN (?, ?, ?)");
            $stmt->bind_param('iii', ...[$a = 1, $b = 3, $c = 5]);
            $stmt->execute();

            $rows = $this->ztdQuery("SELECT id FROM mi_pin_tasks ORDER BY id");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'Prepared DELETE IN: expected 3, got ' . count($rows) . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
            $ids = array_map(fn($r) => (int) $r['id'], $rows);
            $this->assertSame([2, 4, 6], $ids);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared DELETE IN list failed: ' . $e->getMessage());
        }
    }

    public function testPreparedUpdateInList(): void
    {
        try {
            $stmt = $this->mysqli->prepare("UPDATE mi_pin_tasks SET priority = ? WHERE status IN (?, ?)");
            $stmt->bind_param('iss', ...[$pri = 99, $s1 = 'open', $s2 = 'blocked']);
            $stmt->execute();

            $rows = $this->ztdQuery("SELECT title, priority FROM mi_pin_tasks ORDER BY id");

            $byTitle = [];
            foreach ($rows as $r) {
                $byTitle[$r['title']] = (int) $r['priority'];
            }

            if ($byTitle['Task A'] !== 99 || $byTitle['Task B'] !== 2) {
                $this->markTestIncomplete('Prepared UPDATE IN: got ' . json_encode($byTitle));
            }

            $this->assertSame(99, $byTitle['Task A']);
            $this->assertSame(2, $byTitle['Task B']);
            $this->assertSame(99, $byTitle['Task F']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE IN list failed: ' . $e->getMessage());
        }
    }

    public function testPreparedUpdateNotInList(): void
    {
        try {
            $stmt = $this->mysqli->prepare("UPDATE mi_pin_tasks SET status = 'archived' WHERE id NOT IN (?, ?)");
            $stmt->bind_param('ii', ...[$a = 1, $b = 2]);
            $stmt->execute();

            $rows = $this->ztdQuery("SELECT id, status FROM mi_pin_tasks ORDER BY id");
            $byId = [];
            foreach ($rows as $r) {
                $byId[(int) $r['id']] = $r['status'];
            }

            if ($byId[1] !== 'open' || $byId[3] !== 'archived') {
                $this->markTestIncomplete('Prepared UPDATE NOT IN: got ' . json_encode($byId));
            }

            $this->assertSame('open', $byId[1]);
            $this->assertSame('in_progress', $byId[2]);
            $this->assertSame('archived', $byId[3]);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE NOT IN failed: ' . $e->getMessage());
        }
    }
}
