<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests the bind_result() + fetch() pattern which is the classic
 * alternative to get_result() for reading prepared statement results.
 * @spec pending
 */
class BindResultFetchTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE bind_result_test (id INT PRIMARY KEY, name VARCHAR(255), score INT)';
    }

    protected function getTableNames(): array
    {
        return ['bind_result_test'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO bind_result_test (id, name, score) VALUES (1, 'Alice', 100)");
        $this->mysqli->query("INSERT INTO bind_result_test (id, name, score) VALUES (2, 'Bob', 85)");
        $this->mysqli->query("INSERT INTO bind_result_test (id, name, score) VALUES (3, 'Charlie', 70)");
    }

    public function testBindResultAndFetch(): void
    {
        $stmt = $this->mysqli->prepare('SELECT id, name, score FROM bind_result_test WHERE id = ?');
        $id = 1;
        $stmt->bind_param('i', $id);
        $stmt->execute();

        $resultId = null;
        $resultName = null;
        $resultScore = null;
        $stmt->bind_result($resultId, $resultName, $resultScore);

        $fetched = $stmt->fetch();
        $this->assertTrue($fetched);
        $this->assertSame(1, $resultId);
        $this->assertSame('Alice', $resultName);
        $this->assertSame(100, $resultScore);

        // No more rows
        $noMore = $stmt->fetch();
        $this->assertNull($noMore);
    }

    public function testBindResultMultipleRows(): void
    {
        $stmt = $this->mysqli->prepare('SELECT id, name FROM bind_result_test ORDER BY id');
        $stmt->execute();

        $id = null;
        $name = null;
        $stmt->bind_result($id, $name);

        $rows = [];
        while ($stmt->fetch()) {
            $rows[] = ['id' => $id, 'name' => $name];
        }

        $this->assertCount(3, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Bob', $rows[1]['name']);
        $this->assertSame('Charlie', $rows[2]['name']);
    }

    public function testBindResultAfterUpdate(): void
    {
        $this->mysqli->query("UPDATE bind_result_test SET score = 999 WHERE id = 2");

        $stmt = $this->mysqli->prepare('SELECT score FROM bind_result_test WHERE id = ?');
        $id = 2;
        $stmt->bind_param('i', $id);
        $stmt->execute();

        $score = null;
        $stmt->bind_result($score);
        $stmt->fetch();

        $this->assertSame(999, $score);
    }

    public function testBindResultAfterDelete(): void
    {
        $this->mysqli->query("DELETE FROM bind_result_test WHERE id = 3");

        $stmt = $this->mysqli->prepare('SELECT id, name FROM bind_result_test ORDER BY id');
        $stmt->execute();

        $id = null;
        $name = null;
        $stmt->bind_result($id, $name);

        $rows = [];
        while ($stmt->fetch()) {
            $rows[] = ['id' => $id, 'name' => $name];
        }

        $this->assertCount(2, $rows);
    }
}
