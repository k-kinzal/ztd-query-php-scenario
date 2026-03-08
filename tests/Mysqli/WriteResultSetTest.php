<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/** @spec SPEC-4.5 */
class WriteResultSetTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE write_result_test (id INT PRIMARY KEY, val VARCHAR(255))';
    }

    protected function getTableNames(): array
    {
        return ['write_result_test'];
    }


    public function testInsertResultHasExhaustedCursor(): void
    {
        $result = $this->mysqli->query("INSERT INTO write_result_test (id, val) VALUES (1, 'a')");

        // Write operations return a result with exhausted cursor
        $this->assertInstanceOf(\mysqli_result::class, $result);
        $this->assertNull($result->fetch_assoc());
    }

    public function testUpdateResultHasExhaustedCursor(): void
    {
        $this->mysqli->query("INSERT INTO write_result_test (id, val) VALUES (1, 'a')");
        $result = $this->mysqli->query("UPDATE write_result_test SET val = 'b' WHERE id = 1");

        $this->assertInstanceOf(\mysqli_result::class, $result);
        $this->assertNull($result->fetch_assoc());
    }

    public function testDeleteResultHasExhaustedCursor(): void
    {
        $this->mysqli->query("INSERT INTO write_result_test (id, val) VALUES (1, 'a')");
        $result = $this->mysqli->query("DELETE FROM write_result_test WHERE id = 1");

        $this->assertInstanceOf(\mysqli_result::class, $result);
        $this->assertNull($result->fetch_assoc());
    }
}
