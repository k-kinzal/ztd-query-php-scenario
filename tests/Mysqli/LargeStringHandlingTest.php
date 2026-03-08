<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests handling of large string data through ZTD shadow store.
 * Real user pattern: CMS content, log storage, large text fields.
 * @spec SPEC-10.2.49
 */
class LargeStringHandlingTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_ls_docs (id INT PRIMARY KEY, title VARCHAR(255), body LONGTEXT, metadata TEXT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_ls_docs'];
    }

    public function testInsertAndSelect10KBString(): void
    {
        $body = str_repeat('A', 10240);
        $this->mysqli->query("INSERT INTO mi_ls_docs VALUES (1, 'doc1', '" . $body . "', NULL)");

        $rows = $this->ztdQuery("SELECT id, LENGTH(body) AS body_len FROM mi_ls_docs WHERE id = 1");
        $this->assertCount(1, $rows);
        $this->assertEquals(10240, (int) $rows[0]['body_len']);
    }

    public function testInsertAndSelect100KBString(): void
    {
        $body = str_repeat('B', 102400);
        $this->mysqli->query("INSERT INTO mi_ls_docs VALUES (1, 'doc1', '" . $body . "', NULL)");

        $rows = $this->ztdQuery("SELECT LENGTH(body) AS body_len FROM mi_ls_docs WHERE id = 1");
        $this->assertEquals(102400, (int) $rows[0]['body_len']);
    }

    public function testPreparedInsert500KBString(): void
    {
        $body = str_repeat('C', 512000);
        $stmt = $this->mysqli->prepare("INSERT INTO mi_ls_docs VALUES (?, ?, ?, ?)");
        $id = 1;
        $title = 'large-doc';
        $stmt->bind_param('isss', $id, $title, $body, $title);
        $stmt->execute();

        $rows = $this->ztdQuery("SELECT LENGTH(body) AS body_len FROM mi_ls_docs WHERE id = 1");
        $this->assertEquals(512000, (int) $rows[0]['body_len']);
    }

    public function testLikeSearchOnLargeText(): void
    {
        $body1 = str_repeat('x', 5000) . 'NEEDLE' . str_repeat('y', 5000);
        $body2 = str_repeat('z', 10000);

        $this->mysqli->query("INSERT INTO mi_ls_docs VALUES (1, 'has-needle', '" . $body1 . "', NULL)");
        $this->mysqli->query("INSERT INTO mi_ls_docs VALUES (2, 'no-needle', '" . $body2 . "', NULL)");

        $rows = $this->ztdQuery("SELECT title FROM mi_ls_docs WHERE body LIKE '%NEEDLE%'");
        $this->assertCount(1, $rows);
        $this->assertSame('has-needle', $rows[0]['title']);
    }

    public function testAggregateFunctionsOnLargeText(): void
    {
        $this->mysqli->query("INSERT INTO mi_ls_docs VALUES (1, 'short', 'hello', NULL)");
        $this->mysqli->query("INSERT INTO mi_ls_docs VALUES (2, 'medium', '" . str_repeat('m', 1000) . "', NULL)");
        $this->mysqli->query("INSERT INTO mi_ls_docs VALUES (3, 'long', '" . str_repeat('l', 50000) . "', NULL)");

        $rows = $this->ztdQuery("
            SELECT
                MAX(LENGTH(body)) AS max_len,
                MIN(LENGTH(body)) AS min_len,
                SUM(LENGTH(body)) AS total_len
            FROM mi_ls_docs
        ");
        $this->assertEquals(50000, (int) $rows[0]['max_len']);
        $this->assertEquals(5, (int) $rows[0]['min_len']);
        $this->assertEquals(51005, (int) $rows[0]['total_len']);
    }

    public function testUpdateLargeTextField(): void
    {
        $this->mysqli->query("INSERT INTO mi_ls_docs VALUES (1, 'doc1', 'initial', NULL)");

        $newBody = str_repeat('U', 20000);
        $this->mysqli->query("UPDATE mi_ls_docs SET body = '" . $newBody . "' WHERE id = 1");

        $rows = $this->ztdQuery("SELECT LENGTH(body) AS body_len FROM mi_ls_docs WHERE id = 1");
        $this->assertEquals(20000, (int) $rows[0]['body_len']);
    }

    public function testMultipleLargeRowsIndependent(): void
    {
        for ($i = 1; $i <= 5; $i++) {
            $body = str_repeat((string) $i, $i * 10000);
            $this->mysqli->query("INSERT INTO mi_ls_docs VALUES ({$i}, 'doc{$i}', '{$body}', NULL)");
        }

        $rows = $this->ztdQuery("
            SELECT id, LENGTH(body) AS body_len, SUBSTRING(body, 1, 1) AS first_char
            FROM mi_ls_docs
            ORDER BY id
        ");
        $this->assertCount(5, $rows);
        for ($i = 0; $i < 5; $i++) {
            $expected = ($i + 1) * 10000;
            $this->assertEquals($expected, (int) $rows[$i]['body_len']);
            $this->assertSame((string) ($i + 1), $rows[$i]['first_char']);
        }
    }

    public function testPhysicalIsolation(): void
    {
        $body = str_repeat('P', 50000);
        $this->mysqli->query("INSERT INTO mi_ls_docs VALUES (1, 'persistent?', '{$body}', NULL)");

        $rows = $this->ztdQuery("SELECT LENGTH(body) AS len FROM mi_ls_docs WHERE id = 1");
        $this->assertEquals(50000, (int) $rows[0]['len']);

        $this->mysqli->disableZtd();
        $result = $this->mysqli->query("SELECT COUNT(*) AS cnt FROM mi_ls_docs");
        $row = $result->fetch_assoc();
        $this->assertEquals(0, (int) $row['cnt']);
    }
}
