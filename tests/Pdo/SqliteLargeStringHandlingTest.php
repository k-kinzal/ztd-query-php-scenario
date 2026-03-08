<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests handling of large string data through ZTD shadow store.
 * Real user pattern: CMS content, log storage, template engines, large JSON payloads.
 * @spec SPEC-10.2.49
 */
class SqliteLargeStringHandlingTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_ls_docs (id INTEGER PRIMARY KEY, title TEXT, body TEXT, metadata TEXT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_ls_docs'];
    }

    /**
     * Insert and retrieve a 10KB string.
     */
    public function testInsertAndSelect10KBString(): void
    {
        $body = str_repeat('A', 10240);
        $this->pdo->exec("INSERT INTO sl_ls_docs VALUES (1, 'doc1', '" . $body . "', NULL)");

        $rows = $this->ztdQuery("SELECT id, LENGTH(body) AS body_len FROM sl_ls_docs WHERE id = 1");
        $this->assertCount(1, $rows);
        $this->assertEquals(10240, (int) $rows[0]['body_len']);
    }

    /**
     * Insert and retrieve a 100KB string.
     */
    public function testInsertAndSelect100KBString(): void
    {
        $body = str_repeat('B', 102400);
        $this->pdo->exec("INSERT INTO sl_ls_docs VALUES (1, 'doc1', '" . $body . "', NULL)");

        $rows = $this->ztdQuery("SELECT LENGTH(body) AS body_len FROM sl_ls_docs WHERE id = 1");
        $this->assertCount(1, $rows);
        $this->assertEquals(102400, (int) $rows[0]['body_len']);
    }

    /**
     * Insert 500KB string via prepared statement.
     */
    public function testPreparedInsert500KBString(): void
    {
        $body = str_repeat('C', 512000);
        $stmt = $this->pdo->prepare("INSERT INTO sl_ls_docs VALUES (?, ?, ?, ?)");
        $stmt->execute([1, 'large-doc', $body, null]);

        $rows = $this->ztdQuery("SELECT LENGTH(body) AS body_len FROM sl_ls_docs WHERE id = 1");
        $this->assertCount(1, $rows);
        $this->assertEquals(512000, (int) $rows[0]['body_len']);
    }

    /**
     * LIKE search on large text field.
     */
    public function testLikeSearchOnLargeText(): void
    {
        $body1 = str_repeat('x', 5000) . 'NEEDLE' . str_repeat('y', 5000);
        $body2 = str_repeat('z', 10000);

        $this->pdo->exec("INSERT INTO sl_ls_docs VALUES (1, 'has-needle', '" . $body1 . "', NULL)");
        $this->pdo->exec("INSERT INTO sl_ls_docs VALUES (2, 'no-needle', '" . $body2 . "', NULL)");

        $rows = $this->ztdQuery("SELECT title FROM sl_ls_docs WHERE body LIKE '%NEEDLE%'");
        $this->assertCount(1, $rows);
        $this->assertSame('has-needle', $rows[0]['title']);
    }

    /**
     * Aggregate functions on large text — LENGTH, SUBSTR.
     */
    public function testAggregateFunctionsOnLargeText(): void
    {
        $this->pdo->exec("INSERT INTO sl_ls_docs VALUES (1, 'short', 'hello', NULL)");
        $this->pdo->exec("INSERT INTO sl_ls_docs VALUES (2, 'medium', '" . str_repeat('m', 1000) . "', NULL)");
        $this->pdo->exec("INSERT INTO sl_ls_docs VALUES (3, 'long', '" . str_repeat('l', 50000) . "', NULL)");

        $rows = $this->ztdQuery("
            SELECT
                MAX(LENGTH(body)) AS max_len,
                MIN(LENGTH(body)) AS min_len,
                SUM(LENGTH(body)) AS total_len,
                AVG(LENGTH(body)) AS avg_len
            FROM sl_ls_docs
        ");
        $this->assertEquals(50000, (int) $rows[0]['max_len']);
        $this->assertEquals(5, (int) $rows[0]['min_len']);
        $this->assertEquals(51005, (int) $rows[0]['total_len']);
    }

    /**
     * UPDATE large text field and verify.
     */
    public function testUpdateLargeTextField(): void
    {
        $this->pdo->exec("INSERT INTO sl_ls_docs VALUES (1, 'doc1', 'initial', NULL)");

        $newBody = str_repeat('U', 20000);
        $this->pdo->exec("UPDATE sl_ls_docs SET body = '" . $newBody . "' WHERE id = 1");

        $rows = $this->ztdQuery("SELECT LENGTH(body) AS body_len FROM sl_ls_docs WHERE id = 1");
        $this->assertEquals(20000, (int) $rows[0]['body_len']);
    }

    /**
     * Multiple large rows — verify no cross-contamination.
     */
    public function testMultipleLargeRowsIndependent(): void
    {
        for ($i = 1; $i <= 5; $i++) {
            $body = str_repeat((string) $i, $i * 10000);
            $this->pdo->exec("INSERT INTO sl_ls_docs VALUES ({$i}, 'doc{$i}', '{$body}', NULL)");
        }

        $rows = $this->ztdQuery("
            SELECT id, LENGTH(body) AS body_len, SUBSTR(body, 1, 1) AS first_char
            FROM sl_ls_docs
            ORDER BY id
        ");
        $this->assertCount(5, $rows);
        for ($i = 0; $i < 5; $i++) {
            $expected = ($i + 1) * 10000;
            $this->assertEquals($expected, (int) $rows[$i]['body_len']);
            $this->assertSame((string) ($i + 1), $rows[$i]['first_char']);
        }
    }

    /**
     * Physical isolation — large strings do not persist.
     */
    public function testPhysicalIsolationLargeStrings(): void
    {
        $body = str_repeat('P', 50000);
        $this->pdo->exec("INSERT INTO sl_ls_docs VALUES (1, 'persistent?', '{$body}', NULL)");

        // Visible in ZTD
        $rows = $this->ztdQuery("SELECT LENGTH(body) AS len FROM sl_ls_docs WHERE id = 1");
        $this->assertEquals(50000, (int) $rows[0]['len']);

        // Not visible after disable
        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM sl_ls_docs")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
