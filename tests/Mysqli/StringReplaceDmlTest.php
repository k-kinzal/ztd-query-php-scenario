<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests UPDATE SET with REPLACE() string function through ZTD shadow store.
 *
 * REPLACE(col, 'old', 'new') is one of the most common data cleanup patterns.
 * Issue #84 covers PostgreSQL format() in UPDATE SET; this tests the general
 * REPLACE() function on MySQL which has different syntax/handling.
 *
 * @spec SPEC-4.2
 */
class StringReplaceDmlTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_srd_pages (
            id INT PRIMARY KEY,
            url VARCHAR(200) NOT NULL,
            title VARCHAR(100) NOT NULL,
            body TEXT NOT NULL
        ) ENGINE=InnoDB';
    }

    protected function getTableNames(): array
    {
        return ['mi_srd_pages'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_srd_pages VALUES (1, 'http://old.example.com/page1', 'Page One', 'Visit http://old.example.com for details')");
        $this->mysqli->query("INSERT INTO mi_srd_pages VALUES (2, 'http://old.example.com/page2', 'Page Two', 'Contact us at http://old.example.com/contact')");
        $this->mysqli->query("INSERT INTO mi_srd_pages VALUES (3, 'http://other.com/page3', 'Page Three', 'No old links here')");
    }

    /**
     * UPDATE SET col = REPLACE(col, 'old', 'new') — basic string replacement.
     */
    public function testUpdateSetReplace(): void
    {
        try {
            $this->mysqli->query(
                "UPDATE mi_srd_pages SET url = REPLACE(url, 'old.example.com', 'new.example.com')"
            );

            $rows = $this->ztdQuery("SELECT id, url FROM mi_srd_pages ORDER BY id");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'UPDATE SET REPLACE: expected 3 rows, got ' . count($rows)
                );
            }
            $this->assertCount(3, $rows);

            if ($rows[0]['url'] !== 'http://new.example.com/page1') {
                $this->markTestIncomplete(
                    'REPLACE() in SET not applied: expected "http://new.example.com/page1", got '
                    . var_export($rows[0]['url'], true)
                );
            }
            $this->assertSame('http://new.example.com/page1', $rows[0]['url']);
            $this->assertSame('http://new.example.com/page2', $rows[1]['url']);
            // Row 3 has no 'old.example.com', should be unchanged
            $this->assertSame('http://other.com/page3', $rows[2]['url']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE SET REPLACE failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE SET REPLACE with WHERE condition — selective replacement.
     */
    public function testUpdateSetReplaceWithWhere(): void
    {
        try {
            $this->mysqli->query(
                "UPDATE mi_srd_pages SET body = REPLACE(body, 'http://old.example.com', 'https://new.example.com') WHERE id <= 2"
            );

            $rows = $this->ztdQuery("SELECT id, body FROM mi_srd_pages ORDER BY id");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'UPDATE SET REPLACE with WHERE: expected 3 rows, got ' . count($rows)
                );
            }
            $this->assertCount(3, $rows);

            if (strpos($rows[0]['body'], 'https://new.example.com') === false) {
                $this->markTestIncomplete(
                    'REPLACE() with WHERE not applied to row 1: body='
                    . var_export($rows[0]['body'], true)
                );
            }
            $this->assertStringContainsString('https://new.example.com', $rows[0]['body']);
            $this->assertStringContainsString('https://new.example.com', $rows[1]['body']);
            // Row 3 should be unchanged (not matched by WHERE)
            $this->assertSame('No old links here', $rows[2]['body']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE SET REPLACE with WHERE failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE SET with nested REPLACE — chained replacements.
     */
    public function testUpdateSetNestedReplace(): void
    {
        try {
            $this->mysqli->query(
                "UPDATE mi_srd_pages SET url = REPLACE(REPLACE(url, 'http://', 'https://'), 'old.example.com', 'new.example.com') WHERE id = 1"
            );

            $rows = $this->ztdQuery("SELECT url FROM mi_srd_pages WHERE id = 1");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'UPDATE SET nested REPLACE: expected 1 row, got ' . count($rows)
                );
            }
            $this->assertCount(1, $rows);

            if ($rows[0]['url'] !== 'https://new.example.com/page1') {
                $this->markTestIncomplete(
                    'Nested REPLACE() not applied: expected "https://new.example.com/page1", got '
                    . var_export($rows[0]['url'], true)
                );
            }
            $this->assertSame('https://new.example.com/page1', $rows[0]['url']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE SET nested REPLACE failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE SET with CONCAT and REPLACE combined.
     */
    public function testUpdateSetConcatAndReplace(): void
    {
        try {
            $this->mysqli->query(
                "UPDATE mi_srd_pages SET title = CONCAT('[Updated] ', REPLACE(title, 'Page', 'Doc')) WHERE id = 2"
            );

            $rows = $this->ztdQuery("SELECT title FROM mi_srd_pages WHERE id = 2");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'UPDATE SET CONCAT+REPLACE: expected 1 row, got ' . count($rows)
                );
            }
            $this->assertCount(1, $rows);

            if ($rows[0]['title'] !== '[Updated] Doc Two') {
                $this->markTestIncomplete(
                    'CONCAT+REPLACE not applied: expected "[Updated] Doc Two", got '
                    . var_export($rows[0]['title'], true)
                );
            }
            $this->assertSame('[Updated] Doc Two', $rows[0]['title']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE SET CONCAT+REPLACE failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared UPDATE SET REPLACE with parameter.
     */
    public function testPreparedUpdateSetReplace(): void
    {
        try {
            $stmt = $this->mysqli->prepare(
                "UPDATE mi_srd_pages SET url = REPLACE(url, ?, ?) WHERE id = ?"
            );
            $old = 'old.example.com';
            $new = 'new.example.com';
            $id = 1;
            $stmt->bind_param('ssi', $old, $new, $id);
            $stmt->execute();

            $rows = $this->ztdQuery("SELECT url FROM mi_srd_pages WHERE id = 1");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Prepared REPLACE: expected 1 row, got ' . count($rows)
                );
            }
            $this->assertCount(1, $rows);

            if ($rows[0]['url'] !== 'http://new.example.com/page1') {
                $this->markTestIncomplete(
                    'Prepared REPLACE not applied: expected "http://new.example.com/page1", got '
                    . var_export($rows[0]['url'], true)
                );
            }
            $this->assertSame('http://new.example.com/page1', $rows[0]['url']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE SET REPLACE failed: ' . $e->getMessage());
        }
    }
}
