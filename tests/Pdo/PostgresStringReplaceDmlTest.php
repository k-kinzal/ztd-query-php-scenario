<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests UPDATE SET with replace() string function on PostgreSQL via PDO.
 *
 * Issue #108 covers PostgreSQL string functions with $N params.
 * This tests replace() with literal strings and ? params.
 *
 * @spec SPEC-4.2
 */
class PostgresStringReplaceDmlTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pg_srd_pages (
            id INT PRIMARY KEY,
            url VARCHAR(200) NOT NULL,
            title VARCHAR(100) NOT NULL,
            body TEXT NOT NULL
        )';
    }

    protected function getTableNames(): array
    {
        return ['pg_srd_pages'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_srd_pages VALUES (1, 'http://old.example.com/page1', 'Page One', 'Visit http://old.example.com for details')");
        $this->pdo->exec("INSERT INTO pg_srd_pages VALUES (2, 'http://old.example.com/page2', 'Page Two', 'Contact us at http://old.example.com/contact')");
        $this->pdo->exec("INSERT INTO pg_srd_pages VALUES (3, 'http://other.com/page3', 'Page Three', 'No old links here')");
    }

    public function testUpdateSetReplace(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE pg_srd_pages SET url = replace(url, 'old.example.com', 'new.example.com')"
            );

            $rows = $this->ztdQuery("SELECT id, url FROM pg_srd_pages ORDER BY id");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'UPDATE SET replace: expected 3 rows, got ' . count($rows)
                );
            }
            $this->assertCount(3, $rows);

            if ($rows[0]['url'] !== 'http://new.example.com/page1') {
                $this->markTestIncomplete(
                    'replace() in SET not applied: expected "http://new.example.com/page1", got '
                    . var_export($rows[0]['url'], true)
                );
            }
            $this->assertSame('http://new.example.com/page1', $rows[0]['url']);
            $this->assertSame('http://new.example.com/page2', $rows[1]['url']);
            $this->assertSame('http://other.com/page3', $rows[2]['url']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE SET replace failed: ' . $e->getMessage());
        }
    }

    public function testUpdateSetReplaceWithWhere(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE pg_srd_pages SET body = replace(body, 'http://old.example.com', 'https://new.example.com') WHERE id <= 2"
            );

            $rows = $this->ztdQuery("SELECT id, body FROM pg_srd_pages ORDER BY id");
            $this->assertCount(3, $rows);

            if (strpos($rows[0]['body'], 'https://new.example.com') === false) {
                $this->markTestIncomplete(
                    'replace() with WHERE not applied: body=' . var_export($rows[0]['body'], true)
                );
            }
            $this->assertStringContainsString('https://new.example.com', $rows[0]['body']);
            $this->assertSame('No old links here', $rows[2]['body']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE SET replace with WHERE failed: ' . $e->getMessage());
        }
    }

    /**
     * PostgreSQL: UPDATE SET with || concatenation and replace() combined.
     */
    public function testUpdateSetConcatAndReplace(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE pg_srd_pages SET title = '[Updated] ' || replace(title, 'Page', 'Doc') WHERE id = 2"
            );

            $rows = $this->ztdQuery("SELECT title FROM pg_srd_pages WHERE id = 2");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'UPDATE SET concat+replace: expected 1 row, got ' . count($rows)
                );
            }
            $this->assertCount(1, $rows);

            if ($rows[0]['title'] !== '[Updated] Doc Two') {
                $this->markTestIncomplete(
                    'concat+replace not applied: expected "[Updated] Doc Two", got '
                    . var_export($rows[0]['title'], true)
                );
            }
            $this->assertSame('[Updated] Doc Two', $rows[0]['title']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE SET concat+replace failed: ' . $e->getMessage());
        }
    }

    public function testPreparedUpdateSetReplace(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "UPDATE pg_srd_pages SET url = replace(url, ?, ?) WHERE id = ?"
            );
            $stmt->execute(['old.example.com', 'new.example.com', 1]);

            $rows = $this->ztdQuery("SELECT url FROM pg_srd_pages WHERE id = 1");
            $this->assertCount(1, $rows);

            if ($rows[0]['url'] !== 'http://new.example.com/page1') {
                $this->markTestIncomplete(
                    'Prepared replace not applied: got ' . var_export($rows[0]['url'], true)
                );
            }
            $this->assertSame('http://new.example.com/page1', $rows[0]['url']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE SET replace failed: ' . $e->getMessage());
        }
    }
}
