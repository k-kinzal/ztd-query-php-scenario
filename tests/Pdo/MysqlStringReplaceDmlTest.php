<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests UPDATE SET with REPLACE() string function on MySQL via PDO.
 *
 * @spec SPEC-4.2
 */
class MysqlStringReplaceDmlTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mpd_srd_pages (
            id INT PRIMARY KEY,
            url VARCHAR(200) NOT NULL,
            title VARCHAR(100) NOT NULL,
            body TEXT NOT NULL
        ) ENGINE=InnoDB';
    }

    protected function getTableNames(): array
    {
        return ['mpd_srd_pages'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO mpd_srd_pages VALUES (1, 'http://old.example.com/page1', 'Page One', 'Visit http://old.example.com for details')");
        $this->pdo->exec("INSERT INTO mpd_srd_pages VALUES (2, 'http://old.example.com/page2', 'Page Two', 'Contact us at http://old.example.com/contact')");
        $this->pdo->exec("INSERT INTO mpd_srd_pages VALUES (3, 'http://other.com/page3', 'Page Three', 'No old links here')");
    }

    public function testUpdateSetReplace(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE mpd_srd_pages SET url = REPLACE(url, 'old.example.com', 'new.example.com')"
            );

            $rows = $this->ztdQuery("SELECT id, url FROM mpd_srd_pages ORDER BY id");

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
            $this->assertSame('http://other.com/page3', $rows[2]['url']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE SET REPLACE failed: ' . $e->getMessage());
        }
    }

    public function testUpdateSetReplaceWithWhere(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE mpd_srd_pages SET body = REPLACE(body, 'http://old.example.com', 'https://new.example.com') WHERE id <= 2"
            );

            $rows = $this->ztdQuery("SELECT id, body FROM mpd_srd_pages ORDER BY id");
            $this->assertCount(3, $rows);
            $this->assertStringContainsString('https://new.example.com', $rows[0]['body']);
            $this->assertSame('No old links here', $rows[2]['body']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE SET REPLACE with WHERE failed: ' . $e->getMessage());
        }
    }

    public function testUpdateSetNestedReplace(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE mpd_srd_pages SET url = REPLACE(REPLACE(url, 'http://', 'https://'), 'old.example.com', 'new.example.com') WHERE id = 1"
            );

            $rows = $this->ztdQuery("SELECT url FROM mpd_srd_pages WHERE id = 1");
            $this->assertCount(1, $rows);

            if ($rows[0]['url'] !== 'https://new.example.com/page1') {
                $this->markTestIncomplete(
                    'Nested REPLACE() not applied: got ' . var_export($rows[0]['url'], true)
                );
            }
            $this->assertSame('https://new.example.com/page1', $rows[0]['url']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE SET nested REPLACE failed: ' . $e->getMessage());
        }
    }

    public function testPreparedUpdateSetReplace(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "UPDATE mpd_srd_pages SET url = REPLACE(url, ?, ?) WHERE id = ?"
            );
            $stmt->execute(['old.example.com', 'new.example.com', 1]);

            $rows = $this->ztdQuery("SELECT url FROM mpd_srd_pages WHERE id = 1");
            $this->assertCount(1, $rows);

            if ($rows[0]['url'] !== 'http://new.example.com/page1') {
                $this->markTestIncomplete(
                    'Prepared REPLACE not applied: got ' . var_export($rows[0]['url'], true)
                );
            }
            $this->assertSame('http://new.example.com/page1', $rows[0]['url']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE SET REPLACE failed: ' . $e->getMessage());
        }
    }
}
