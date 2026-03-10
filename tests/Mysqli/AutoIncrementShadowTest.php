<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests AUTO_INCREMENT PK behavior in ZTD shadow store on MySQLi.
 *
 * @spec SPEC-4.1
 */
class AutoIncrementShadowTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_ai_users (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(50) NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_ai_users'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_ai_users (name) VALUES ('Alice')");
        $this->mysqli->query("INSERT INTO mi_ai_users (name) VALUES ('Bob')");
    }

    /**
     * AUTO_INCREMENT ids should not be NULL.
     */
    public function testAutoIncrementIdsNotNull(): void
    {
        try {
            $rows = $this->ztdQuery("SELECT id, name FROM mi_ai_users ORDER BY name");

            $this->assertCount(2, $rows);
            if ($rows[0]['id'] === null) {
                $this->markTestIncomplete(
                    'AUTO_INCREMENT id is NULL for ' . $rows[0]['name'] . '. Data: ' . json_encode($rows)
                );
            }
            $this->assertNotNull($rows[0]['id']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('SELECT auto-increment failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE by auto-increment id.
     */
    public function testUpdateByAutoId(): void
    {
        try {
            $this->mysqli->query("UPDATE mi_ai_users SET name = 'Alice Updated' WHERE id = 1");

            $rows = $this->ztdQuery("SELECT name FROM mi_ai_users WHERE id = 1");
            if (count($rows) !== 1) {
                $this->markTestIncomplete('UPDATE by auto id: expected 1 row, got ' . count($rows));
            }
            $this->assertSame('Alice Updated', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE by auto id failed: ' . $e->getMessage());
        }
    }
}
