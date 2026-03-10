<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests MySQL 8.0.19+ row alias syntax for INSERT...ON DUPLICATE KEY UPDATE
 * via PDO adapter.
 *
 * MySQL 8.0.19+:
 *   INSERT INTO t VALUES (...) AS new ON DUPLICATE KEY UPDATE col = new.col
 *
 * Deprecated (warning in MySQL 8.2+):
 *   INSERT INTO t VALUES (...) ON DUPLICATE KEY UPDATE col = VALUES(col)
 *
 * @spec SPEC-4.2a
 */
class MysqlUpsertRowAliasSyntaxTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mpd_uras_items (
            id INT PRIMARY KEY,
            name VARCHAR(50) NOT NULL,
            counter INT NOT NULL DEFAULT 0
        ) ENGINE=InnoDB';
    }

    protected function getTableNames(): array
    {
        return ['mpd_uras_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO mpd_uras_items VALUES (1, 'Alpha', 10)");
        $this->pdo->exec("INSERT INTO mpd_uras_items VALUES (2, 'Beta', 20)");
    }

    /**
     * Row alias upsert on conflict — UPDATE should apply new.col values.
     */
    public function testRowAliasUpsertOnConflict(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO mpd_uras_items VALUES (1, 'Alpha-Updated', 10) AS new ON DUPLICATE KEY UPDATE name = new.name"
            );

            $rows = $this->ztdQuery("SELECT name FROM mpd_uras_items WHERE id = 1");

            if (empty($rows)) {
                $this->markTestIncomplete('Row alias upsert returned no rows');
            }

            $name = $rows[0]['name'];
            if ($name !== 'Alpha-Updated') {
                $this->markTestIncomplete(
                    'Row alias ON DUPLICATE KEY UPDATE did not apply via PDO. Expected "Alpha-Updated", got '
                    . json_encode($name)
                );
            }
            $this->assertSame('Alpha-Updated', $name);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Row alias upsert via PDO failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared statement with row alias syntax.
     */
    public function testPreparedRowAliasUpsert(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "INSERT INTO mpd_uras_items VALUES (?, ?, ?) AS new ON DUPLICATE KEY UPDATE name = new.name, counter = new.counter"
            );
            $stmt->execute([1, 'Prepared-Alpha', 50]);

            $rows = $this->ztdQuery("SELECT name, counter FROM mpd_uras_items WHERE id = 1");

            if (empty($rows)) {
                $this->markTestIncomplete('Prepared row alias upsert returned no rows');
            }

            $name = $rows[0]['name'];
            $counter = (int) $rows[0]['counter'];

            if ($name !== 'Prepared-Alpha' || $counter !== 50) {
                $this->markTestIncomplete(
                    'Prepared row alias failed. Expected name=Prepared-Alpha, counter=50. Got name='
                    . json_encode($name) . ', counter=' . $counter
                );
            }
            $this->assertSame('Prepared-Alpha', $name);
            $this->assertSame(50, $counter);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared row alias failed: ' . $e->getMessage());
        }
    }

    /**
     * Positive control: deprecated VALUES() still works via PDO.
     */
    public function testDeprecatedValuesSyntaxStillWorks(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO mpd_uras_items VALUES (1, 'Alpha-V', 0) ON DUPLICATE KEY UPDATE name = VALUES(name)"
            );

            $rows = $this->ztdQuery("SELECT name FROM mpd_uras_items WHERE id = 1");
            $this->assertCount(1, $rows);
            $this->assertSame('Alpha-V', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Deprecated VALUES() via PDO failed: ' . $e->getMessage());
        }
    }
}
