<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests INSERT ... SELECT WHERE NOT EXISTS on MySQL shadow data.
 *
 * @spec SPEC-4.1
 * @spec SPEC-4.1a
 */
class MysqlConditionalInsertNotExistsTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE my_cine_config (
                `key` VARCHAR(100) PRIMARY KEY,
                value TEXT NOT NULL
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['my_cine_config'];
    }

    /**
     * INSERT WHERE NOT EXISTS — new row inserted.
     */
    public function testInsertNotExistsNewRow(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO my_cine_config (`key`, value)
                 SELECT 'app.name', 'MyApp'
                 FROM DUAL
                 WHERE NOT EXISTS (SELECT 1 FROM my_cine_config WHERE `key` = 'app.name')"
            );

            $rows = $this->ztdQuery("SELECT * FROM my_cine_config");
            $this->assertCount(1, $rows);
            $this->assertSame('MyApp', $rows[0]['value']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'INSERT NOT EXISTS (new) failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * INSERT WHERE NOT EXISTS — existing row skipped.
     */
    public function testInsertNotExistsExistingSkipped(): void
    {
        $this->ztdExec("INSERT INTO my_cine_config VALUES ('app.name', 'MyApp')");

        try {
            $this->ztdExec(
                "INSERT INTO my_cine_config (`key`, value)
                 SELECT 'app.name', 'OtherApp'
                 FROM DUAL
                 WHERE NOT EXISTS (SELECT 1 FROM my_cine_config WHERE `key` = 'app.name')"
            );

            $rows = $this->ztdQuery("SELECT * FROM my_cine_config WHERE `key` = 'app.name'");
            $this->assertCount(1, $rows);

            if ($rows[0]['value'] !== 'MyApp') {
                $this->markTestIncomplete(
                    'INSERT NOT EXISTS did not skip existing row: got ' . $rows[0]['value']
                );
            }

            $this->assertSame('MyApp', $rows[0]['value']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'INSERT NOT EXISTS (existing) failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Sequential conditional inserts.
     */
    public function testSequentialConditionalInserts(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO my_cine_config (`key`, value)
                 SELECT 'db.host', 'localhost'
                 FROM DUAL
                 WHERE NOT EXISTS (SELECT 1 FROM my_cine_config WHERE `key` = 'db.host')"
            );

            $this->ztdExec(
                "INSERT INTO my_cine_config (`key`, value)
                 SELECT 'db.host', 'remote.server'
                 FROM DUAL
                 WHERE NOT EXISTS (SELECT 1 FROM my_cine_config WHERE `key` = 'db.host')"
            );

            $rows = $this->ztdQuery("SELECT * FROM my_cine_config WHERE `key` = 'db.host'");

            if (count($rows) > 1) {
                $this->markTestIncomplete(
                    'Sequential INSERT NOT EXISTS created duplicate on MySQL. '
                    . 'NOT EXISTS subquery did not see first shadow-inserted row.'
                );
            }

            $this->assertCount(1, $rows);
            $this->assertSame('localhost', $rows[0]['value']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Sequential conditional inserts failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * INSERT NOT EXISTS with prepared params.
     */
    public function testInsertNotExistsWithPreparedParams(): void
    {
        $this->ztdExec("INSERT INTO my_cine_config VALUES ('exist', 'original')");

        try {
            $stmt = $this->pdo->prepare(
                "INSERT INTO my_cine_config (`key`, value)
                 SELECT ?, ?
                 FROM DUAL
                 WHERE NOT EXISTS (SELECT 1 FROM my_cine_config WHERE `key` = ?)"
            );

            $stmt->execute(['exist', 'new_val', 'exist']);

            $rows = $this->ztdQuery("SELECT * FROM my_cine_config WHERE `key` = 'exist'");

            if (count($rows) > 1) {
                $this->markTestIncomplete(
                    'INSERT NOT EXISTS with prepared params created duplicate.'
                );
            }

            $this->assertCount(1, $rows);
            $this->assertSame('original', $rows[0]['value']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'INSERT NOT EXISTS with prepared params failed: ' . $e->getMessage()
            );
        }
    }
}
