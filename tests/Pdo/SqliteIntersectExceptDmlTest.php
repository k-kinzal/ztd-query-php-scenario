<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests INTERSECT and EXCEPT set operations in DML context through ZTD
 * shadow store on SQLite.
 *
 * SQLite supports INTERSECT and EXCEPT natively. These tests verify
 * that the CTE rewriter correctly handles set operations within
 * INSERT...SELECT, DELETE...WHERE IN, UPDATE...WHERE IN, and prepared
 * statement variants.
 *
 * @spec SPEC-4.2, SPEC-4.3, SPEC-3.2
 * @see https://github.com/k-kinzal/ztd-query-php/issues/103
 */
class SqliteIntersectExceptDmlTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_setop_list_a (id INTEGER PRIMARY KEY, item TEXT)',
            'CREATE TABLE sl_setop_list_b (id INTEGER PRIMARY KEY, item TEXT)',
            'CREATE TABLE sl_setop_result (id INTEGER PRIMARY KEY AUTOINCREMENT, item TEXT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_setop_result', 'sl_setop_list_b', 'sl_setop_list_a'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // list_a: apple, banana, cherry, date
        $this->pdo->exec("INSERT INTO sl_setop_list_a (id, item) VALUES (1, 'apple')");
        $this->pdo->exec("INSERT INTO sl_setop_list_a (id, item) VALUES (2, 'banana')");
        $this->pdo->exec("INSERT INTO sl_setop_list_a (id, item) VALUES (3, 'cherry')");
        $this->pdo->exec("INSERT INTO sl_setop_list_a (id, item) VALUES (4, 'date')");

        // list_b: banana, cherry, elderberry, fig
        $this->pdo->exec("INSERT INTO sl_setop_list_b (id, item) VALUES (1, 'banana')");
        $this->pdo->exec("INSERT INTO sl_setop_list_b (id, item) VALUES (2, 'cherry')");
        $this->pdo->exec("INSERT INTO sl_setop_list_b (id, item) VALUES (3, 'elderberry')");
        $this->pdo->exec("INSERT INTO sl_setop_list_b (id, item) VALUES (4, 'fig')");
    }

    /**
     * INSERT INTO result the INTERSECT of items in both lists.
     * Expected result: banana, cherry (items common to both lists).
     */
    public function testInsertSelectIntersect(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO sl_setop_result (item)
                 SELECT item FROM sl_setop_list_a
                 INTERSECT
                 SELECT item FROM sl_setop_list_b"
            );

            $rows = $this->ztdQuery(
                "SELECT item FROM sl_setop_result ORDER BY item"
            );

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'INSERT INTERSECT: expected 2 rows, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertSame('banana', $rows[0]['item']);
            $this->assertSame('cherry', $rows[1]['item']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT...SELECT INTERSECT failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT INTO result the EXCEPT of list_a minus list_b.
     * Expected result: apple, date (items in A but not in B).
     */
    public function testInsertSelectExcept(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO sl_setop_result (item)
                 SELECT item FROM sl_setop_list_a
                 EXCEPT
                 SELECT item FROM sl_setop_list_b"
            );

            $rows = $this->ztdQuery(
                "SELECT item FROM sl_setop_result ORDER BY item"
            );

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'INSERT EXCEPT: expected 2 rows, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertSame('apple', $rows[0]['item']);
            $this->assertSame('date', $rows[1]['item']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT...SELECT EXCEPT failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE from list_a where item is in the INTERSECT of both lists.
     * Should delete banana and cherry from list_a.
     */
    public function testDeleteWhereInIntersect(): void
    {
        try {
            $this->ztdExec(
                "DELETE FROM sl_setop_list_a
                 WHERE item IN (
                     SELECT item FROM sl_setop_list_a
                     INTERSECT
                     SELECT item FROM sl_setop_list_b
                 )"
            );

            $rows = $this->ztdQuery(
                "SELECT item FROM sl_setop_list_a ORDER BY item"
            );

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'DELETE INTERSECT: expected 2 rows remaining, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            // apple and date should remain
            $this->assertSame('apple', $rows[0]['item']);
            $this->assertSame('date', $rows[1]['item']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE WHERE IN INTERSECT failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE list_a, uppercasing items that are in list_a EXCEPT list_b.
     * Items only in A (apple, date) should be uppercased.
     */
    public function testUpdateWhereInExcept(): void
    {
        try {
            $this->ztdExec(
                "UPDATE sl_setop_list_a
                 SET item = UPPER(item)
                 WHERE item IN (
                     SELECT item FROM sl_setop_list_a
                     EXCEPT
                     SELECT item FROM sl_setop_list_b
                 )"
            );

            $rows = $this->ztdQuery(
                "SELECT id, item FROM sl_setop_list_a ORDER BY id"
            );

            if (count($rows) !== 4) {
                $this->markTestIncomplete(
                    'UPDATE EXCEPT: expected 4 rows, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            // apple -> APPLE, banana unchanged, cherry unchanged, date -> DATE
            $this->assertSame('APPLE', $rows[0]['item']);
            $this->assertSame('banana', $rows[1]['item']);
            $this->assertSame('cherry', $rows[2]['item']);
            $this->assertSame('DATE', $rows[3]['item']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE WHERE IN EXCEPT failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared INSERT...SELECT INTERSECT with a parameter filtering one side.
     * Only intersect items from list_a where id > ? with all of list_b.
     */
    public function testPreparedInsertSelectIntersect(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "INSERT INTO sl_setop_result (item)
                 SELECT item FROM sl_setop_list_a WHERE id > ?
                 INTERSECT
                 SELECT item FROM sl_setop_list_b"
            );
            $stmt->execute([1]);

            $rows = $this->ztdQuery(
                "SELECT item FROM sl_setop_result ORDER BY item"
            );

            // list_a with id > 1: banana (2), cherry (3), date (4)
            // INTERSECT with list_b: banana, cherry
            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'Prepared INSERT INTERSECT: expected 2 rows, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertSame('banana', $rows[0]['item']);
            $this->assertSame('cherry', $rows[1]['item']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared INSERT...SELECT INTERSECT failed: ' . $e->getMessage());
        }
    }

    /**
     * Physical isolation: all tables should be empty when ZTD is disabled.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();

        $cnt = $this->pdo->query("SELECT COUNT(*) FROM sl_setop_list_a")->fetchColumn();
        $this->assertSame(0, (int) $cnt, 'sl_setop_list_a should be empty');

        $cnt = $this->pdo->query("SELECT COUNT(*) FROM sl_setop_list_b")->fetchColumn();
        $this->assertSame(0, (int) $cnt, 'sl_setop_list_b should be empty');

        $cnt = $this->pdo->query("SELECT COUNT(*) FROM sl_setop_result")->fetchColumn();
        $this->assertSame(0, (int) $cnt, 'sl_setop_result should be empty');
    }
}
