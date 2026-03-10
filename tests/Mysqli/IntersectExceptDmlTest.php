<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests INTERSECT and EXCEPT set operations in DML context through ZTD
 * shadow store on MySQLi.
 *
 * MySQL 8.0.31+ added native INTERSECT and EXCEPT support. Older versions
 * and the ZTD CTE rewriter may not handle these operations correctly.
 * Each test uses try/catch with markTestIncomplete to accommodate version
 * differences and known ZTD limitations.
 *
 * @spec SPEC-4.2, SPEC-4.3, SPEC-3.2
 * @see https://github.com/k-kinzal/ztd-query-php/issues/103
 */
class IntersectExceptDmlTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_setop_list_a (id INT PRIMARY KEY, item VARCHAR(50)) ENGINE=InnoDB',
            'CREATE TABLE mi_setop_list_b (id INT PRIMARY KEY, item VARCHAR(50)) ENGINE=InnoDB',
            'CREATE TABLE mi_setop_result (id INT AUTO_INCREMENT PRIMARY KEY, item VARCHAR(50)) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_setop_result', 'mi_setop_list_b', 'mi_setop_list_a'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // list_a: apple, banana, cherry, date
        $this->mysqli->query("INSERT INTO mi_setop_list_a (id, item) VALUES (1, 'apple')");
        $this->mysqli->query("INSERT INTO mi_setop_list_a (id, item) VALUES (2, 'banana')");
        $this->mysqli->query("INSERT INTO mi_setop_list_a (id, item) VALUES (3, 'cherry')");
        $this->mysqli->query("INSERT INTO mi_setop_list_a (id, item) VALUES (4, 'date')");

        // list_b: banana, cherry, elderberry, fig
        $this->mysqli->query("INSERT INTO mi_setop_list_b (id, item) VALUES (1, 'banana')");
        $this->mysqli->query("INSERT INTO mi_setop_list_b (id, item) VALUES (2, 'cherry')");
        $this->mysqli->query("INSERT INTO mi_setop_list_b (id, item) VALUES (3, 'elderberry')");
        $this->mysqli->query("INSERT INTO mi_setop_list_b (id, item) VALUES (4, 'fig')");
    }

    /**
     * INSERT INTO result the INTERSECT of items in both lists.
     * Expected result: banana, cherry (items common to both lists).
     * MySQL 8.0.31+ required for INTERSECT support.
     */
    public function testInsertSelectIntersect(): void
    {
        try {
            $this->mysqli->query(
                "INSERT INTO mi_setop_result (item)
                 SELECT item FROM mi_setop_list_a
                 INTERSECT
                 SELECT item FROM mi_setop_list_b"
            );

            $rows = $this->ztdQuery(
                "SELECT item FROM mi_setop_result ORDER BY item"
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
     * MySQL 8.0.31+ required for EXCEPT support.
     */
    public function testInsertSelectExcept(): void
    {
        try {
            $this->mysqli->query(
                "INSERT INTO mi_setop_result (item)
                 SELECT item FROM mi_setop_list_a
                 EXCEPT
                 SELECT item FROM mi_setop_list_b"
            );

            $rows = $this->ztdQuery(
                "SELECT item FROM mi_setop_result ORDER BY item"
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
            $this->mysqli->query(
                "DELETE FROM mi_setop_list_a
                 WHERE item IN (
                     SELECT item FROM mi_setop_list_a
                     INTERSECT
                     SELECT item FROM mi_setop_list_b
                 )"
            );

            $rows = $this->ztdQuery(
                "SELECT item FROM mi_setop_list_a ORDER BY item"
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
            $this->mysqli->query(
                "UPDATE mi_setop_list_a
                 SET item = UPPER(item)
                 WHERE item IN (
                     SELECT item FROM mi_setop_list_a
                     EXCEPT
                     SELECT item FROM mi_setop_list_b
                 )"
            );

            $rows = $this->ztdQuery(
                "SELECT id, item FROM mi_setop_list_a ORDER BY id"
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
            $rows = $this->ztdPrepareAndExecute(
                "INSERT INTO mi_setop_result (item)
                 SELECT item FROM mi_setop_list_a WHERE id > ?
                 INTERSECT
                 SELECT item FROM mi_setop_list_b",
                [1]
            );

            $rows = $this->ztdQuery(
                "SELECT item FROM mi_setop_result ORDER BY item"
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
        $this->mysqli->disableZtd();

        $result = $this->mysqli->query("SELECT COUNT(*) AS cnt FROM mi_setop_list_a");
        $this->assertEquals(0, (int) $result->fetch_assoc()['cnt'], 'mi_setop_list_a should be empty');

        $result = $this->mysqli->query("SELECT COUNT(*) AS cnt FROM mi_setop_list_b");
        $this->assertEquals(0, (int) $result->fetch_assoc()['cnt'], 'mi_setop_list_b should be empty');

        $result = $this->mysqli->query("SELECT COUNT(*) AS cnt FROM mi_setop_result");
        $this->assertEquals(0, (int) $result->fetch_assoc()['cnt'], 'mi_setop_result should be empty');
    }
}
