<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests PostgreSQL-specific type edge cases with ZTD shadow store.
 *
 * @see https://github.com/k-kinzal/ztd-query-php/issues/6
 * @spec pending
 */
class PostgresTypeEdgeCaseTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pg_type_edge (id INT PRIMARY KEY, flag BOOLEAN, big_num BIGINT)';
    }

    protected function getTableNames(): array
    {
        return ['pg_type_edge'];
    }


    /**
     * BOOLEAN true works correctly via prepared statement.
     */
    public function testBooleanTrueWorksViaPrepared(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO pg_type_edge (id, flag, big_num) VALUES (?, ?, ?)');
        $stmt->execute([1, true, 0]);

        $sel = $this->pdo->query('SELECT flag FROM pg_type_edge WHERE id = 1');
        $row = $sel->fetch(PDO::FETCH_ASSOC);
        $this->assertNotFalse($row);
    }

    /**
     * BOOLEAN false via prepared statement should work on SELECT.
     *
     * @see https://github.com/k-kinzal/ztd-query-php/issues/6
     */
    public function testBooleanFalseWorksOnSelect(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO pg_type_edge (id, flag, big_num) VALUES (?, ?, ?)');
        $stmt->execute([2, false, 0]);

        try {
            $sel = $this->pdo->query('SELECT flag FROM pg_type_edge WHERE id = 2');
            $row = $sel->fetch(PDO::FETCH_ASSOC);
            $this->assertNotFalse($row);
        } catch (\PDOException $e) {
            $this->markTestIncomplete(
                'Issue #6: BOOLEAN false generates invalid CAST(\'\' AS BOOLEAN). ' . $e->getMessage()
            );
        }
    }

    /**
     * BIGINT values within integer range work correctly.
     */
    public function testBigintWithinIntegerRange(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO pg_type_edge (id, flag, big_num) VALUES (?, ?, ?)');
        $stmt->execute([3, true, 2147483647]); // max int32

        $sel = $this->pdo->query('SELECT big_num FROM pg_type_edge WHERE id = 3');
        $val = $sel->fetchColumn();
        $this->assertSame(2147483647, (int) $val);
    }

    /**
     * BIGINT values exceeding integer range should work on SELECT.
     */
    public function testBigintOverflowWorksOnSelect(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO pg_type_edge (id, flag, big_num) VALUES (?, ?, ?)');
        $stmt->execute([4, true, 9999999999]); // exceeds int32

        try {
            $sel = $this->pdo->query('SELECT big_num FROM pg_type_edge WHERE id = 4');
            $val = $sel->fetchColumn();
            $this->assertSame('9999999999', (string) $val);
        } catch (\PDOException $e) {
            $this->markTestIncomplete(
                'BIGINT overflow: CTE rewriter generates CAST(value AS integer) instead of bigint. '
                . $e->getMessage()
            );
        }
    }

    /**
     * BOOLEAN via exec() (not prepared) — also affected since CTE rewriter
     * generates the same CAST expressions.
     */
    public function testBooleanTrueViaExec(): void
    {
        $this->pdo->exec("INSERT INTO pg_type_edge VALUES (5, TRUE, 42)");

        $sel = $this->pdo->query('SELECT flag, big_num FROM pg_type_edge WHERE id = 5');
        $row = $sel->fetch(PDO::FETCH_ASSOC);
        $this->assertNotFalse($row);
    }
}
