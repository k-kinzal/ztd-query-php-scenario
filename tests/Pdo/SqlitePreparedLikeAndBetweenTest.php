<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests prepared statements with LIKE/BETWEEN/IN list through ZTD shadow store.
 *
 * These are very common query patterns in web applications (search, filtering,
 * pagination) that must work correctly through the CTE rewriter.
 * @spec SPEC-3.2
 */
class SqlitePreparedLikeAndBetweenTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE plb_items (id INT PRIMARY KEY, name VARCHAR(100), price INT, created VARCHAR(10))',
        ];
    }

    protected function getTableNames(): array
    {
        return ['plb_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO plb_items VALUES (1, 'Red Widget', 100, '2024-01-10')");
        $this->pdo->exec("INSERT INTO plb_items VALUES (2, 'Blue Widget', 150, '2024-02-15')");
        $this->pdo->exec("INSERT INTO plb_items VALUES (3, 'Red Gadget', 200, '2024-03-20')");
        $this->pdo->exec("INSERT INTO plb_items VALUES (4, 'Green Gadget', 250, '2024-04-25')");
        $this->pdo->exec("INSERT INTO plb_items VALUES (5, 'Blue Gizmo', 300, '2024-05-30')");
    }

    /**
     * Prepared LIKE with leading wildcard.
     */
    public function testPreparedLikeLeadingWildcard(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            'SELECT name FROM plb_items WHERE name LIKE ? ORDER BY name',
            ['%Widget']
        );

        $this->assertCount(2, $rows);
        $this->assertSame('Blue Widget', $rows[0]['name']);
        $this->assertSame('Red Widget', $rows[1]['name']);
    }

    /**
     * Prepared LIKE with both wildcards.
     */
    public function testPreparedLikeBothWildcards(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            'SELECT name FROM plb_items WHERE name LIKE ? ORDER BY name',
            ['%Red%']
        );

        $this->assertCount(2, $rows);
        $this->assertSame('Red Gadget', $rows[0]['name']);
        $this->assertSame('Red Widget', $rows[1]['name']);
    }

    /**
     * Prepared LIKE with trailing wildcard (prefix search).
     */
    public function testPreparedLikeTrailingWildcard(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            'SELECT name FROM plb_items WHERE name LIKE ? ORDER BY name',
            ['Blue%']
        );

        $this->assertCount(2, $rows);
        $this->assertSame('Blue Gizmo', $rows[0]['name']);
        $this->assertSame('Blue Widget', $rows[1]['name']);
    }

    /**
     * Prepared BETWEEN for range queries.
     */
    public function testPreparedBetween(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            'SELECT name, price FROM plb_items WHERE price BETWEEN ? AND ? ORDER BY price',
            [150, 250]
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Blue Widget', $rows[0]['name']);
        $this->assertSame('Red Gadget', $rows[1]['name']);
        $this->assertSame('Green Gadget', $rows[2]['name']);
    }

    /**
     * Prepared BETWEEN for date range.
     */
    public function testPreparedBetweenDates(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            'SELECT name FROM plb_items WHERE created BETWEEN ? AND ? ORDER BY created',
            ['2024-02-01', '2024-04-30']
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Blue Widget', $rows[0]['name']);
        $this->assertSame('Red Gadget', $rows[1]['name']);
        $this->assertSame('Green Gadget', $rows[2]['name']);
    }

    /**
     * Prepared LIKE after shadow mutation.
     */
    public function testPreparedLikeAfterInsert(): void
    {
        $this->pdo->exec("INSERT INTO plb_items VALUES (6, 'Red Doohickey', 175, '2024-06-01')");

        $rows = $this->ztdPrepareAndExecute(
            'SELECT name FROM plb_items WHERE name LIKE ? ORDER BY name',
            ['Red%']
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Red Doohickey', $rows[0]['name']);
        $this->assertSame('Red Gadget', $rows[1]['name']);
        $this->assertSame('Red Widget', $rows[2]['name']);
    }

    /**
     * Prepared LIKE combined with other conditions.
     */
    public function testPreparedLikeWithOtherConditions(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            'SELECT name, price FROM plb_items WHERE name LIKE ? AND price > ? ORDER BY price',
            ['%Gadget%', 100]
        );

        $this->assertCount(2, $rows);
        $this->assertSame('Red Gadget', $rows[0]['name']);
        $this->assertSame('Green Gadget', $rows[1]['name']);
    }

    /**
     * LIMIT/OFFSET with prepared parameters (pagination).
     */
    public function testPreparedLimitOffset(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            'SELECT name FROM plb_items ORDER BY id LIMIT ? OFFSET ?',
            [2, 1]
        );

        $this->assertCount(2, $rows);
        $this->assertSame('Blue Widget', $rows[0]['name']);
        $this->assertSame('Red Gadget', $rows[1]['name']);
    }

    /**
     * NOT LIKE with prepared parameter.
     */
    public function testPreparedNotLike(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            'SELECT name FROM plb_items WHERE name NOT LIKE ? ORDER BY name',
            ['%Widget%']
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Blue Gizmo', $rows[0]['name']);
        $this->assertSame('Green Gadget', $rows[1]['name']);
        $this->assertSame('Red Gadget', $rows[2]['name']);
    }

    /**
     * Physical table isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM plb_items');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
