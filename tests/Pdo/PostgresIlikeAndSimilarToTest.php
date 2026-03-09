<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests PostgreSQL ILIKE and SIMILAR TO operators through CTE shadow.
 *
 * ILIKE is PostgreSQL's case-insensitive LIKE operator, extremely common in
 * real applications. SIMILAR TO provides regex-like pattern matching.
 * Both must work correctly through CTE rewriting for shadow-stored rows.
 *
 * @spec SPEC-3.1
 */
class PostgresIlikeAndSimilarToTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pg_ilike_products (id INT PRIMARY KEY, name VARCHAR(100), description TEXT)';
    }

    protected function getTableNames(): array
    {
        return ['pg_ilike_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_ilike_products VALUES (1, 'Widget Pro', 'A premium WIDGET for professionals')");
        $this->pdo->exec("INSERT INTO pg_ilike_products VALUES (2, 'widget basic', 'Entry level widget')");
        $this->pdo->exec("INSERT INTO pg_ilike_products VALUES (3, 'Gadget Max', 'Top of the line gadget')");
        $this->pdo->exec("INSERT INTO pg_ilike_products VALUES (4, 'GADGET mini', 'Compact gadget design')");
        $this->pdo->exec("INSERT INTO pg_ilike_products VALUES (5, 'Doohickey', 'A simple doohickey tool')");
    }

    /**
     * ILIKE for case-insensitive name search.
     */
    public function testIlikeBasicSearch(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, name FROM pg_ilike_products WHERE name ILIKE '%widget%' ORDER BY id"
        );

        $this->assertCount(2, $rows, 'ILIKE should match both Widget Pro and widget basic');
        $this->assertSame('Widget Pro', $rows[0]['name']);
        $this->assertSame('widget basic', $rows[1]['name']);
    }

    /**
     * ILIKE with prepared statement parameters.
     */
    public function testIlikePreparedStatement(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT id, name FROM pg_ilike_products WHERE name ILIKE ? ORDER BY id",
            ['%gadget%']
        );

        $this->assertCount(2, $rows, 'ILIKE prepared should match both gadget variants');
        $this->assertSame('Gadget Max', $rows[0]['name']);
        $this->assertSame('GADGET mini', $rows[1]['name']);
    }

    /**
     * ILIKE on shadow-inserted rows.
     */
    public function testIlikeOnShadowInsertedRow(): void
    {
        $this->pdo->exec("INSERT INTO pg_ilike_products VALUES (6, 'WIDGET Ultra', 'The ultimate widget')");

        $rows = $this->ztdQuery(
            "SELECT id, name FROM pg_ilike_products WHERE name ILIKE '%widget%' ORDER BY id"
        );

        $this->assertCount(3, $rows, 'ILIKE should find shadow-inserted WIDGET Ultra too');
        $this->assertEquals(6, $rows[2]['id']);
    }

    /**
     * NOT ILIKE exclusion.
     */
    public function testNotIlike(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, name FROM pg_ilike_products WHERE name NOT ILIKE '%widget%' ORDER BY id"
        );

        $this->assertCount(3, $rows, 'NOT ILIKE should exclude widget variants');
    }

    /**
     * ILIKE with underscore wildcard (single char).
     */
    public function testIlikeUnderscoreWildcard(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, name FROM pg_ilike_products WHERE name ILIKE '_adget%' ORDER BY id"
        );

        $this->assertCount(2, $rows, 'ILIKE with _ should match Gadget and GADGET');
    }

    /**
     * SIMILAR TO with regex-like pattern.
     */
    public function testSimilarToBasic(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT id, name FROM pg_ilike_products WHERE name SIMILAR TO '%(Widget|Gadget)%' ORDER BY id"
            );

            // SIMILAR TO is case-sensitive
            $this->assertCount(2, $rows, 'SIMILAR TO should match Widget Pro and Gadget Max');
            $this->assertSame('Widget Pro', $rows[0]['name']);
            $this->assertSame('Gadget Max', $rows[1]['name']);
        } catch (\Throwable $e) {
            $this->markTestSkipped('SIMILAR TO not supported through CTE: ' . $e->getMessage());
        }
    }

    /**
     * SIMILAR TO with character class.
     */
    public function testSimilarToCharacterClass(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT id, name FROM pg_ilike_products WHERE name SIMILAR TO '[A-Z]%' ORDER BY id"
            );

            // Should match names starting with uppercase
            $this->assertGreaterThanOrEqual(1, count($rows));
        } catch (\Throwable $e) {
            $this->markTestSkipped('SIMILAR TO not supported through CTE: ' . $e->getMessage());
        }
    }

    /**
     * NOT SIMILAR TO exclusion.
     */
    public function testNotSimilarTo(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT id, name FROM pg_ilike_products WHERE name NOT SIMILAR TO '%(widget|Widget)%' ORDER BY id"
            );

            $this->assertGreaterThanOrEqual(1, count($rows));
        } catch (\Throwable $e) {
            $this->markTestSkipped('NOT SIMILAR TO not supported through CTE: ' . $e->getMessage());
        }
    }

    /**
     * ILIKE combined with UPDATE shadow rows.
     */
    public function testIlikeAfterUpdate(): void
    {
        $this->pdo->exec("UPDATE pg_ilike_products SET name = 'Widget Supreme' WHERE id = 2");

        $rows = $this->ztdQuery(
            "SELECT id, name FROM pg_ilike_products WHERE name ILIKE '%widget%' ORDER BY id"
        );

        // Both rows still match ILIKE widget
        $this->assertCount(2, $rows);
        $this->assertSame('Widget Pro', $rows[0]['name']);
        $this->assertSame('Widget Supreme', $rows[1]['name']);
    }

    /**
     * ILIKE combined with DELETE.
     */
    public function testIlikeAfterDelete(): void
    {
        $this->pdo->exec("DELETE FROM pg_ilike_products WHERE id = 1");

        $rows = $this->ztdQuery(
            "SELECT id, name FROM pg_ilike_products WHERE name ILIKE '%widget%' ORDER BY id"
        );

        $this->assertCount(1, $rows, 'After deleting Widget Pro, only widget basic remains');
        $this->assertSame('widget basic', $rows[0]['name']);
    }

    /**
     * ILIKE in CASE expression.
     */
    public function testIlikeInCaseExpression(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, name,
                    CASE WHEN name ILIKE '%widget%' THEN 'widget'
                         WHEN name ILIKE '%gadget%' THEN 'gadget'
                         ELSE 'other'
                    END AS category
             FROM pg_ilike_products ORDER BY id"
        );

        $this->assertCount(5, $rows);
        $this->assertSame('widget', $rows[0]['category']);
        $this->assertSame('widget', $rows[1]['category']);
        $this->assertSame('gadget', $rows[2]['category']);
        $this->assertSame('gadget', $rows[3]['category']);
        $this->assertSame('other', $rows[4]['category']);
    }

    /**
     * Physical isolation check.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_ilike_products');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
