<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests PostgreSQL-specific string functions through CTE shadow.
 *
 * Covers string_agg, regexp_replace, position, overlay, left/right,
 * and other PostgreSQL string functions.
 * @spec pending
 */
class PostgresStringFunctionsTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pg_sf_items (id INT PRIMARY KEY, name VARCHAR(50), category VARCHAR(30), description TEXT)';
    }

    protected function getTableNames(): array
    {
        return ['pg_sf_items'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_sf_items VALUES (1, 'Widget Pro', 'tools', 'A professional widget')");
        $this->pdo->exec("INSERT INTO pg_sf_items VALUES (2, 'Gadget Plus', 'electronics', 'An enhanced gadget')");
        $this->pdo->exec("INSERT INTO pg_sf_items VALUES (3, 'Super Widget', 'tools', 'The best widget ever')");
    }

    /**
     * STRING_AGG — aggregate string concatenation.
     */
    public function testStringAgg(): void
    {
        $stmt = $this->pdo->query(
            "SELECT STRING_AGG(name, ', ' ORDER BY name) AS names FROM pg_sf_items WHERE category = 'tools'"
        );
        $result = $stmt->fetchColumn();
        $this->assertSame('Super Widget, Widget Pro', $result);
    }

    /**
     * POSITION — find substring position.
     */
    public function testPositionFunction(): void
    {
        $stmt = $this->pdo->query(
            "SELECT POSITION('Pro' IN name) AS pos FROM pg_sf_items WHERE id = 1"
        );
        $this->assertEquals(8, (int) $stmt->fetchColumn());
    }

    /**
     * LEFT / RIGHT — substring from start/end.
     */
    public function testLeftRightFunctions(): void
    {
        $stmt = $this->pdo->query(
            "SELECT LEFT(name, 6) AS l, RIGHT(name, 3) AS r FROM pg_sf_items WHERE id = 1"
        );
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Widget', $row['l']);
        $this->assertSame('Pro', $row['r']);
    }

    /**
     * INITCAP — capitalize first letter of each word.
     */
    public function testInitcapFunction(): void
    {
        $stmt = $this->pdo->query(
            "SELECT INITCAP(description) AS cap FROM pg_sf_items WHERE id = 1"
        );
        $this->assertSame('A Professional Widget', $stmt->fetchColumn());
    }

    /**
     * REPEAT — repeat string N times.
     */
    public function testRepeatFunction(): void
    {
        $stmt = $this->pdo->query(
            "SELECT REPEAT('ab', 3) AS rep FROM pg_sf_items WHERE id = 1"
        );
        $this->assertSame('ababab', $stmt->fetchColumn());
    }

    /**
     * REGEXP_REPLACE — regex-based replacement.
     */
    public function testRegexpReplace(): void
    {
        $stmt = $this->pdo->query(
            "SELECT REGEXP_REPLACE(name, '\\s+', '-', 'g') AS slug FROM pg_sf_items WHERE id = 1"
        );
        $this->assertSame('Widget-Pro', $stmt->fetchColumn());
    }

    /**
     * String functions after shadow mutation.
     */
    public function testStringFunctionsAfterMutation(): void
    {
        $this->pdo->exec("UPDATE pg_sf_items SET name = 'Updated Name' WHERE id = 1");

        $stmt = $this->pdo->query("SELECT UPPER(name) AS up FROM pg_sf_items WHERE id = 1");
        $this->assertSame('UPDATED NAME', $stmt->fetchColumn());
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_sf_items');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
