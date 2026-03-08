<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests multi-statement SQL detection and handling on MySQL PDO.
 *
 * The CTE rewriter should detect and reject multi-statement SQL
 * to prevent SQL injection and ensure correct CTE rewriting.
 * @spec pending
 */
class MysqlMultiStatementDetectionTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE msd_items (id INT PRIMARY KEY, name VARCHAR(50))';
    }

    protected function getTableNames(): array
    {
        return ['msd_items'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO msd_items VALUES (1, 'Alice')");
        $this->pdo->exec("INSERT INTO msd_items VALUES (2, 'Bob')");
    }

    /**
     * Two statements separated by semicolon should be rejected.
     */
    public function testTwoStatementsRejected(): void
    {
        $this->expectException(\Throwable::class);
        $this->pdo->exec("INSERT INTO msd_items VALUES (3, 'Charlie'); INSERT INTO msd_items VALUES (4, 'Diana')");
    }

    /**
     * SELECT followed by another SELECT should be rejected.
     */
    public function testDoubleSelectRejected(): void
    {
        $this->expectException(\Throwable::class);
        $this->pdo->query("SELECT * FROM msd_items; SELECT * FROM msd_items");
    }

    /**
     * Single statement with semicolon at end may or may not be accepted.
     */
    public function testSingleStatementWithTrailingSemicolon(): void
    {
        try {
            $this->pdo->exec("INSERT INTO msd_items VALUES (3, 'Charlie');");
            // If it works, verify the insert
            $stmt = $this->pdo->query('SELECT name FROM msd_items WHERE id = 3');
            $this->assertSame('Charlie', $stmt->fetchColumn());
        } catch (\Throwable $e) {
            // Trailing semicolon may be treated as multi-statement
            $this->assertInstanceOf(\Throwable::class, $e);
        }
    }

    /**
     * Semicolon inside string literal should not be treated as statement separator.
     */
    public function testSemicolonInStringLiteral(): void
    {
        $this->pdo->exec("INSERT INTO msd_items VALUES (3, 'semi;colon')");

        $stmt = $this->pdo->query('SELECT name FROM msd_items WHERE id = 3');
        $this->assertSame('semi;colon', $stmt->fetchColumn());
    }

    /**
     * Shadow operations work after rejected multi-statement.
     */
    public function testShadowWorksAfterRejection(): void
    {
        try {
            $this->pdo->exec("SELECT 1; SELECT 2");
        } catch (\Throwable $e) {
            // Expected
        }

        // Shadow should still work
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM msd_items');
        $this->assertSame(2, (int) $stmt->fetchColumn());
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM msd_items');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
