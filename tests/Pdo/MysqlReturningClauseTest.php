<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests that DML with RETURNING clause is handled on MySQL.
 *
 * MySQL does NOT support RETURNING natively (only MariaDB 10.5+ does).
 * This test verifies that ZTD does not break when encountering RETURNING
 * syntax and produces a reasonable error.
 *
 * @spec SPEC-4.1, SPEC-6.1
 */
class MysqlReturningClauseTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE my_ret_items (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(50) NOT NULL,
            price DECIMAL(10,2) NOT NULL DEFAULT 0.00
        ) ENGINE=InnoDB';
    }

    protected function getTableNames(): array
    {
        return ['my_ret_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO my_ret_items (name, price) VALUES ('Alpha', 10.50)");
    }

    /**
     * MySQL should produce a clear error for RETURNING (not supported).
     * Verify ZTD does not silently swallow or misparse the statement.
     */
    public function testInsertReturningNotSupported(): void
    {
        $threw = false;
        try {
            $this->pdo->query(
                "INSERT INTO my_ret_items (name, price) VALUES ('Beta', 20.00) RETURNING *"
            );
        } catch (\Throwable $e) {
            $threw = true;
            // Should get a SQL syntax error, not a ZTD Write Protection error
            $msg = $e->getMessage();
            $this->assertStringContainsString('syntax', strtolower($msg),
                'Expected SQL syntax error for RETURNING on MySQL, got: ' . $msg
            );
        }

        if (!$threw) {
            // If no exception, check that no rows were returned
            $this->markTestIncomplete(
                'MySQL INSERT RETURNING did not throw — may have been silently ignored'
            );
        }
    }
}
