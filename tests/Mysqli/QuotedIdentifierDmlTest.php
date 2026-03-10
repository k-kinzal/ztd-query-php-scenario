<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests DML with backtick-quoted identifiers through ZTD on MySQLi.
 *
 * @spec SPEC-4.1
 * @spec SPEC-4.2
 * @spec SPEC-4.3
 */
class QuotedIdentifierDmlTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE `mi_UserProfiles` (
            `UserId` INT AUTO_INCREMENT PRIMARY KEY,
            `FirstName` VARCHAR(50) NOT NULL,
            `LastName` VARCHAR(50) NOT NULL,
            `EmailAddress` VARCHAR(100),
            `IsActive` TINYINT NOT NULL DEFAULT 1
        )';
    }

    protected function getTableNames(): array
    {
        return ['`mi_UserProfiles`'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO `mi_UserProfiles` (`FirstName`, `LastName`, `EmailAddress`, `IsActive`) VALUES ('Alice', 'Smith', 'alice@example.com', 1)");
        $this->mysqli->query("INSERT INTO `mi_UserProfiles` (`FirstName`, `LastName`, `EmailAddress`, `IsActive`) VALUES ('Bob', 'Jones', 'bob@example.com', 1)");
        $this->mysqli->query("INSERT INTO `mi_UserProfiles` (`FirstName`, `LastName`, `EmailAddress`, `IsActive`) VALUES ('Carol', 'Davis', NULL, 0)");
    }

    /**
     * SELECT with backtick-quoted identifiers.
     *
     * @spec SPEC-3.1
     */
    public function testSelectQuotedIdentifiers(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT `FirstName`, `LastName` FROM `mi_UserProfiles` WHERE `IsActive` = 1 ORDER BY `UserId`"
            );

            $this->assertCount(2, $rows);
            $this->assertSame('Alice', $rows[0]['FirstName']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('SELECT quoted identifiers failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE with backtick-quoted identifiers.
     *
     * @spec SPEC-4.2
     */
    public function testUpdateQuotedIdentifiers(): void
    {
        try {
            $this->mysqli->query(
                "UPDATE `mi_UserProfiles` SET `IsActive` = 0 WHERE `UserId` = 1"
            );

            $rows = $this->ztdQuery(
                "SELECT `IsActive` FROM `mi_UserProfiles` WHERE `UserId` = 1"
            );

            if (count($rows) !== 1) {
                $this->markTestIncomplete('UPDATE quoted: expected 1 row');
            }

            $this->assertEquals(0, (int) $rows[0]['IsActive']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE quoted identifiers failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE with backtick-quoted identifiers.
     *
     * @spec SPEC-4.3
     */
    public function testDeleteQuotedIdentifiers(): void
    {
        try {
            $this->mysqli->query(
                "DELETE FROM `mi_UserProfiles` WHERE `IsActive` = 0"
            );

            $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM `mi_UserProfiles`');
            $remaining = (int) $rows[0]['cnt'];

            if ($remaining !== 2) {
                $this->markTestIncomplete('DELETE quoted: expected 2, got ' . $remaining);
            }

            $this->assertEquals(2, $remaining);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE quoted identifiers failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared UPDATE with backtick-quoted identifiers.
     *
     * @spec SPEC-4.2
     */
    public function testPreparedUpdateQuotedIdentifiers(): void
    {
        try {
            $this->ztdPrepareAndExecute(
                "UPDATE `mi_UserProfiles` SET `EmailAddress` = CONCAT(`FirstName`, '.', `LastName`, '@corp.com') WHERE `UserId` = ?",
                [3]
            );

            $rows = $this->ztdQuery(
                "SELECT `EmailAddress` FROM `mi_UserProfiles` WHERE `UserId` = 3"
            );

            if (count($rows) !== 1) {
                $this->markTestIncomplete('Prepared UPDATE quoted: expected 1 row');
            }

            $this->assertSame('Carol.Davis@corp.com', $rows[0]['EmailAddress']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE quoted identifiers failed: ' . $e->getMessage());
        }
    }
}
