<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests PDO::lastInsertId() behavior after shadow INSERT on MySQL.
 * Cross-platform verification of Issue #77 (discovered on SQLite).
 *
 * SQL patterns exercised: INSERT + lastInsertId with AUTO_INCREMENT,
 * multiple INSERTs, prepared INSERT, explicit PK.
 * @spec SPEC-4.7
 */
class MysqlLastInsertIdShadowTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE my_lid_auto (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100) NOT NULL
            ) ENGINE=InnoDB',
            'CREATE TABLE my_lid_manual (
                id INT PRIMARY KEY,
                name VARCHAR(100) NOT NULL
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['my_lid_manual', 'my_lid_auto'];
    }

    /**
     * lastInsertId after exec INSERT with AUTO_INCREMENT.
     */
    public function testLastInsertIdAfterExecInsert(): void
    {
        $this->ztdExec("INSERT INTO my_lid_auto (name) VALUES ('Alice')");
        $id = $this->pdo->lastInsertId();

        if ($id === '0' || $id === '' || $id === false) {
            $this->markTestIncomplete(
                'lastInsertId() returned ' . var_export($id, true) . ' after shadow INSERT on MySQL [Issue #77]'
            );
        }

        $this->assertGreaterThan(0, (int) $id);
    }

    /**
     * lastInsertId should advance after multiple INSERTs.
     */
    public function testLastInsertIdAdvancesAfterMultipleInserts(): void
    {
        $this->ztdExec("INSERT INTO my_lid_auto (name) VALUES ('First')");
        $id1 = $this->pdo->lastInsertId();

        $this->ztdExec("INSERT INTO my_lid_auto (name) VALUES ('Second')");
        $id2 = $this->pdo->lastInsertId();

        if ($id1 === '0' || $id2 === '0') {
            $this->markTestIncomplete(
                "lastInsertId() returned 0 after shadow INSERT on MySQL (id1={$id1}, id2={$id2}) [Issue #77]"
            );
        }

        $this->assertGreaterThan((int) $id1, (int) $id2);
    }

    /**
     * lastInsertId after prepared statement INSERT.
     */
    public function testLastInsertIdAfterPreparedInsert(): void
    {
        $stmt = $this->pdo->prepare("INSERT INTO my_lid_auto (name) VALUES (?)");
        $stmt->execute(['Carol']);
        $id = $this->pdo->lastInsertId();

        if ($id === '0' || $id === '' || $id === false) {
            $this->markTestIncomplete(
                'lastInsertId() returned ' . var_export($id, true) . ' after prepared shadow INSERT on MySQL [Issue #77]'
            );
        }

        $this->assertGreaterThan(0, (int) $id);
    }

    /**
     * lastInsertId with explicit PK.
     */
    public function testLastInsertIdWithExplicitPk(): void
    {
        $this->ztdExec("INSERT INTO my_lid_manual VALUES (42, 'Bob')");
        $id = $this->pdo->lastInsertId();

        if ($id === '0' || $id === '' || $id === false) {
            $this->markTestIncomplete(
                'lastInsertId() returned ' . var_export($id, true) . ' after shadow INSERT with explicit PK on MySQL [Issue #77]'
            );
        }
    }

    /**
     * lastInsertId should NOT change after UPDATE.
     */
    public function testLastInsertIdUnchangedAfterUpdate(): void
    {
        $this->ztdExec("INSERT INTO my_lid_manual VALUES (10, 'Eve')");
        $idAfterInsert = $this->pdo->lastInsertId();

        $this->ztdExec("UPDATE my_lid_manual SET name = 'Eve Updated' WHERE id = 10");
        $idAfterUpdate = $this->pdo->lastInsertId();

        $this->assertSame($idAfterInsert, $idAfterUpdate);
    }
}
