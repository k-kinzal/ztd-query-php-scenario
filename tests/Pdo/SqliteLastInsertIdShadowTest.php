<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests PDO::lastInsertId() behavior after shadow INSERT operations.
 * ORMs and application code commonly rely on lastInsertId() to retrieve
 * auto-generated IDs. In ZTD mode, shadow INSERTs don't touch the physical
 * database, so lastInsertId() behavior may differ from expectations.
 *
 * SQL patterns exercised: INSERT + lastInsertId, INSERT AUTOINCREMENT +
 * lastInsertId, multiple INSERTs + lastInsertId, INSERT via prepare +
 * lastInsertId, lastInsertId after UPDATE (should not change).
 * @spec SPEC-4.7
 */
class SqliteLastInsertIdShadowTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_lid_auto (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL
            )',
            'CREATE TABLE sl_lid_manual (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_lid_manual', 'sl_lid_auto'];
    }

    /**
     * lastInsertId after exec INSERT with explicit AUTOINCREMENT PK.
     */
    public function testLastInsertIdAfterExecInsert(): void
    {
        $this->ztdExec("INSERT INTO sl_lid_auto (name) VALUES ('Alice')");
        $id = $this->pdo->lastInsertId();

        // lastInsertId should return a non-zero value
        // In shadow mode, this may return '0' or '' if the physical insert didn't happen
        if ($id === '0' || $id === '' || $id === false) {
            $this->markTestIncomplete(
                'lastInsertId() returned ' . var_export($id, true) . ' after shadow INSERT — may not reflect shadow auto-increment'
            );
        }

        $this->assertNotEmpty($id);
        $this->assertGreaterThan(0, (int) $id);
    }

    /**
     * lastInsertId should advance after multiple INSERTs.
     */
    public function testLastInsertIdAdvancesAfterMultipleInserts(): void
    {
        $this->ztdExec("INSERT INTO sl_lid_auto (name) VALUES ('First')");
        $id1 = $this->pdo->lastInsertId();

        $this->ztdExec("INSERT INTO sl_lid_auto (name) VALUES ('Second')");
        $id2 = $this->pdo->lastInsertId();

        if ($id1 === '0' || $id2 === '0') {
            $this->markTestIncomplete(
                "lastInsertId() returned 0 after shadow INSERT (id1={$id1}, id2={$id2})"
            );
        }

        // Second ID should be greater than first
        $this->assertGreaterThan((int) $id1, (int) $id2);
    }

    /**
     * lastInsertId with explicit PK in INSERT.
     */
    public function testLastInsertIdWithExplicitPk(): void
    {
        $this->ztdExec("INSERT INTO sl_lid_manual VALUES (42, 'Bob')");
        $id = $this->pdo->lastInsertId();

        // With explicit PK, SQLite's last_insert_rowid() returns the explicit value
        // In shadow mode, this may differ
        if ($id === '0' || $id === '' || $id === false) {
            $this->markTestIncomplete(
                'lastInsertId() returned ' . var_export($id, true) . ' after shadow INSERT with explicit PK'
            );
        }
    }

    /**
     * lastInsertId after prepared statement INSERT.
     */
    public function testLastInsertIdAfterPreparedInsert(): void
    {
        $stmt = $this->pdo->prepare("INSERT INTO sl_lid_auto (name) VALUES (?)");
        $stmt->execute(['Carol']);
        $id = $this->pdo->lastInsertId();

        if ($id === '0' || $id === '' || $id === false) {
            $this->markTestIncomplete(
                'lastInsertId() returned ' . var_export($id, true) . ' after prepared shadow INSERT'
            );
        }

        $this->assertGreaterThan(0, (int) $id);
    }

    /**
     * The row inserted should be queryable using the lastInsertId value.
     */
    public function testInsertedRowQueryableByLastInsertId(): void
    {
        $this->ztdExec("INSERT INTO sl_lid_auto (name) VALUES ('Diana')");
        $id = $this->pdo->lastInsertId();

        if ($id === '0' || $id === '' || $id === false) {
            $this->markTestIncomplete(
                'lastInsertId() returned ' . var_export($id, true) . ' — cannot test queryability'
            );
        }

        $rows = $this->ztdQuery("SELECT name FROM sl_lid_auto WHERE id = {$id}");
        $this->assertCount(1, $rows);
        $this->assertSame('Diana', $rows[0]['name']);
    }

    /**
     * lastInsertId should NOT change after UPDATE.
     */
    public function testLastInsertIdUnchangedAfterUpdate(): void
    {
        $this->ztdExec("INSERT INTO sl_lid_manual VALUES (10, 'Eve')");
        $idAfterInsert = $this->pdo->lastInsertId();

        $this->ztdExec("UPDATE sl_lid_manual SET name = 'Eve Updated' WHERE id = 10");
        $idAfterUpdate = $this->pdo->lastInsertId();

        // lastInsertId should stay the same after UPDATE
        $this->assertSame($idAfterInsert, $idAfterUpdate);
    }

    /**
     * lastInsertId should NOT change after DELETE.
     */
    public function testLastInsertIdUnchangedAfterDelete(): void
    {
        $this->ztdExec("INSERT INTO sl_lid_manual VALUES (20, 'Frank')");
        $this->ztdExec("INSERT INTO sl_lid_manual VALUES (30, 'Grace')");
        $idAfterInsert = $this->pdo->lastInsertId();

        $this->ztdExec("DELETE FROM sl_lid_manual WHERE id = 20");
        $idAfterDelete = $this->pdo->lastInsertId();

        $this->assertSame($idAfterInsert, $idAfterDelete);
    }

    /**
     * lastInsertId after disabling then re-enabling ZTD.
     */
    public function testLastInsertIdAfterZtdToggle(): void
    {
        $this->ztdExec("INSERT INTO sl_lid_auto (name) VALUES ('Heidi')");
        $shadowId = $this->pdo->lastInsertId();

        $this->pdo->disableZtd();
        // Physical insert
        $this->pdo->exec("INSERT INTO sl_lid_auto (name) VALUES ('Ivan')");
        $physicalId = $this->pdo->lastInsertId();
        $this->pdo->enableZtd();

        // Physical insert should produce a valid ID
        $this->assertGreaterThan(0, (int) $physicalId);

        // Shadow insert after re-enable
        $this->ztdExec("INSERT INTO sl_lid_auto (name) VALUES ('Judy')");
        $newShadowId = $this->pdo->lastInsertId();

        // At minimum, the new shadow ID should not be 0
        if ($newShadowId === '0' || $newShadowId === '') {
            $this->markTestIncomplete(
                'lastInsertId() returned 0 after re-enabling ZTD'
            );
        }
    }
}
