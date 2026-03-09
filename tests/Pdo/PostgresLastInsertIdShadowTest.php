<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests PDO::lastInsertId() behavior after shadow INSERT on PostgreSQL.
 * Cross-platform verification of Issue #77 (discovered on SQLite).
 * PostgreSQL uses SERIAL/BIGSERIAL or GENERATED AS IDENTITY for auto-increment.
 *
 * SQL patterns exercised: INSERT + lastInsertId with SERIAL, multiple INSERTs,
 * prepared INSERT.
 * @spec SPEC-4.7
 */
class PostgresLastInsertIdShadowTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_lid_auto (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL
            )',
            'CREATE TABLE pg_lid_manual (
                id INT PRIMARY KEY,
                name VARCHAR(100) NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_lid_manual', 'pg_lid_auto'];
    }

    /**
     * lastInsertId after exec INSERT with SERIAL.
     * PostgreSQL requires sequence name for lastInsertId().
     *
     * On PostgreSQL, this is worse than SQLite: it throws a PDOException
     * because currval() has no value when nextval() was never called
     * (shadow INSERT doesn't actually execute on the DB).
     */
    public function testLastInsertIdAfterExecInsert(): void
    {
        $this->ztdExec("INSERT INTO pg_lid_auto (name) VALUES ('Alice')");

        try {
            $id = $this->pdo->lastInsertId('pg_lid_auto_id_seq');
        } catch (\PDOException $e) {
            $this->markTestIncomplete(
                'SPEC-11.LAST-INSERT-ID [Issue #77]: lastInsertId() throws PDOException on PostgreSQL: ' . $e->getMessage()
            );
            return;
        }

        if ($id === '0' || $id === '' || $id === false) {
            $this->markTestIncomplete(
                'lastInsertId() returned ' . var_export($id, true) . ' after shadow INSERT on PostgreSQL [Issue #77]'
            );
        }

        $this->assertGreaterThan(0, (int) $id);
    }

    /**
     * lastInsertId should advance after multiple INSERTs.
     */
    public function testLastInsertIdAdvancesAfterMultipleInserts(): void
    {
        $this->ztdExec("INSERT INTO pg_lid_auto (name) VALUES ('First')");

        try {
            $id1 = $this->pdo->lastInsertId('pg_lid_auto_id_seq');
        } catch (\PDOException $e) {
            $this->markTestIncomplete(
                'SPEC-11.LAST-INSERT-ID [Issue #77]: lastInsertId() throws PDOException on PostgreSQL: ' . $e->getMessage()
            );
            return;
        }

        $this->ztdExec("INSERT INTO pg_lid_auto (name) VALUES ('Second')");
        $id2 = $this->pdo->lastInsertId('pg_lid_auto_id_seq');

        if ($id1 === '0' || $id2 === '0' || $id1 === false || $id2 === false) {
            $this->markTestIncomplete(
                "lastInsertId() returned 0 after shadow INSERT on PostgreSQL [Issue #77]"
            );
        }

        $this->assertGreaterThan((int) $id1, (int) $id2);
    }

    /**
     * lastInsertId after prepared statement INSERT.
     */
    public function testLastInsertIdAfterPreparedInsert(): void
    {
        $stmt = $this->pdo->prepare("INSERT INTO pg_lid_auto (name) VALUES ($1)");
        $stmt->execute(['Carol']);

        try {
            $id = $this->pdo->lastInsertId('pg_lid_auto_id_seq');
        } catch (\PDOException $e) {
            $this->markTestIncomplete(
                'SPEC-11.LAST-INSERT-ID [Issue #77]: lastInsertId() throws PDOException after prepared INSERT on PostgreSQL: ' . $e->getMessage()
            );
            return;
        }

        if ($id === '0' || $id === '' || $id === false) {
            $this->markTestIncomplete(
                'lastInsertId() returned ' . var_export($id, true) . ' after prepared shadow INSERT on PostgreSQL [Issue #77]'
            );
        }

        $this->assertGreaterThan(0, (int) $id);
    }

    /**
     * lastInsertId should NOT change after UPDATE.
     */
    public function testLastInsertIdUnchangedAfterUpdate(): void
    {
        $this->ztdExec("INSERT INTO pg_lid_manual VALUES (10, 'Eve')");
        $this->ztdExec("INSERT INTO pg_lid_auto (name) VALUES ('seed')");

        try {
            $idAfterInsert = $this->pdo->lastInsertId('pg_lid_auto_id_seq');
        } catch (\PDOException $e) {
            $this->markTestIncomplete(
                'SPEC-11.LAST-INSERT-ID [Issue #77]: lastInsertId() throws PDOException on PostgreSQL: ' . $e->getMessage()
            );
            return;
        }

        $this->ztdExec("UPDATE pg_lid_manual SET name = 'Eve Updated' WHERE id = 10");
        $idAfterUpdate = $this->pdo->lastInsertId('pg_lid_auto_id_seq');

        $this->assertSame($idAfterInsert, $idAfterUpdate);
    }
}
