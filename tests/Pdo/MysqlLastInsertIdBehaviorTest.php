<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests lastInsertId() behavior with ZTD on MySQL via PDO.
 *
 * ZtdPdo::lastInsertId() delegates to the inner PDO connection.
 * Since ZTD rewrites INSERT into CTE-based SELECT, the database's
 * AUTO_INCREMENT counter is NOT updated by shadow INSERT operations.
 * @spec SPEC-4.7
 */
class MysqlLastInsertIdBehaviorTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pdo_lid_test (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(50), score INT)';
    }

    protected function getTableNames(): array
    {
        return ['pdo_lid_test'];
    }


    /**
     * lastInsertId() returns "0" after shadow INSERT.
     */
    public function testLastInsertIdReturnsZeroAfterShadowInsert(): void
    {
        $this->pdo->exec("INSERT INTO pdo_lid_test (name, score) VALUES ('Alice', 90)");

        $id = $this->pdo->lastInsertId();
        $this->assertSame('0', $id);
    }

    /**
     * lastInsertId() stays "0" across multiple shadow INSERTs.
     */
    public function testLastInsertIdStaysZeroAcrossInserts(): void
    {
        $this->pdo->exec("INSERT INTO pdo_lid_test (name, score) VALUES ('Alice', 90)");
        $this->pdo->exec("INSERT INTO pdo_lid_test (name, score) VALUES ('Bob', 80)");

        $this->assertSame('0', $this->pdo->lastInsertId());
    }

    /**
     * Shadow data is accessible even though lastInsertId returns "0".
     */
    public function testShadowDataAccessibleDespiteZeroId(): void
    {
        $this->pdo->exec("INSERT INTO pdo_lid_test (name, score) VALUES ('Alice', 90)");
        $this->assertSame('0', $this->pdo->lastInsertId());

        $stmt = $this->pdo->query("SELECT name FROM pdo_lid_test WHERE name = 'Alice'");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Alice', $row['name']);
    }

    /**
     * lastInsertId() works correctly when ZTD is disabled.
     */
    public function testLastInsertIdWorksWithZtdDisabled(): void
    {
        $this->pdo->disableZtd();
        $this->pdo->exec("INSERT INTO pdo_lid_test (name, score) VALUES ('Physical', 100)");

        $id = $this->pdo->lastInsertId();
        $this->assertNotSame('0', $id);
        $this->assertGreaterThan(0, (int) $id);
    }

    /**
     * Prepared statement also does not update lastInsertId.
     */
    public function testPreparedInsertDoesNotUpdateLastInsertId(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO pdo_lid_test (name, score) VALUES (?, ?)');
        $stmt->execute(['Alice', 90]);

        $this->assertSame('0', $this->pdo->lastInsertId());
    }

    /**
     * Physical isolation verification.
     *
     * Checks that a specific shadow-inserted row is not in the physical table.
     * (Other tests may have inserted physical data via disableZtd.)
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO pdo_lid_test (name, score) VALUES ('ShadowOnly', 42)");

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query("SELECT COUNT(*) FROM pdo_lid_test WHERE name = 'ShadowOnly'");
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
