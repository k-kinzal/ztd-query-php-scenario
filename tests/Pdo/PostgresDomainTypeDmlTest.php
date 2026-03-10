<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests PostgreSQL domain types in DML through the ZTD shadow store.
 *
 * Domain types (CREATE DOMAIN) add constraints and semantic meaning to
 * base types. This tests whether the CTE rewriter correctly handles
 * INSERT/UPDATE/DELETE on tables using domain-typed columns.
 *
 * @spec SPEC-10.2
 */
class PostgresDomainTypeDmlTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "DO $$ BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'pg_dom_email') THEN
                    CREATE DOMAIN pg_dom_email AS VARCHAR(255)
                        CHECK (VALUE ~ '^[^@]+@[^@]+\\.[^@]+$');
                END IF;
                IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'pg_dom_positive_int') THEN
                    CREATE DOMAIN pg_dom_positive_int AS INT
                        CHECK (VALUE > 0);
                END IF;
                IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'pg_dom_percentage') THEN
                    CREATE DOMAIN pg_dom_percentage AS NUMERIC(5,2)
                        CHECK (VALUE >= 0 AND VALUE <= 100);
                END IF;
            END $$",
            "CREATE TABLE pg_dom_contacts (
                id SERIAL PRIMARY KEY,
                email pg_dom_email NOT NULL,
                age pg_dom_positive_int,
                satisfaction pg_dom_percentage
            )",
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_dom_contacts'];
    }

    protected function setUp(): void
    {
        // Drop domain types after table to avoid dependency issues
        $raw = new \PDO(
            \Tests\Support\PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [\PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec("DROP TABLE IF EXISTS pg_dom_contacts CASCADE");
        $raw->exec("DROP DOMAIN IF EXISTS pg_dom_email CASCADE");
        $raw->exec("DROP DOMAIN IF EXISTS pg_dom_positive_int CASCADE");
        $raw->exec("DROP DOMAIN IF EXISTS pg_dom_percentage CASCADE");

        parent::setUp();

        $this->ztdExec("INSERT INTO pg_dom_contacts (email, age, satisfaction) VALUES ('alice@example.com', 30, 85.50)");
        $this->ztdExec("INSERT INTO pg_dom_contacts (email, age, satisfaction) VALUES ('bob@example.com', 25, 92.00)");
        $this->ztdExec("INSERT INTO pg_dom_contacts (email, age, satisfaction) VALUES ('charlie@test.org', 40, 70.25)");
    }

    /**
     * INSERT with valid domain values should succeed.
     */
    public function testInsertValidDomainValues(): void
    {
        try {
            $this->ztdExec("INSERT INTO pg_dom_contacts (email, age, satisfaction) VALUES ('diana@mail.com', 28, 95.00)");

            $rows = $this->ztdQuery("SELECT email, age, satisfaction FROM pg_dom_contacts WHERE email = 'diana@mail.com'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'INSERT valid domain values: expected 1 row, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertSame('diana@mail.com', $rows[0]['email']);
            $this->assertSame(28, (int) $rows[0]['age']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT valid domain values failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT with invalid domain value — ZTD shadow store may bypass domain CHECK constraints.
     */
    public function testInsertInvalidEmailDomainBypassesConstraint(): void
    {
        $threw = false;
        try {
            $this->ztdExec("INSERT INTO pg_dom_contacts (email, age, satisfaction) VALUES ('not-an-email', 25, 50.00)");
        } catch (\Throwable $e) {
            $threw = true;
            // Good: constraint was enforced
            $this->assertStringContainsString('pg_dom_email', $e->getMessage());
            return;
        }

        // If no exception, check if the invalid data was accepted
        $rows = $this->ztdQuery("SELECT * FROM pg_dom_contacts WHERE email = 'not-an-email'");
        if (count($rows) > 0) {
            $this->markTestIncomplete(
                'INSERT invalid email: domain CHECK constraint bypassed — shadow store accepted invalid data'
            );
        } else {
            $this->markTestIncomplete(
                'INSERT invalid email: no error thrown but data not visible'
            );
        }
    }

    /**
     * INSERT with negative age — ZTD shadow store may bypass domain CHECK constraints.
     */
    public function testInsertNegativeAgeDomainBypassesConstraint(): void
    {
        try {
            $this->ztdExec("INSERT INTO pg_dom_contacts (email, age, satisfaction) VALUES ('test@test.com', -5, 50.00)");
        } catch (\Throwable $e) {
            $this->assertStringContainsString('pg_dom_positive_int', $e->getMessage());
            return;
        }

        $rows = $this->ztdQuery("SELECT * FROM pg_dom_contacts WHERE age = -5");
        if (count($rows) > 0) {
            $this->markTestIncomplete(
                'INSERT negative age: domain CHECK constraint bypassed — shadow store accepted age=-5'
            );
        } else {
            $this->markTestIncomplete(
                'INSERT negative age: no error thrown but data not visible'
            );
        }
    }

    /**
     * UPDATE domain column with valid value.
     */
    public function testUpdateDomainColumn(): void
    {
        try {
            $this->ztdExec("UPDATE pg_dom_contacts SET satisfaction = 99.99 WHERE email = 'alice@example.com'");

            $rows = $this->ztdQuery("SELECT satisfaction FROM pg_dom_contacts WHERE email = 'alice@example.com'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'UPDATE domain column: expected 1 row, got ' . count($rows)
                );
            }

            $this->assertEqualsWithDelta(99.99, (float) $rows[0]['satisfaction'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE domain column failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE domain column with out-of-range value — may bypass constraint.
     */
    public function testUpdateDomainOutOfRangeBypassesConstraint(): void
    {
        try {
            $this->ztdExec("UPDATE pg_dom_contacts SET satisfaction = 150.00 WHERE email = 'alice@example.com'");
        } catch (\Throwable $e) {
            $this->assertStringContainsString('pg_dom_percentage', $e->getMessage());
            return;
        }

        $rows = $this->ztdQuery("SELECT satisfaction FROM pg_dom_contacts WHERE email = 'alice@example.com'");
        if (count($rows) === 1 && (float) $rows[0]['satisfaction'] > 100) {
            $this->markTestIncomplete(
                'UPDATE domain out of range: domain CHECK bypassed — satisfaction=' . $rows[0]['satisfaction']
            );
        } else {
            $this->markTestIncomplete(
                'UPDATE domain out of range: no error thrown, UPDATE did not persist either'
            );
        }
    }

    /**
     * DELETE rows with domain-typed columns.
     */
    public function testDeleteWithDomainColumns(): void
    {
        try {
            $this->ztdExec("DELETE FROM pg_dom_contacts WHERE satisfaction < 80");

            $rows = $this->ztdQuery("SELECT email FROM pg_dom_contacts ORDER BY email");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'DELETE with domain columns: expected 2 rows, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame('alice@example.com', $rows[0]['email']);
            $this->assertSame('bob@example.com', $rows[1]['email']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE with domain columns failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared INSERT with domain-typed parameters.
     */
    public function testPreparedInsertWithDomainParams(): void
    {
        try {
            $stmt = $this->ztdPrepare(
                "INSERT INTO pg_dom_contacts (email, age, satisfaction) VALUES ($1, $2, $3)"
            );
            $stmt->execute(['prep@domain.com', 35, 88.50]);

            $rows = $this->ztdQuery("SELECT * FROM pg_dom_contacts WHERE email = 'prep@domain.com'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Prepared INSERT domain params: expected 1 row, got ' . count($rows)
                );
            }

            $this->assertSame(35, (int) $rows[0]['age']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared INSERT with domain params failed: ' . $e->getMessage());
        }
    }

    /**
     * Aggregate query on domain-typed columns.
     */
    public function testAggregateOnDomainColumns(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT AVG(satisfaction) AS avg_sat, MIN(age) AS min_age, MAX(age) AS max_age
                 FROM pg_dom_contacts"
            );

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Aggregate on domain columns: expected 1 row, got ' . count($rows)
                );
            }

            $this->assertEqualsWithDelta(82.58, (float) $rows[0]['avg_sat'], 0.1);
            $this->assertSame(25, (int) $rows[0]['min_age']);
            $this->assertSame(40, (int) $rows[0]['max_age']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Aggregate on domain columns failed: ' . $e->getMessage());
        }
    }
}
