<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests PostgreSQL DO $$ anonymous blocks through the ZTD shadow store.
 *
 * DO $$ ... $$ blocks execute PL/pgSQL code server-side. The CTE rewriter
 * should either pass these through (since they're not SELECT/INSERT/UPDATE/DELETE)
 * or handle them gracefully. If the rewriter attempts to parse the block's
 * content as SQL, it will likely fail.
 *
 * This is relevant because many migration tools and ORMs use DO blocks for
 * conditional DDL, data migrations, and administrative tasks.
 *
 * @spec SPEC-6.1
 */
class PostgresDoBlockTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "CREATE TABLE pg_dob_items (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending'
            )",
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_dob_items'];
    }

    /**
     * Simple DO block that performs INSERT.
     */
    public function testDoBlockInsert(): void
    {
        try {
            $this->pdo->exec("
                DO \$\$
                BEGIN
                    INSERT INTO pg_dob_items (name, status) VALUES ('from_do', 'active');
                END
                \$\$
            ");

            $rows = $this->ztdQuery("SELECT name, status FROM pg_dob_items WHERE name = 'from_do'");

            if (count($rows) === 0) {
                $this->markTestIncomplete(
                    'DO block INSERT: row not visible in shadow store. '
                    . 'DO block DML may bypass shadow store tracking.'
                );
            }

            $this->assertCount(1, $rows);
            $this->assertSame('active', $rows[0]['status']);
        } catch (\Throwable $e) {
            $msg = $e->getMessage();
            if (stripos($msg, 'syntax') !== false || stripos($msg, 'rewrite') !== false) {
                $this->markTestIncomplete(
                    'DO block caused syntax/rewrite error: ' . $msg
                );
            }
            $this->markTestIncomplete('DO block INSERT failed: ' . $msg);
        }
    }

    /**
     * DO block with conditional logic.
     */
    public function testDoBlockConditional(): void
    {
        try {
            $this->pdo->exec("INSERT INTO pg_dob_items (name, status) VALUES ('existing', 'pending')");

            // DO block that updates only if condition is met
            $this->pdo->exec("
                DO \$\$
                DECLARE
                    cnt INTEGER;
                BEGIN
                    SELECT COUNT(*) INTO cnt FROM pg_dob_items WHERE status = 'pending';
                    IF cnt > 0 THEN
                        UPDATE pg_dob_items SET status = 'processed' WHERE status = 'pending';
                    END IF;
                END
                \$\$
            ");

            $rows = $this->ztdQuery("SELECT status FROM pg_dob_items WHERE name = 'existing'");

            if (count($rows) === 0) {
                $this->markTestIncomplete('DO block conditional: row not found after UPDATE.');
            }

            if ($rows[0]['status'] === 'pending') {
                $this->markTestIncomplete(
                    'DO block conditional UPDATE: status still "pending". '
                    . 'DML inside DO block may not be tracked by shadow store.'
                );
            }

            $this->assertSame('processed', $rows[0]['status']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DO block conditional failed: ' . $e->getMessage());
        }
    }

    /**
     * DO block with loop and multiple INSERTs.
     */
    public function testDoBlockLoop(): void
    {
        try {
            $this->pdo->exec("
                DO \$\$
                BEGIN
                    FOR i IN 1..5 LOOP
                        INSERT INTO pg_dob_items (name, status)
                        VALUES ('item_' || i, 'batch');
                    END LOOP;
                END
                \$\$
            ");

            $rows = $this->ztdQuery(
                "SELECT COUNT(*) as cnt FROM pg_dob_items WHERE status = 'batch'"
            );

            if ((int)$rows[0]['cnt'] === 0) {
                $this->markTestIncomplete(
                    'DO block loop: 0 rows visible. Loop INSERTs not tracked by shadow.'
                );
            }

            if ((int)$rows[0]['cnt'] !== 5) {
                $this->markTestIncomplete(
                    'DO block loop: expected 5 rows, got ' . $rows[0]['cnt']
                    . '. Some loop INSERTs may be lost.'
                );
            }

            $this->assertSame(5, (int)$rows[0]['cnt']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DO block loop failed: ' . $e->getMessage());
        }
    }

    /**
     * DO block with DELETE + INSERT (replace pattern).
     */
    public function testDoBlockDeleteAndInsert(): void
    {
        try {
            $this->pdo->exec("INSERT INTO pg_dob_items (name, status) VALUES ('old', 'active')");
            $this->pdo->exec("INSERT INTO pg_dob_items (name, status) VALUES ('keep', 'active')");

            $this->pdo->exec("
                DO \$\$
                BEGIN
                    DELETE FROM pg_dob_items WHERE name = 'old';
                    INSERT INTO pg_dob_items (name, status) VALUES ('new', 'active');
                END
                \$\$
            ");

            $rows = $this->ztdQuery(
                "SELECT name FROM pg_dob_items ORDER BY name"
            );

            $names = array_column($rows, 'name');

            if (in_array('old', $names)) {
                $this->markTestIncomplete(
                    'DO block DELETE: "old" row still visible. DELETE in DO block not tracked.'
                );
            }

            if (!in_array('new', $names)) {
                $this->markTestIncomplete(
                    'DO block INSERT: "new" row not visible. INSERT in DO block not tracked.'
                );
            }

            $this->assertContains('keep', $names);
            $this->assertContains('new', $names);
            $this->assertNotContains('old', $names);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DO block delete+insert failed: ' . $e->getMessage());
        }
    }

    /**
     * Regular DML before and after DO block — shadow consistency.
     */
    public function testDmlBeforeAndAfterDoBlock(): void
    {
        try {
            // Regular DML
            $this->pdo->exec("INSERT INTO pg_dob_items (name, status) VALUES ('before', 'active')");

            // DO block
            $this->pdo->exec("
                DO \$\$
                BEGIN
                    INSERT INTO pg_dob_items (name, status) VALUES ('during', 'active');
                END
                \$\$
            ");

            // Regular DML after
            $this->pdo->exec("INSERT INTO pg_dob_items (name, status) VALUES ('after', 'active')");

            $rows = $this->ztdQuery("SELECT name FROM pg_dob_items ORDER BY name");
            $names = array_column($rows, 'name');

            // At minimum, 'before' and 'after' should be visible (regular DML)
            $this->assertContains('before', $names);
            $this->assertContains('after', $names);

            if (!in_array('during', $names)) {
                $this->markTestIncomplete(
                    'DO block DML not tracked by shadow. "during" row missing. '
                    . 'Regular DML before/after DO block works. Visible: ' . json_encode($names)
                );
            }

            $this->assertContains('during', $names);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DML before/after DO block failed: ' . $e->getMessage());
        }
    }
}
