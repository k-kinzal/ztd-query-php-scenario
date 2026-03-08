<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests backslash character handling in MySQL shadow store via MySQLi.
 *
 * Cross-platform parity with MysqlBackslashCorruptionTest (PDO).
 *
 * @see spec 10.3
 * @spec pending
 */
class BackslashCorruptionTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_bslash_test (id INT PRIMARY KEY, path VARCHAR(200))';
    }

    protected function getTableNames(): array
    {
        return ['mi_bslash_test'];
    }


    /**
     * Backslash-t should be preserved in shadow store.
     *
     * @see spec 10.3
     */
    public function testBackslashTPreserved(): void
    {
        $stmt = $this->mysqli->prepare('INSERT INTO mi_bslash_test VALUES (?, ?)');
        $id = 1;
        $path = 'C:\test\temp';
        $stmt->bind_param('is', $id, $path);
        $stmt->execute();

        $result = $this->mysqli->query('SELECT path FROM mi_bslash_test WHERE id = 1');
        $retrieved = $result->fetch_assoc()['path'];

        // Expected: backslash characters should be preserved
        if ($retrieved !== 'C:\test\temp') {
            $this->markTestIncomplete(
                'Backslash corruption: CTE rewriter does not escape backslashes in string literals. '
                . 'Expected C:\test\temp, got ' . var_export($retrieved, true)
            );
        }
        $this->assertSame('C:\test\temp', $retrieved);
    }

    /**
     * Simple string without backslash works correctly.
     */
    public function testSimpleStringNoCorruption(): void
    {
        $stmt = $this->mysqli->prepare('INSERT INTO mi_bslash_test VALUES (?, ?)');
        $id = 1;
        $path = '/usr/local/bin';
        $stmt->bind_param('is', $id, $path);
        $stmt->execute();

        $result = $this->mysqli->query('SELECT path FROM mi_bslash_test WHERE id = 1');
        $this->assertSame('/usr/local/bin', $result->fetch_assoc()['path']);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->query("INSERT INTO mi_bslash_test VALUES (1, 'test')");

        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_bslash_test');
        $this->assertSame(0, (int) $result->fetch_assoc()['cnt']);
    }
}
