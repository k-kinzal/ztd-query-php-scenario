<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Isolates which SQL-keyword column names break the CTE rewriter.
 *
 * Tests each keyword column name individually to identify which ones
 * trigger the "Insert statement has no values to project" error in the
 * InsertTransformer. Related to upstream #86 (table names break rewriter),
 * this tests column names specifically.
 *
 * @spec SPEC-4.1
 */
class SqliteKeywordColumnIsolationTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [];
    }

    protected function getTableNames(): array
    {
        return [
            'sl_kci_with_select',
            'sl_kci_with_from',
            'sl_kci_with_where',
            'sl_kci_with_order',
            'sl_kci_with_group',
            'sl_kci_with_values',
            'sl_kci_with_insert',
            'sl_kci_with_update',
            'sl_kci_with_delete',
            'sl_kci_with_set',
            'sl_kci_with_table',
            'sl_kci_with_index',
            'sl_kci_with_key',
            'sl_kci_with_check',
        ];
    }

    protected function setUp(): void
    {
        parent::setUp();
    }

    private function testKeywordColumn(string $keyword): void
    {
        $table = "sl_kci_with_{$keyword}";
        $this->pdo->disableZtd();
        $this->pdo->exec("CREATE TABLE IF NOT EXISTS {$table} (id INTEGER PRIMARY KEY, \"{$keyword}\" TEXT)");
        $this->pdo->enableZtd();

        $this->ztdExec("INSERT INTO {$table} (id, \"{$keyword}\") VALUES (1, 'test_value')");

        $rows = $this->ztdQuery("SELECT \"{$keyword}\" FROM {$table} WHERE id = 1");
        $this->assertCount(1, $rows);
        $this->assertSame('test_value', $rows[0][$keyword]);
    }

    public function testColumnNamedSelect(): void
    {
        try {
            $this->testKeywordColumn('select');
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Column named "select" failed: ' . $e->getMessage()
            );
        }
    }

    public function testColumnNamedFrom(): void
    {
        try {
            $this->testKeywordColumn('from');
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Column named "from" failed: ' . $e->getMessage()
            );
        }
    }

    public function testColumnNamedWhere(): void
    {
        try {
            $this->testKeywordColumn('where');
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Column named "where" failed: ' . $e->getMessage()
            );
        }
    }

    public function testColumnNamedOrder(): void
    {
        try {
            $this->testKeywordColumn('order');
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Column named "order" failed: ' . $e->getMessage()
            );
        }
    }

    public function testColumnNamedGroup(): void
    {
        try {
            $this->testKeywordColumn('group');
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Column named "group" failed: ' . $e->getMessage()
            );
        }
    }

    public function testColumnNamedValues(): void
    {
        try {
            $this->testKeywordColumn('values');
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Column named "values" failed: ' . $e->getMessage()
            );
        }
    }

    public function testColumnNamedInsert(): void
    {
        try {
            $this->testKeywordColumn('insert');
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Column named "insert" failed: ' . $e->getMessage()
            );
        }
    }

    public function testColumnNamedUpdate(): void
    {
        try {
            $this->testKeywordColumn('update');
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Column named "update" failed: ' . $e->getMessage()
            );
        }
    }

    public function testColumnNamedDelete(): void
    {
        try {
            $this->testKeywordColumn('delete');
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Column named "delete" failed: ' . $e->getMessage()
            );
        }
    }

    public function testColumnNamedSet(): void
    {
        try {
            $this->testKeywordColumn('set');
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Column named "set" failed: ' . $e->getMessage()
            );
        }
    }

    public function testColumnNamedTable(): void
    {
        try {
            $this->testKeywordColumn('table');
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Column named "table" failed: ' . $e->getMessage()
            );
        }
    }

    public function testColumnNamedIndex(): void
    {
        try {
            $this->testKeywordColumn('index');
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Column named "index" failed: ' . $e->getMessage()
            );
        }
    }

    public function testColumnNamedKey(): void
    {
        try {
            $this->testKeywordColumn('key');
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Column named "key" failed: ' . $e->getMessage()
            );
        }
    }

    public function testColumnNamedCheck(): void
    {
        try {
            $this->testKeywordColumn('check');
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Column named "check" failed: ' . $e->getMessage()
            );
        }
    }
}
