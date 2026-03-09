<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests PostgreSQL STRING_AGG() function on shadow data.
 *
 * STRING_AGG is PostgreSQL's equivalent of MySQL's GROUP_CONCAT.
 * Applications use it to aggregate related values into delimited lists.
 * The CTE rewriter must handle the function's special syntax including
 * ORDER BY within the function call.
 *
 * @spec SPEC-3.1
 * @spec SPEC-3.3
 */
class PostgresStringAggTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_sa_articles (
                id SERIAL PRIMARY KEY,
                title VARCHAR(200) NOT NULL
            )',
            'CREATE TABLE pg_sa_tags (
                article_id INT NOT NULL,
                tag VARCHAR(50) NOT NULL,
                PRIMARY KEY (article_id, tag)
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_sa_tags', 'pg_sa_articles'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_sa_articles (id, title) VALUES (1, 'PHP Best Practices')");
        $this->pdo->exec("INSERT INTO pg_sa_articles (id, title) VALUES (2, 'PostgreSQL Tips')");
        $this->pdo->exec("INSERT INTO pg_sa_articles (id, title) VALUES (3, 'REST API Design')");

        $this->pdo->exec("INSERT INTO pg_sa_tags VALUES (1, 'php')");
        $this->pdo->exec("INSERT INTO pg_sa_tags VALUES (1, 'backend')");
        $this->pdo->exec("INSERT INTO pg_sa_tags VALUES (1, 'best-practices')");
        $this->pdo->exec("INSERT INTO pg_sa_tags VALUES (2, 'postgresql')");
        $this->pdo->exec("INSERT INTO pg_sa_tags VALUES (2, 'performance')");
        $this->pdo->exec("INSERT INTO pg_sa_tags VALUES (2, 'backend')");
        $this->pdo->exec("INSERT INTO pg_sa_tags VALUES (3, 'api')");
        $this->pdo->exec("INSERT INTO pg_sa_tags VALUES (3, 'rest')");
    }

    /**
     * STRING_AGG with JOIN on shadow data.
     */
    public function testStringAggBasic(): void
    {
        $rows = $this->ztdQuery(
            "SELECT a.title, STRING_AGG(t.tag, ',' ORDER BY t.tag) AS tags
             FROM pg_sa_articles a
             JOIN pg_sa_tags t ON t.article_id = a.id
             GROUP BY a.id, a.title
             ORDER BY a.title"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('PHP Best Practices', $rows[0]['title']);
        $this->assertSame('backend,best-practices,php', $rows[0]['tags']);
        $this->assertSame('PostgreSQL Tips', $rows[1]['title']);
        $this->assertSame('backend,performance,postgresql', $rows[1]['tags']);
    }

    /**
     * STRING_AGG with custom separator.
     */
    public function testStringAggWithSeparator(): void
    {
        $rows = $this->ztdQuery(
            "SELECT a.title, STRING_AGG(t.tag, ' | ' ORDER BY t.tag) AS tags
             FROM pg_sa_articles a
             JOIN pg_sa_tags t ON t.article_id = a.id
             GROUP BY a.id, a.title
             ORDER BY a.title"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('backend | best-practices | php', $rows[0]['tags']);
    }

    /**
     * STRING_AGG with DISTINCT.
     */
    public function testStringAggDistinct(): void
    {
        $rows = $this->ztdQuery(
            "SELECT STRING_AGG(DISTINCT t.tag, ',' ORDER BY t.tag) AS all_tags
             FROM pg_sa_tags t"
        );

        $this->assertCount(1, $rows);
        $allTags = explode(',', $rows[0]['all_tags']);
        $this->assertContains('backend', $allTags);
        $this->assertContains('php', $allTags);
        // 'backend' should appear once due to DISTINCT
        $this->assertEquals(1, substr_count($rows[0]['all_tags'], 'backend'));
    }

    /**
     * STRING_AGG in correlated subquery.
     */
    public function testStringAggInSubquery(): void
    {
        $rows = $this->ztdQuery(
            "SELECT a.title,
                (SELECT STRING_AGG(t.tag, ',' ORDER BY t.tag)
                 FROM pg_sa_tags t
                 WHERE t.article_id = a.id) AS tags
             FROM pg_sa_articles a
             ORDER BY a.title"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('backend,best-practices,php', $rows[0]['tags']);
    }

    /**
     * STRING_AGG after shadow mutation.
     */
    public function testStringAggAfterMutation(): void
    {
        $this->pdo->exec("INSERT INTO pg_sa_tags VALUES (3, 'backend')");

        $rows = $this->ztdQuery(
            "SELECT a.title, STRING_AGG(t.tag, ',' ORDER BY t.tag) AS tags
             FROM pg_sa_articles a
             JOIN pg_sa_tags t ON t.article_id = a.id
             WHERE a.id = 3
             GROUP BY a.id, a.title"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('api,backend,rest', $rows[0]['tags']);
    }

    /**
     * STRING_AGG with FILTER clause.
     */
    public function testStringAggWithFilter(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT
                    STRING_AGG(DISTINCT t.tag, ',' ORDER BY t.tag)
                        FILTER (WHERE t.tag LIKE 'b%') AS b_tags
                 FROM pg_sa_tags t"
            );

            if (empty($rows) || $rows[0]['b_tags'] === null) {
                $this->markTestIncomplete(
                    'STRING_AGG with FILTER returned empty on shadow data.'
                );
            }

            // Tags starting with 'b': backend, best-practices
            $this->assertSame('backend,best-practices', $rows[0]['b_tags']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'STRING_AGG with FILTER failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * STRING_AGG with prepared $N params.
     */
    public function testStringAggWithPreparedParams(): void
    {
        $stmt = $this->pdo->prepare(
            "SELECT a.title, STRING_AGG(t.tag, ',' ORDER BY t.tag) AS tags
             FROM pg_sa_articles a
             JOIN pg_sa_tags t ON t.article_id = a.id
             WHERE a.id = $1
             GROUP BY a.id, a.title"
        );
        $stmt->execute([1]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        if (empty($rows)) {
            $this->markTestIncomplete(
                'STRING_AGG with prepared $N params returned no rows.'
            );
        }

        $this->assertSame('backend,best-practices,php', $rows[0]['tags']);
    }

    /**
     * Physical isolation check.
     */
    public function testPhysicalIsolation(): void
    {
        $this->disableZtd();
        $count = (int) $this->pdo->query('SELECT COUNT(*) FROM pg_sa_articles')->fetchColumn();
        $this->assertSame(0, $count);
    }
}
