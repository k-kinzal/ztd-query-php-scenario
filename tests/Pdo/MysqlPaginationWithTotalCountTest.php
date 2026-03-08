<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests the common API pattern: get paginated data AND total count in the same session.
 * This is a realistic workflow for REST APIs and data table UIs.
 * MySQL PDO requires PDO::PARAM_INT binding for LIMIT/OFFSET parameters.
 * @spec SPEC-3.1
 */
class MysqlPaginationWithTotalCountTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mp_ptc_articles (
            id INT PRIMARY KEY,
            title VARCHAR(255) NOT NULL,
            author VARCHAR(100) NOT NULL,
            category VARCHAR(50) NOT NULL,
            published TINYINT(1) NOT NULL DEFAULT 0,
            created_at DATE NOT NULL
        )';
    }

    protected function getTableNames(): array
    {
        return ['mp_ptc_articles'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO mp_ptc_articles VALUES (1, 'Intro to PHP', 'Alice', 'tutorial', 1, '2024-01-10')");
        $this->pdo->exec("INSERT INTO mp_ptc_articles VALUES (2, 'Advanced SQL', 'Bob', 'tutorial', 1, '2024-01-15')");
        $this->pdo->exec("INSERT INTO mp_ptc_articles VALUES (3, 'REST API Design', 'Alice', 'architecture', 1, '2024-02-01')");
        $this->pdo->exec("INSERT INTO mp_ptc_articles VALUES (4, 'Unit Testing', 'Charlie', 'tutorial', 1, '2024-02-10')");
        $this->pdo->exec("INSERT INTO mp_ptc_articles VALUES (5, 'Docker Basics', 'Bob', 'devops', 1, '2024-03-05')");
        $this->pdo->exec("INSERT INTO mp_ptc_articles VALUES (6, 'CI/CD Pipelines', 'Charlie', 'devops', 1, '2024-03-15')");
        $this->pdo->exec("INSERT INTO mp_ptc_articles VALUES (7, 'Database Indexing', 'Alice', 'tutorial', 0, '2024-04-01')");
        $this->pdo->exec("INSERT INTO mp_ptc_articles VALUES (8, 'GraphQL Intro', 'Bob', 'architecture', 1, '2024-04-10')");
        $this->pdo->exec("INSERT INTO mp_ptc_articles VALUES (9, 'Security Patterns', 'Alice', 'architecture', 1, '2024-05-01')");
        $this->pdo->exec("INSERT INTO mp_ptc_articles VALUES (10, 'Caching Strategies', 'Charlie', 'tutorial', 1, '2024-05-15')");
    }

    /**
     * Page 1 data + total count (two separate queries in same session).
     */
    public function testPage1DataAndTotalCount(): void
    {
        $countRows = $this->ztdQuery("SELECT COUNT(*) AS total FROM mp_ptc_articles");
        $total = (int) $countRows[0]['total'];
        $this->assertSame(10, $total);

        $stmt = $this->ztdPrepare(
            "SELECT id, title FROM mp_ptc_articles ORDER BY id LIMIT ? OFFSET ?"
        );
        $stmt->bindValue(1, 3, PDO::PARAM_INT);
        $stmt->bindValue(2, 0, PDO::PARAM_INT);
        $stmt->execute();
        $pageRows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(3, $pageRows);
        $this->assertSame('Intro to PHP', $pageRows[0]['title']);
        $this->assertSame('Advanced SQL', $pageRows[1]['title']);
        $this->assertSame('REST API Design', $pageRows[2]['title']);
    }

    /**
     * Total count updates after INSERT within same session.
     */
    public function testTotalCountUpdatesAfterInsert(): void
    {
        $countBefore = $this->ztdQuery("SELECT COUNT(*) AS total FROM mp_ptc_articles");
        $this->assertSame(10, (int) $countBefore[0]['total']);

        $this->pdo->exec("INSERT INTO mp_ptc_articles VALUES (11, 'New Article', 'Diana', 'tutorial', 1, '2024-06-01')");

        $countAfter = $this->ztdQuery("SELECT COUNT(*) AS total FROM mp_ptc_articles");
        $this->assertSame(11, (int) $countAfter[0]['total']);
    }

    /**
     * Paginate with filters + total filtered count.
     */
    public function testPaginateWithFilterAndFilteredCount(): void
    {
        // Total published tutorials: Intro to PHP, Advanced SQL, Unit Testing, Caching Strategies
        $filteredCount = $this->ztdPrepareAndExecute(
            "SELECT COUNT(*) AS total FROM mp_ptc_articles WHERE category = ? AND published = ?",
            ['tutorial', 1]
        );
        $this->assertSame(4, (int) $filteredCount[0]['total']);

        // Page 1 of published tutorials (page size 2) — use bindValue for LIMIT/OFFSET
        $stmt = $this->ztdPrepare(
            "SELECT title FROM mp_ptc_articles WHERE category = ? AND published = ? ORDER BY created_at LIMIT ? OFFSET ?"
        );
        $stmt->bindValue(1, 'tutorial', PDO::PARAM_STR);
        $stmt->bindValue(2, 1, PDO::PARAM_INT);
        $stmt->bindValue(3, 2, PDO::PARAM_INT);
        $stmt->bindValue(4, 0, PDO::PARAM_INT);
        $stmt->execute();
        $page1 = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(2, $page1);
        $this->assertSame('Intro to PHP', $page1[0]['title']);
        $this->assertSame('Advanced SQL', $page1[1]['title']);

        // Page 2 of published tutorials
        $stmt->bindValue(1, 'tutorial', PDO::PARAM_STR);
        $stmt->bindValue(2, 1, PDO::PARAM_INT);
        $stmt->bindValue(3, 2, PDO::PARAM_INT);
        $stmt->bindValue(4, 2, PDO::PARAM_INT);
        $stmt->execute();
        $page2 = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(2, $page2);
        $this->assertSame('Unit Testing', $page2[0]['title']);
        $this->assertSame('Caching Strategies', $page2[1]['title']);
    }

    /**
     * Empty page (offset beyond data) + total count.
     */
    public function testEmptyPageBeyondDataPlusTotalCount(): void
    {
        $countRows = $this->ztdQuery("SELECT COUNT(*) AS total FROM mp_ptc_articles");
        $this->assertSame(10, (int) $countRows[0]['total']);

        $stmt = $this->ztdPrepare(
            "SELECT id, title FROM mp_ptc_articles ORDER BY id LIMIT ? OFFSET ?"
        );
        $stmt->bindValue(1, 5, PDO::PARAM_INT);
        $stmt->bindValue(2, 100, PDO::PARAM_INT);
        $stmt->execute();
        $pageRows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(0, $pageRows);
    }

    /**
     * Change page size mid-session.
     */
    public function testChangePageSizeMidSession(): void
    {
        // First request: page size 3
        $stmt = $this->ztdPrepare(
            "SELECT title FROM mp_ptc_articles ORDER BY id LIMIT ? OFFSET ?"
        );
        $stmt->bindValue(1, 3, PDO::PARAM_INT);
        $stmt->bindValue(2, 0, PDO::PARAM_INT);
        $stmt->execute();
        $page1Small = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(3, $page1Small);

        // User changes to page size 5
        $stmt->bindValue(1, 5, PDO::PARAM_INT);
        $stmt->bindValue(2, 0, PDO::PARAM_INT);
        $stmt->execute();
        $page1Large = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(5, $page1Large);

        // Verify first 3 items match in both page sizes
        $this->assertSame($page1Small[0]['title'], $page1Large[0]['title']);
        $this->assertSame($page1Small[1]['title'], $page1Large[1]['title']);
        $this->assertSame($page1Small[2]['title'], $page1Large[2]['title']);
    }

    /**
     * Prepared COUNT(*) + prepared paginated query reuse.
     */
    public function testPreparedCountAndPreparedPage(): void
    {
        $countStmt = $this->ztdPrepare("SELECT COUNT(*) AS total FROM mp_ptc_articles WHERE author = ?");
        $pageStmt = $this->ztdPrepare("SELECT title FROM mp_ptc_articles WHERE author = ? ORDER BY created_at LIMIT ? OFFSET ?");

        // Alice's articles
        $countStmt->execute(['Alice']);
        $aliceCount = $countStmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(4, (int) $aliceCount[0]['total']);

        $pageStmt->bindValue(1, 'Alice', PDO::PARAM_STR);
        $pageStmt->bindValue(2, 2, PDO::PARAM_INT);
        $pageStmt->bindValue(3, 0, PDO::PARAM_INT);
        $pageStmt->execute();
        $alicePage1 = $pageStmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $alicePage1);
        $this->assertSame('Intro to PHP', $alicePage1[0]['title']);
        $this->assertSame('REST API Design', $alicePage1[1]['title']);
    }

    /**
     * Keyset (cursor) pagination + total count.
     */
    public function testKeysetPaginationWithTotalCount(): void
    {
        $countRows = $this->ztdQuery("SELECT COUNT(*) AS total FROM mp_ptc_articles");
        $this->assertSame(10, (int) $countRows[0]['total']);

        // First page: id > 0
        $page1 = $this->ztdPrepareAndExecute(
            "SELECT id, title FROM mp_ptc_articles WHERE id > ? ORDER BY id LIMIT 3",
            [0]
        );
        $this->assertCount(3, $page1);
        $this->assertSame(1, (int) $page1[0]['id']);
        $this->assertSame(3, (int) $page1[2]['id']);

        // Second page: cursor from last id of page 1
        $lastId = (int) $page1[2]['id'];
        $page2 = $this->ztdPrepareAndExecute(
            "SELECT id, title FROM mp_ptc_articles WHERE id > ? ORDER BY id LIMIT 3",
            [$lastId]
        );
        $this->assertCount(3, $page2);
        $this->assertSame(4, (int) $page2[0]['id']);
        $this->assertSame(6, (int) $page2[2]['id']);

        // Third page
        $lastId2 = (int) $page2[2]['id'];
        $page3 = $this->ztdPrepareAndExecute(
            "SELECT id, title FROM mp_ptc_articles WHERE id > ? ORDER BY id LIMIT 3",
            [$lastId2]
        );
        $this->assertCount(3, $page3);
        $this->assertSame(7, (int) $page3[0]['id']);

        // Fourth page (partial)
        $lastId3 = (int) $page3[2]['id'];
        $page4 = $this->ztdPrepareAndExecute(
            "SELECT id, title FROM mp_ptc_articles WHERE id > ? ORDER BY id LIMIT 3",
            [$lastId3]
        );
        $this->assertCount(1, $page4);
        $this->assertSame(10, (int) $page4[0]['id']);
    }

    /**
     * Physical isolation -- shadow data does not reach physical table.
     */
    public function testPhysicalIsolation(): void
    {
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mp_ptc_articles");
        $this->assertSame(10, (int) $rows[0]['cnt']);

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query("SELECT COUNT(*) FROM mp_ptc_articles");
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
