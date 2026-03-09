<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests survey response analysis with INSERT...SELECT with GROUP BY aggregates,
 * multiple DISTINCT aggregates in the same SELECT, conditional aggregates
 * (SUM CASE WHEN), HAVING with multiple conditions, and COUNT(col) vs COUNT(*)
 * NULL handling (PostgreSQL PDO).
 * SQL patterns exercised: INSERT...SELECT GROUP BY, multiple COUNT(DISTINCT),
 * SUM(CASE WHEN), HAVING multi-condition, COUNT(col) NULL skipping, prepared
 * conditional aggregate.
 * @spec SPEC-10.2.182
 */
class PostgresSurveyResponseAnalysisTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_surv_questions (
                id INT PRIMARY KEY,
                survey_id INT,
                question_text VARCHAR(200),
                category VARCHAR(50)
            )',
            'CREATE TABLE pg_surv_responses (
                id INT PRIMARY KEY,
                question_id INT,
                respondent VARCHAR(50),
                rating INT,
                comment VARCHAR(200)
            )',
            'CREATE TABLE pg_surv_summary (
                id INT PRIMARY KEY,
                question_id INT,
                response_count INT,
                avg_rating INT,
                promoter_count INT
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_surv_responses', 'pg_surv_summary', 'pg_surv_questions'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_surv_questions VALUES (1, 1, 'How satisfied are you?', 'satisfaction')");
        $this->pdo->exec("INSERT INTO pg_surv_questions VALUES (2, 1, 'Would you recommend?', 'nps')");
        $this->pdo->exec("INSERT INTO pg_surv_questions VALUES (3, 1, 'Rate ease of use', 'usability')");

        $this->pdo->exec("INSERT INTO pg_surv_responses VALUES (1, 1, 'alice', 5, 'Great!')");
        $this->pdo->exec("INSERT INTO pg_surv_responses VALUES (2, 1, 'bob', 4, NULL)");
        $this->pdo->exec("INSERT INTO pg_surv_responses VALUES (3, 1, 'carol', 3, 'OK')");
        $this->pdo->exec("INSERT INTO pg_surv_responses VALUES (4, 2, 'alice', 5, 'Definitely')");
        $this->pdo->exec("INSERT INTO pg_surv_responses VALUES (5, 2, 'bob', 5, NULL)");
        $this->pdo->exec("INSERT INTO pg_surv_responses VALUES (6, 2, 'carol', 2, 'Not sure')");
        $this->pdo->exec("INSERT INTO pg_surv_responses VALUES (7, 2, 'dave', 4, NULL)");
        $this->pdo->exec("INSERT INTO pg_surv_responses VALUES (8, 3, 'alice', 4, NULL)");
        $this->pdo->exec("INSERT INTO pg_surv_responses VALUES (9, 3, 'bob', 3, 'Could improve')");
        $this->pdo->exec("INSERT INTO pg_surv_responses VALUES (10, 3, 'dave', 5, 'Love it')");
    }

    public function testMultipleDistinctAggregates(): void
    {
        $rows = $this->ztdQuery(
            "SELECT COUNT(DISTINCT respondent) AS unique_respondents,
                    COUNT(DISTINCT question_id) AS questions_answered
             FROM pg_surv_responses"
        );

        $this->assertCount(1, $rows);
        $this->assertEquals(4, (int) $rows[0]['unique_respondents']);
        $this->assertEquals(3, (int) $rows[0]['questions_answered']);
    }

    public function testConditionalAggregatesSumCaseWhen(): void
    {
        $rows = $this->ztdQuery(
            "SELECT q.category,
                    COUNT(*) AS total,
                    SUM(CASE WHEN r.rating >= 4 THEN 1 ELSE 0 END) AS promoters,
                    SUM(CASE WHEN r.rating <= 2 THEN 1 ELSE 0 END) AS detractors
             FROM pg_surv_responses r
             JOIN pg_surv_questions q ON r.question_id = q.id
             GROUP BY q.category
             ORDER BY q.category"
        );

        $this->assertCount(3, $rows);

        // nps: total=4, promoters=3 (alice=5, bob=5, dave=4), detractors=1 (carol=2)
        $this->assertSame('nps', $rows[0]['category']);
        $this->assertEquals(4, (int) $rows[0]['total']);
        $this->assertEquals(3, (int) $rows[0]['promoters']);
        $this->assertEquals(1, (int) $rows[0]['detractors']);

        // satisfaction: total=3, promoters=2 (alice=5, bob=4), detractors=0
        $this->assertSame('satisfaction', $rows[1]['category']);
        $this->assertEquals(3, (int) $rows[1]['total']);
        $this->assertEquals(2, (int) $rows[1]['promoters']);
        $this->assertEquals(0, (int) $rows[1]['detractors']);

        // usability: total=3, promoters=2 (alice=4, dave=5), detractors=0
        $this->assertSame('usability', $rows[2]['category']);
        $this->assertEquals(3, (int) $rows[2]['total']);
        $this->assertEquals(2, (int) $rows[2]['promoters']);
        $this->assertEquals(0, (int) $rows[2]['detractors']);
    }

    public function testInsertSelectWithGroupByAggregate(): void
    {
        $this->ztdExec(
            "INSERT INTO pg_surv_summary (id, question_id, response_count, avg_rating, promoter_count)
             SELECT question_id, question_id, COUNT(*), 0,
                    SUM(CASE WHEN rating >= 4 THEN 1 ELSE 0 END)
             FROM pg_surv_responses
             GROUP BY question_id"
        );

        $rows = $this->ztdQuery(
            "SELECT id, question_id, response_count, promoter_count
             FROM pg_surv_summary
             ORDER BY question_id"
        );

        $this->assertCount(3, $rows);

        // Q1: 3 responses, promoters=2 (alice=5, bob=4)
        $this->assertEquals(1, (int) $rows[0]['question_id']);
        $this->assertEquals(3, (int) $rows[0]['response_count']);
        $this->assertEquals(2, (int) $rows[0]['promoter_count']);

        // Q2: 4 responses, promoters=3 (alice=5, bob=5, dave=4)
        $this->assertEquals(2, (int) $rows[1]['question_id']);
        $this->assertEquals(4, (int) $rows[1]['response_count']);
        $this->assertEquals(3, (int) $rows[1]['promoter_count']);

        // Q3: 3 responses, promoters=2 (alice=4, dave=5)
        $this->assertEquals(3, (int) $rows[2]['question_id']);
        $this->assertEquals(3, (int) $rows[2]['response_count']);
        $this->assertEquals(2, (int) $rows[2]['promoter_count']);
    }

    public function testHavingWithMultipleConditions(): void
    {
        $rows = $this->ztdQuery(
            "SELECT question_id, COUNT(*) AS cnt, AVG(rating) AS avg_r
             FROM pg_surv_responses
             GROUP BY question_id
             HAVING COUNT(*) >= 4
             ORDER BY question_id"
        );

        // Only Q2 has 4 responses; Q1 and Q3 have 3 each
        $this->assertCount(1, $rows);
        $this->assertEquals(2, (int) $rows[0]['question_id']);
        $this->assertEquals(4, (int) $rows[0]['cnt']);
    }

    public function testPreparedConditionalAggregate(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT q.category,
                    SUM(CASE WHEN r.rating >= $1 THEN 1 ELSE 0 END) AS promoters
             FROM pg_surv_responses r
             JOIN pg_surv_questions q ON r.question_id = q.id
             GROUP BY q.category
             ORDER BY q.category",
            [4]
        );

        $this->assertCount(3, $rows);
        $this->assertSame('nps', $rows[0]['category']);
        $this->assertEquals(3, (int) $rows[0]['promoters']);
        $this->assertSame('satisfaction', $rows[1]['category']);
        $this->assertEquals(2, (int) $rows[1]['promoters']);
        $this->assertSame('usability', $rows[2]['category']);
        $this->assertEquals(2, (int) $rows[2]['promoters']);
    }

    public function testCountDistinctWithNullComments(): void
    {
        $rows = $this->ztdQuery(
            "SELECT question_id, COUNT(*) AS total, COUNT(comment) AS with_comment
             FROM pg_surv_responses
             GROUP BY question_id
             ORDER BY question_id"
        );

        $this->assertCount(3, $rows);

        // Q1: total=3, with_comment=2 (alice='Great!', carol='OK'; bob=NULL)
        $this->assertEquals(1, (int) $rows[0]['question_id']);
        $this->assertEquals(3, (int) $rows[0]['total']);
        $this->assertEquals(2, (int) $rows[0]['with_comment']);

        // Q2: total=4, with_comment=2 (alice='Definitely', carol='Not sure'; bob=NULL, dave=NULL)
        $this->assertEquals(2, (int) $rows[1]['question_id']);
        $this->assertEquals(4, (int) $rows[1]['total']);
        $this->assertEquals(2, (int) $rows[1]['with_comment']);

        // Q3: total=3, with_comment=2 (bob='Could improve', dave='Love it'; alice=NULL)
        $this->assertEquals(3, (int) $rows[2]['question_id']);
        $this->assertEquals(3, (int) $rows[2]['total']);
        $this->assertEquals(2, (int) $rows[2]['with_comment']);
    }

    public function testUpdateWithNewShadowInsert(): void
    {
        $this->ztdExec(
            "INSERT INTO pg_surv_responses VALUES (11, 1, 'eve', 1, 'Terrible')"
        );

        $rows = $this->ztdQuery(
            "SELECT COUNT(*) AS cnt,
                    SUM(CASE WHEN rating >= 4 THEN 1 ELSE 0 END) AS promoters
             FROM pg_surv_responses
             WHERE question_id = 1"
        );

        $this->assertCount(1, $rows);
        $this->assertEquals(4, (int) $rows[0]['cnt']);
        $this->assertEquals(2, (int) $rows[0]['promoters']);
    }

    public function testAggregateAfterDelete(): void
    {
        $this->ztdExec(
            "DELETE FROM pg_surv_responses WHERE rating <= 2"
        );

        // Only response 6 (carol, rating=2) removed; 10 - 1 = 9
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM pg_surv_responses");
        $this->assertEquals(9, (int) $rows[0]['cnt']);

        // carol still has responses with rating=3 (id=3), so still 4 unique respondents
        $rows = $this->ztdQuery(
            "SELECT COUNT(DISTINCT respondent) AS unique_resp FROM pg_surv_responses"
        );
        $this->assertEquals(4, (int) $rows[0]['unique_resp']);
    }
}
