<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests a classroom quiz scoring scenario through ZTD shadow store (MySQL PDO).
 * Teachers create quizzes with correct answers, students submit responses,
 * and the system calculates scores, pass/fail status, and class statistics.
 * SQL patterns exercised: multi-table JOIN with CASE for answer verification,
 * SUM CASE / COUNT for score percentage, ROUND, GROUP BY HAVING threshold,
 * NOT EXISTS for unsubmitted quizzes, prepared statement with multiple params.
 * @spec SPEC-10.2.150
 */
class MysqlClassroomQuizScoringTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mp_cqs_students (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100),
                grade_level INT
            )',
            'CREATE TABLE mp_cqs_quizzes (
                id INT AUTO_INCREMENT PRIMARY KEY,
                title VARCHAR(255),
                subject VARCHAR(50),
                passing_score INT
            )',
            'CREATE TABLE mp_cqs_questions (
                id INT AUTO_INCREMENT PRIMARY KEY,
                quiz_id INT,
                question_text VARCHAR(500),
                correct_answer VARCHAR(100)
            )',
            'CREATE TABLE mp_cqs_answers (
                id INT AUTO_INCREMENT PRIMARY KEY,
                student_id INT,
                question_id INT,
                quiz_id INT,
                given_answer VARCHAR(100)
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mp_cqs_answers', 'mp_cqs_questions', 'mp_cqs_quizzes', 'mp_cqs_students'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Students
        $this->pdo->exec("INSERT INTO mp_cqs_students VALUES (1, 'Alice', 10)");
        $this->pdo->exec("INSERT INTO mp_cqs_students VALUES (2, 'Bob', 10)");
        $this->pdo->exec("INSERT INTO mp_cqs_students VALUES (3, 'Carol', 10)");
        $this->pdo->exec("INSERT INTO mp_cqs_students VALUES (4, 'Dave', 11)");

        // Quiz 1: Math (pass = 60%)
        $this->pdo->exec("INSERT INTO mp_cqs_quizzes VALUES (1, 'Math Basics', 'math', 60)");
        $this->pdo->exec("INSERT INTO mp_cqs_questions VALUES (1, 1, '2 + 2 = ?', '4')");
        $this->pdo->exec("INSERT INTO mp_cqs_questions VALUES (2, 1, '3 * 5 = ?', '15')");
        $this->pdo->exec("INSERT INTO mp_cqs_questions VALUES (3, 1, '10 / 2 = ?', '5')");
        $this->pdo->exec("INSERT INTO mp_cqs_questions VALUES (4, 1, '7 - 3 = ?', '4')");
        $this->pdo->exec("INSERT INTO mp_cqs_questions VALUES (5, 1, '9 + 1 = ?', '10')");

        // Quiz 2: Science (pass = 50%)
        $this->pdo->exec("INSERT INTO mp_cqs_quizzes VALUES (2, 'Science Intro', 'science', 50)");
        $this->pdo->exec("INSERT INTO mp_cqs_questions VALUES (6, 2, 'Water formula?', 'H2O')");
        $this->pdo->exec("INSERT INTO mp_cqs_questions VALUES (7, 2, 'Speed of light unit?', 'c')");
        $this->pdo->exec("INSERT INTO mp_cqs_questions VALUES (8, 2, 'Gravity force?', '9.8')");
        $this->pdo->exec("INSERT INTO mp_cqs_questions VALUES (9, 2, 'Atomic number of H?', '1')");

        // Alice: Math 4/5 correct (80%), Science 3/4 correct (75%)
        $this->pdo->exec("INSERT INTO mp_cqs_answers VALUES (1, 1, 1, 1, '4')");
        $this->pdo->exec("INSERT INTO mp_cqs_answers VALUES (2, 1, 2, 1, '15')");
        $this->pdo->exec("INSERT INTO mp_cqs_answers VALUES (3, 1, 3, 1, '5')");
        $this->pdo->exec("INSERT INTO mp_cqs_answers VALUES (4, 1, 4, 1, '3')");   // wrong
        $this->pdo->exec("INSERT INTO mp_cqs_answers VALUES (5, 1, 5, 1, '10')");
        $this->pdo->exec("INSERT INTO mp_cqs_answers VALUES (6, 1, 6, 2, 'H2O')");
        $this->pdo->exec("INSERT INTO mp_cqs_answers VALUES (7, 1, 7, 2, 'c')");
        $this->pdo->exec("INSERT INTO mp_cqs_answers VALUES (8, 1, 8, 2, '10')");  // wrong
        $this->pdo->exec("INSERT INTO mp_cqs_answers VALUES (9, 1, 9, 2, '1')");

        // Bob: Math 2/5 correct (40%), Science 4/4 correct (100%)
        $this->pdo->exec("INSERT INTO mp_cqs_answers VALUES (10, 2, 1, 1, '4')");
        $this->pdo->exec("INSERT INTO mp_cqs_answers VALUES (11, 2, 2, 1, '12')");  // wrong
        $this->pdo->exec("INSERT INTO mp_cqs_answers VALUES (12, 2, 3, 1, '6')");   // wrong
        $this->pdo->exec("INSERT INTO mp_cqs_answers VALUES (13, 2, 4, 1, '4')");
        $this->pdo->exec("INSERT INTO mp_cqs_answers VALUES (14, 2, 5, 1, '11')");  // wrong
        $this->pdo->exec("INSERT INTO mp_cqs_answers VALUES (15, 2, 6, 2, 'H2O')");
        $this->pdo->exec("INSERT INTO mp_cqs_answers VALUES (16, 2, 7, 2, 'c')");
        $this->pdo->exec("INSERT INTO mp_cqs_answers VALUES (17, 2, 8, 2, '9.8')");
        $this->pdo->exec("INSERT INTO mp_cqs_answers VALUES (18, 2, 9, 2, '1')");

        // Carol: Math 5/5 correct (100%), has NOT taken Science
        $this->pdo->exec("INSERT INTO mp_cqs_answers VALUES (19, 3, 1, 1, '4')");
        $this->pdo->exec("INSERT INTO mp_cqs_answers VALUES (20, 3, 2, 1, '15')");
        $this->pdo->exec("INSERT INTO mp_cqs_answers VALUES (21, 3, 3, 1, '5')");
        $this->pdo->exec("INSERT INTO mp_cqs_answers VALUES (22, 3, 4, 1, '4')");
        $this->pdo->exec("INSERT INTO mp_cqs_answers VALUES (23, 3, 5, 1, '10')");

        // Dave: has NOT taken any quiz
    }

    /**
     * Multi-table JOIN with CASE to compare given_answer to correct_answer,
     * SUM correct / COUNT total for score percentage, per student per quiz.
     */
    public function testScorePerStudentPerQuiz(): void
    {
        $rows = $this->ztdQuery(
            "SELECT s.name, q.title,
                    SUM(CASE WHEN a.given_answer = qu.correct_answer THEN 1 ELSE 0 END) AS correct,
                    COUNT(a.id) AS total,
                    ROUND(SUM(CASE WHEN a.given_answer = qu.correct_answer THEN 1 ELSE 0 END) * 100.0 / COUNT(a.id)) AS score_pct
             FROM mp_cqs_answers a
             JOIN mp_cqs_students s ON a.student_id = s.id
             JOIN mp_cqs_questions qu ON a.question_id = qu.id
             JOIN mp_cqs_quizzes q ON a.quiz_id = q.id
             GROUP BY s.id, s.name, q.id, q.title
             ORDER BY s.name, q.title"
        );

        $this->assertCount(5, $rows);

        // Alice - Math: 4/5 = 80%
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Math Basics', $rows[0]['title']);
        $this->assertEquals(4, (int) $rows[0]['correct']);
        $this->assertEquals(5, (int) $rows[0]['total']);
        $this->assertEquals(80, (int) $rows[0]['score_pct']);

        // Alice - Science: 3/4 = 75%
        $this->assertSame('Alice', $rows[1]['name']);
        $this->assertSame('Science Intro', $rows[1]['title']);
        $this->assertEquals(3, (int) $rows[1]['correct']);
        $this->assertEquals(75, (int) $rows[1]['score_pct']);

        // Bob - Math: 2/5 = 40%
        $this->assertSame('Bob', $rows[2]['name']);
        $this->assertSame('Math Basics', $rows[2]['title']);
        $this->assertEquals(2, (int) $rows[2]['correct']);
        $this->assertEquals(40, (int) $rows[2]['score_pct']);

        // Bob - Science: 4/4 = 100%
        $this->assertSame('Bob', $rows[3]['name']);
        $this->assertSame('Science Intro', $rows[3]['title']);
        $this->assertEquals(4, (int) $rows[3]['correct']);
        $this->assertEquals(100, (int) $rows[3]['score_pct']);

        // Carol - Math: 5/5 = 100%
        $this->assertSame('Carol', $rows[4]['name']);
        $this->assertSame('Math Basics', $rows[4]['title']);
        $this->assertEquals(5, (int) $rows[4]['correct']);
        $this->assertEquals(100, (int) $rows[4]['score_pct']);
    }

    /**
     * CASE + HAVING: identify students who failed a quiz (score < passing_score).
     * Bob failed Math (40% < 60%).
     */
    public function testFailingStudents(): void
    {
        $rows = $this->ztdQuery(
            "SELECT s.name, q.title, q.passing_score,
                    ROUND(SUM(CASE WHEN a.given_answer = qu.correct_answer THEN 1 ELSE 0 END) * 100.0 / COUNT(a.id)) AS score_pct
             FROM mp_cqs_answers a
             JOIN mp_cqs_students s ON a.student_id = s.id
             JOIN mp_cqs_questions qu ON a.question_id = qu.id
             JOIN mp_cqs_quizzes q ON a.quiz_id = q.id
             GROUP BY s.id, s.name, q.id, q.title, q.passing_score
             HAVING ROUND(SUM(CASE WHEN a.given_answer = qu.correct_answer THEN 1 ELSE 0 END) * 100.0 / COUNT(a.id)) < q.passing_score
             ORDER BY s.name"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Bob', $rows[0]['name']);
        $this->assertSame('Math Basics', $rows[0]['title']);
        $this->assertEquals(40, (int) $rows[0]['score_pct']);
        $this->assertEquals(60, (int) $rows[0]['passing_score']);
    }

    /**
     * NOT EXISTS: find students who have not submitted a specific quiz.
     * Carol and Dave have not taken Science; Dave has not taken Math.
     */
    public function testStudentsWhoMissedQuiz(): void
    {
        // Students who haven't taken Science (quiz_id = 2)
        $rows = $this->ztdQuery(
            "SELECT s.name
             FROM mp_cqs_students s
             WHERE NOT EXISTS (
                 SELECT 1 FROM mp_cqs_answers a
                 WHERE a.student_id = s.id AND a.quiz_id = 2
             )
             ORDER BY s.name"
        );

        $this->assertCount(2, $rows);
        $this->assertSame('Carol', $rows[0]['name']);
        $this->assertSame('Dave', $rows[1]['name']);
    }

    /**
     * Aggregate class average per quiz using COUNT DISTINCT for student count.
     * Math: (80+40+100)/3 = 73.33 => 73. Science: (75+100)/2 = 87.5 => 88.
     */
    public function testClassAveragePerQuiz(): void
    {
        $rows = $this->ztdQuery(
            "SELECT q.title,
                    COUNT(DISTINCT a.student_id) AS student_count,
                    ROUND(AVG(sub.score_pct)) AS class_avg
             FROM mp_cqs_quizzes q
             JOIN mp_cqs_answers a ON a.quiz_id = q.id
             JOIN (
                 SELECT a2.student_id, a2.quiz_id,
                        SUM(CASE WHEN a2.given_answer = qu2.correct_answer THEN 1 ELSE 0 END) * 100.0 / COUNT(a2.id) AS score_pct
                 FROM mp_cqs_answers a2
                 JOIN mp_cqs_questions qu2 ON a2.question_id = qu2.id
                 GROUP BY a2.student_id, a2.quiz_id
             ) sub ON sub.student_id = a.student_id AND sub.quiz_id = a.quiz_id
             GROUP BY q.id, q.title
             ORDER BY q.title"
        );

        $this->assertCount(2, $rows);

        // Math: 3 students, avg ≈ 73
        $this->assertSame('Math Basics', $rows[0]['title']);
        $this->assertEquals(3, (int) $rows[0]['student_count']);
        $this->assertEquals(73, (int) $rows[0]['class_avg']);

        // Science: 2 students, avg ≈ 88
        $this->assertSame('Science Intro', $rows[1]['title']);
        $this->assertEquals(2, (int) $rows[1]['student_count']);
        $this->assertEquals(88, (int) $rows[1]['class_avg']);
    }

    /**
     * Per-question difficulty: percentage of students who got each question right.
     * Questions with low success rates indicate harder questions.
     */
    public function testQuestionDifficulty(): void
    {
        $rows = $this->ztdQuery(
            "SELECT qu.id AS question_id, qu.question_text,
                    COUNT(a.id) AS attempts,
                    SUM(CASE WHEN a.given_answer = qu.correct_answer THEN 1 ELSE 0 END) AS correct,
                    ROUND(SUM(CASE WHEN a.given_answer = qu.correct_answer THEN 1 ELSE 0 END) * 100.0 / COUNT(a.id)) AS success_rate
             FROM mp_cqs_questions qu
             JOIN mp_cqs_answers a ON a.question_id = qu.id
             GROUP BY qu.id, qu.question_text
             ORDER BY success_rate ASC, qu.id ASC"
        );

        $this->assertCount(9, $rows);

        // Hardest question: question 5 "9 + 1 = ?" — 2/3 got it right (Carol+Alice), Bob wrong => 67%
        // Actually let me recalculate: q5 answers: Alice=10(correct), Bob=11(wrong), Carol=10(correct) => 2/3 = 67%
        // q3 "10/2=?": Alice=5(c), Bob=6(w), Carol=5(c) => 2/3 = 67%
        // q2 "3*5=?": Alice=15(c), Bob=12(w), Carol=15(c) => 2/3 = 67%
        // q4 "7-3=?": Alice=3(w), Bob=4(c), Carol=4(c) => 2/3 = 67%
        // q1 "2+2=?": Alice=4(c), Bob=4(c), Carol=4(c) => 3/3 = 100%
        // All math questions besides q1 have 67% success rate
        // Science: q6(H2O): 2/2=100%, q7(c): 2/2=100%, q8(9.8): Alice wrong, Bob right => 1/2=50%, q9(1): 2/2=100%

        // Hardest: q8 at 50%
        $this->assertEquals(8, (int) $rows[0]['question_id']);
        $this->assertEquals(50, (int) $rows[0]['success_rate']);
    }

    /**
     * Prepared statement: lookup a specific student's results for a given quiz.
     */
    public function testPreparedStudentQuizResults(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT qu.question_text, a.given_answer, qu.correct_answer,
                    CASE WHEN a.given_answer = qu.correct_answer THEN 'correct' ELSE 'wrong' END AS result
             FROM mp_cqs_answers a
             JOIN mp_cqs_questions qu ON a.question_id = qu.id
             WHERE a.student_id = ? AND a.quiz_id = ?
             ORDER BY qu.id",
            [2, 1]  // Bob's Math results
        );

        $this->assertCount(5, $rows);

        // Bob got q1 correct, q2 wrong, q3 wrong, q4 correct, q5 wrong
        $this->assertSame('correct', $rows[0]['result']);
        $this->assertSame('wrong', $rows[1]['result']);
        $this->assertSame('wrong', $rows[2]['result']);
        $this->assertSame('correct', $rows[3]['result']);
        $this->assertSame('wrong', $rows[4]['result']);
    }

    /**
     * Physical isolation: answers inserted through ZTD stay in shadow store.
     */
    public function testPhysicalIsolation(): void
    {
        // Submit a late answer through ZTD
        $this->pdo->exec("INSERT INTO mp_cqs_answers VALUES (24, 4, 1, 1, '4')");

        // ZTD sees all answers
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mp_cqs_answers");
        $this->assertEquals(24, (int) $rows[0]['cnt']);

        // Physical table untouched
        $this->pdo->disableZtd();
        $result = $this->pdo->query('SELECT COUNT(*) AS cnt FROM mp_cqs_answers');
        $this->assertEquals(0, (int) $result->fetchAll(PDO::FETCH_ASSOC)[0]['cnt']);
    }
}
