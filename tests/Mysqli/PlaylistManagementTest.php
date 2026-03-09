<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests playlist management with position reordering, play count tracking,
 * genre distribution, and most-played ranking through ZTD shadow store (MySQLi).
 * Covers UPDATE arithmetic, SUM CASE, GROUP BY + JOIN, and physical isolation.
 * @spec SPEC-10.2.140
 */
class PlaylistManagementTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_pm_playlists (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255),
                created_by VARCHAR(255)
            )',
            'CREATE TABLE mi_pm_songs (
                id INT AUTO_INCREMENT PRIMARY KEY,
                title VARCHAR(255),
                artist VARCHAR(255),
                genre VARCHAR(50),
                duration_sec INT
            )',
            'CREATE TABLE mi_pm_playlist_songs (
                id INT AUTO_INCREMENT PRIMARY KEY,
                playlist_id INT,
                song_id INT,
                position INT,
                play_count INT
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_pm_playlist_songs', 'mi_pm_songs', 'mi_pm_playlists'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // 2 playlists
        $this->mysqli->query("INSERT INTO mi_pm_playlists VALUES (1, 'Road Trip Mix', 'alice')");
        $this->mysqli->query("INSERT INTO mi_pm_playlists VALUES (2, 'Workout Beats', 'bob')");

        // 6 songs
        $this->mysqli->query("INSERT INTO mi_pm_songs VALUES (1, 'Highway Star', 'Deep Purple', 'Rock', 367)");
        $this->mysqli->query("INSERT INTO mi_pm_songs VALUES (2, 'Bohemian Rhapsody', 'Queen', 'Rock', 354)");
        $this->mysqli->query("INSERT INTO mi_pm_songs VALUES (3, 'Billie Jean', 'Michael Jackson', 'Pop', 294)");
        $this->mysqli->query("INSERT INTO mi_pm_songs VALUES (4, 'Lose Yourself', 'Eminem', 'HipHop', 326)");
        $this->mysqli->query("INSERT INTO mi_pm_songs VALUES (5, 'Blinding Lights', 'The Weeknd', 'Pop', 200)");
        $this->mysqli->query("INSERT INTO mi_pm_songs VALUES (6, 'Thunderstruck', 'ACDC', 'Rock', 292)");

        // playlist_songs: playlist 1 has 4 songs, playlist 2 has 3 songs
        $this->mysqli->query("INSERT INTO mi_pm_playlist_songs VALUES (1, 1, 1, 1, 15)");  // Road Trip: Highway Star pos 1
        $this->mysqli->query("INSERT INTO mi_pm_playlist_songs VALUES (2, 1, 2, 2, 22)");  // Road Trip: Bohemian Rhapsody pos 2
        $this->mysqli->query("INSERT INTO mi_pm_playlist_songs VALUES (3, 1, 3, 3, 8)");   // Road Trip: Billie Jean pos 3
        $this->mysqli->query("INSERT INTO mi_pm_playlist_songs VALUES (4, 1, 5, 4, 30)");  // Road Trip: Blinding Lights pos 4
        $this->mysqli->query("INSERT INTO mi_pm_playlist_songs VALUES (5, 2, 4, 1, 45)");  // Workout: Lose Yourself pos 1
        $this->mysqli->query("INSERT INTO mi_pm_playlist_songs VALUES (6, 2, 6, 2, 38)");  // Workout: Thunderstruck pos 2
        $this->mysqli->query("INSERT INTO mi_pm_playlist_songs VALUES (7, 2, 1, 3, 20)");  // Workout: Highway Star pos 3
    }

    /**
     * JOIN playlists + playlist_songs + songs. Show playlist name, song title, artist, position.
     * ORDER BY playlist name, position.
     * Road Trip has 4 songs: Highway Star, Bohemian Rhapsody, Billie Jean, Blinding Lights.
     * Workout has 3: Lose Yourself, Thunderstruck, Highway Star.
     */
    public function testPlaylistContents(): void
    {
        $rows = $this->ztdQuery(
            "SELECT p.name AS playlist_name, s.title, s.artist, ps.position
             FROM mi_pm_playlist_songs ps
             JOIN mi_pm_playlists p ON p.id = ps.playlist_id
             JOIN mi_pm_songs s ON s.id = ps.song_id
             ORDER BY p.name, ps.position"
        );

        $this->assertCount(7, $rows);

        // Road Trip Mix (4 songs)
        $this->assertSame('Road Trip Mix', $rows[0]['playlist_name']);
        $this->assertSame('Highway Star', $rows[0]['title']);
        $this->assertEquals(1, (int) $rows[0]['position']);

        $this->assertSame('Road Trip Mix', $rows[1]['playlist_name']);
        $this->assertSame('Bohemian Rhapsody', $rows[1]['title']);
        $this->assertEquals(2, (int) $rows[1]['position']);

        $this->assertSame('Road Trip Mix', $rows[2]['playlist_name']);
        $this->assertSame('Billie Jean', $rows[2]['title']);
        $this->assertEquals(3, (int) $rows[2]['position']);

        $this->assertSame('Road Trip Mix', $rows[3]['playlist_name']);
        $this->assertSame('Blinding Lights', $rows[3]['title']);
        $this->assertEquals(4, (int) $rows[3]['position']);

        // Workout Beats (3 songs)
        $this->assertSame('Workout Beats', $rows[4]['playlist_name']);
        $this->assertSame('Lose Yourself', $rows[4]['title']);
        $this->assertEquals(1, (int) $rows[4]['position']);

        $this->assertSame('Workout Beats', $rows[5]['playlist_name']);
        $this->assertSame('Thunderstruck', $rows[5]['title']);
        $this->assertEquals(2, (int) $rows[5]['position']);

        $this->assertSame('Workout Beats', $rows[6]['playlist_name']);
        $this->assertSame('Highway Star', $rows[6]['title']);
        $this->assertEquals(3, (int) $rows[6]['position']);
    }

    /**
     * Move Blinding Lights from position 4 to position 2 in playlist 1.
     * First shift positions 2,3 down (+1), then set Blinding Lights to position 2.
     * New order: Highway Star(1), Blinding Lights(2), Bohemian Rhapsody(3), Billie Jean(4).
     */
    public function testMovePositionUp(): void
    {
        // Shift positions 2 and 3 down by 1
        $this->mysqli->query(
            "UPDATE mi_pm_playlist_songs SET position = position + 1
             WHERE playlist_id = 1 AND position >= 2 AND position < 4"
        );

        // Move Blinding Lights (id=4) to position 2
        $this->mysqli->query(
            "UPDATE mi_pm_playlist_songs SET position = 2 WHERE id = 4"
        );

        // Verify new order
        $rows = $this->ztdQuery(
            "SELECT s.title, ps.position
             FROM mi_pm_playlist_songs ps
             JOIN mi_pm_songs s ON s.id = ps.song_id
             WHERE ps.playlist_id = 1
             ORDER BY ps.position"
        );

        $this->assertCount(4, $rows);

        $this->assertSame('Highway Star', $rows[0]['title']);
        $this->assertEquals(1, (int) $rows[0]['position']);

        $this->assertSame('Blinding Lights', $rows[1]['title']);
        $this->assertEquals(2, (int) $rows[1]['position']);

        $this->assertSame('Bohemian Rhapsody', $rows[2]['title']);
        $this->assertEquals(3, (int) $rows[2]['position']);

        $this->assertSame('Billie Jean', $rows[3]['title']);
        $this->assertEquals(4, (int) $rows[3]['position']);
    }

    /**
     * SUM CASE per genre across all playlist_songs entries.
     * Rock = 4 (Highway Star x2 + Bohemian Rhapsody + Thunderstruck),
     * Pop = 2 (Billie Jean + Blinding Lights), HipHop = 1 (Lose Yourself).
     * GROUP BY genre ORDER BY cnt DESC.
     */
    public function testGenreDistribution(): void
    {
        $rows = $this->ztdQuery(
            "SELECT s.genre,
                    SUM(CASE WHEN s.genre = s.genre THEN 1 ELSE 0 END) AS cnt
             FROM mi_pm_playlist_songs ps
             JOIN mi_pm_songs s ON s.id = ps.song_id
             GROUP BY s.genre
             ORDER BY cnt DESC"
        );

        $this->assertCount(3, $rows);

        $this->assertSame('Rock', $rows[0]['genre']);
        $this->assertEquals(4, (int) $rows[0]['cnt']);

        $this->assertSame('Pop', $rows[1]['genre']);
        $this->assertEquals(2, (int) $rows[1]['cnt']);

        $this->assertSame('HipHop', $rows[2]['genre']);
        $this->assertEquals(1, (int) $rows[2]['cnt']);
    }

    /**
     * JOIN playlist_songs + songs, SUM play_count per song across playlists.
     * ORDER BY total_plays DESC, LIMIT 3.
     * Lose Yourself: 45, Thunderstruck: 38, Highway Star: 15+20=35.
     */
    public function testMostPlayedSongs(): void
    {
        $rows = $this->ztdQuery(
            "SELECT s.title, s.artist, SUM(ps.play_count) AS total_plays
             FROM mi_pm_playlist_songs ps
             JOIN mi_pm_songs s ON s.id = ps.song_id
             GROUP BY ps.song_id, s.title, s.artist
             ORDER BY total_plays DESC
             LIMIT 3"
        );

        $this->assertCount(3, $rows);

        $this->assertSame('Lose Yourself', $rows[0]['title']);
        $this->assertSame('Eminem', $rows[0]['artist']);
        $this->assertEquals(45, (int) $rows[0]['total_plays']);

        $this->assertSame('Thunderstruck', $rows[1]['title']);
        $this->assertSame('ACDC', $rows[1]['artist']);
        $this->assertEquals(38, (int) $rows[1]['total_plays']);

        $this->assertSame('Highway Star', $rows[2]['title']);
        $this->assertSame('Deep Purple', $rows[2]['artist']);
        $this->assertEquals(35, (int) $rows[2]['total_plays']);
    }

    /**
     * SUM duration_sec per playlist via JOIN.
     * Road Trip: 367+354+294+200 = 1215. Workout: 326+292+367 = 985.
     * ORDER BY playlist name.
     */
    public function testPlaylistDuration(): void
    {
        $rows = $this->ztdQuery(
            "SELECT p.name AS playlist_name, SUM(s.duration_sec) AS total_duration
             FROM mi_pm_playlist_songs ps
             JOIN mi_pm_playlists p ON p.id = ps.playlist_id
             JOIN mi_pm_songs s ON s.id = ps.song_id
             GROUP BY ps.playlist_id, p.name
             ORDER BY p.name"
        );

        $this->assertCount(2, $rows);

        $this->assertSame('Road Trip Mix', $rows[0]['playlist_name']);
        $this->assertEquals(1215, (int) $rows[0]['total_duration']);

        $this->assertSame('Workout Beats', $rows[1]['playlist_name']);
        $this->assertEquals(985, (int) $rows[1]['total_duration']);
    }

    /**
     * Physical isolation: shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->query("INSERT INTO mi_pm_playlist_songs VALUES (8, 2, 3, 4, 5)");

        // Visible through ZTD
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_pm_playlist_songs");
        $this->assertSame(8, (int) $rows[0]['cnt']);

        // Physical table untouched
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_pm_playlist_songs');
        $this->assertEquals(0, (int) $result->fetch_assoc()['cnt']);
    }
}
