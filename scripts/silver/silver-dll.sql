/*
================================================================================================
PROJECT: Baseball Data Warehouse 2026 (Silver Layer)
DESCRIPTION: 
    This script defines the Silver Layer schema for the Lahman Baseball Database. 
    It features:
    - "Decoded" single-letter codes (e.g., bats/throws, award ties, playoff rounds).
    - Descriptive column names for Batting and Fielding (e.g., 'doubles' instead of 'H2B').
    - Source-prefixed table names (core, org, psa, perf) for easy lineage tracking.
    - Re-runnable DDL logic with IF OBJECT_ID checks.
    - Primary Keys to ensure data integrity and prevent duplicates.

DATA ARCHITECT: Gemini
TARGET ENVIRONMENT: SQL Server
================================================================================================
*/

-- 1. BATTING (Source: core)
IF OBJECT_ID ('silver.core_batting', 'U') IS NOT NULL DROP TABLE silver.core_batting;
GO
CREATE TABLE silver.core_batting (
    player_id       VARCHAR(10) NOT NULL,
    year_id         SMALLINT NOT NULL,
    stint           TINYINT NOT NULL,
    team_id         CHAR(3),
    league_id       CHAR(3),
    games           INT DEFAULT 0,
    at_bats         INT DEFAULT 0,
    runs            INT DEFAULT 0,
    hits            INT DEFAULT 0,
    doubles         INT DEFAULT 0, -- Decoded from H2B
    triples         INT DEFAULT 0, -- Decoded from H3B
    home_runs       INT DEFAULT 0,
    runs_batted_in  INT DEFAULT 0, -- Decoded from RBI
    stolen_bases    INT DEFAULT 0,
    caught_stealing INT DEFAULT 0,
    base_on_balls   INT DEFAULT 0, -- Decoded from BB
    strikeouts      INT DEFAULT 0, -- Decoded from SO
    CONSTRAINT PK_core_batting PRIMARY KEY (player_id, year_id, stint)
);
PRINT 'Table silver.core_batting created successfully.';
GO

-- 2. FIELDING (Source: core)
IF OBJECT_ID ('silver.core_fielding', 'U') IS NOT NULL DROP TABLE silver.core_fielding;
GO
CREATE TABLE silver.core_fielding (
    player_id       VARCHAR(10) NOT NULL,
    year_id         SMALLINT NOT NULL,
    stint           TINYINT NOT NULL,
    team_id         CHAR(3),
    league_id       CHAR(3),
    position        VARCHAR(2),    -- Decoded from pos
    games           INT DEFAULT 0,
    games_started   INT DEFAULT 0,
    innings_outs    INT DEFAULT 0,
    putouts         INT DEFAULT 0, -- Decoded from PO
    assists         INT DEFAULT 0, -- Decoded from A
    errors          INT DEFAULT 0, -- Decoded from E
    double_plays    INT DEFAULT 0, -- Decoded from DP
    passed_balls    INT DEFAULT 0, -- Decoded from PB
    wild_pitches    INT DEFAULT 0, -- Decoded from WP
    stolen_bases    INT DEFAULT 0,
    caught_stealing INT DEFAULT 0,
    zone_rating     FLOAT,         -- Decoded from ZR
    CONSTRAINT PK_core_fielding PRIMARY KEY (player_id, year_id, stint, position)
);
PRINT 'Table silver.core_fielding created successfully.';
GO

-- 3. PEOPLE (Source: core)
IF OBJECT_ID ('silver.core_people', 'U') IS NOT NULL DROP TABLE silver.core_people;
GO
CREATE TABLE silver.core_people (
    player_id     VARCHAR(10) NOT NULL,
    birth_year    SMALLINT,
    birth_month   TINYINT,
    birth_day     TINYINT,
    birth_country VARCHAR(50),
    birth_state   VARCHAR(50),
    birth_city    VARCHAR(50),
    death_year    SMALLINT,
    death_month   TINYINT,
    death_day     TINYINT,
    death_country VARCHAR(50),
    death_state   VARCHAR(50),
    death_city    VARCHAR(50),
    name_first    VARCHAR(50),
    name_last     VARCHAR(50),
    name_given    VARCHAR(255),
    weight        SMALLINT,
    height        SMALLINT,
    bats          VARCHAR(10), -- Will be 'Left', 'Right', 'Switch'
    throws        VARCHAR(10), -- Will be 'Left', 'Right'
    debut         DATE,
    final_game    DATE,
    retro_id      VARCHAR(10),
    bbref_id      VARCHAR(10),
    CONSTRAINT PK_core_people PRIMARY KEY (player_id)
);
PRINT 'Table silver.core_people created successfully.';
GO

-- 4. PITCHING (Source: core)
IF OBJECT_ID ('silver.core_pitching', 'U') IS NOT NULL DROP TABLE silver.core_pitching;
GO
CREATE TABLE silver.core_pitching (
    player_id         VARCHAR(10) NOT NULL,
    year_id           SMALLINT NOT NULL,
    stint             TINYINT NOT NULL,
    team_id           CHAR(3),
    league_id         CHAR(3),
    wins              INT DEFAULT 0,
    losses            INT DEFAULT 0,
    games_played      INT DEFAULT 0,
    games_started     INT DEFAULT 0,
    complete_games    INT DEFAULT 0,
    shutouts          INT DEFAULT 0,
    saves             INT DEFAULT 0,
    outs_pitched      INT DEFAULT 0, 
    hits_allowed      INT DEFAULT 0,
    earned_runs       INT DEFAULT 0,
    home_runs_allowed INT DEFAULT 0,
    walks             INT DEFAULT 0,
    strikeouts        INT DEFAULT 0,
    opponent_avg      DECIMAL(5, 3),
    era               DECIMAL(5, 2),
    intentional_walks INT DEFAULT 0,
    wild_pitches      INT DEFAULT 0,
    hit_by_pitch      INT DEFAULT 0,
    balks             INT DEFAULT 0,
    batters_faced     INT DEFAULT 0,
    games_finished    INT DEFAULT 0,
    runs_allowed      INT DEFAULT 0,
    sac_hits          INT DEFAULT 0,
    sac_flies         INT DEFAULT 0,
    double_plays_induced INT DEFAULT 0,
    CONSTRAINT PK_core_pitching PRIMARY KEY (player_id, year_id, stint)
);
PRINT 'Table silver.core_pitching created successfully.';
GO

-- 5. HOME GAMES (Source: org)
IF OBJECT_ID ('silver.org_home_games', 'U') IS NOT NULL DROP TABLE silver.org_home_games;
GO
CREATE TABLE silver.org_home_games (
    year_id          SMALLINT NOT NULL,
    league_id        CHAR(3) NOT NULL,
    team_id          CHAR(3) NOT NULL,
    park_id          VARCHAR(10) NOT NULL,
    span_first       DATE,
    span_last        DATE,
    games_played     INT DEFAULT 0,
    openings         INT DEFAULT 0,
    attendance       INT DEFAULT 0,
    CONSTRAINT PK_org_home_games PRIMARY KEY (year_id, league_id, team_id, park_id)
);
PRINT 'Table silver.org_home_games created successfully.';
GO

-- 6. TEAMS (Source: org)
IF OBJECT_ID ('silver.org_teams', 'U') IS NOT NULL DROP TABLE silver.org_teams;
GO
CREATE TABLE silver.org_teams (
    year_id          SMALLINT NOT NULL,
    league_id        CHAR(3) NOT NULL,
    team_id          CHAR(3) NOT NULL,
    franchise_id     CHAR(3),
    division_id      CHAR(1),
    team_rank        TINYINT,
    wins             INT,
    losses           INT,
    division_winner  VARCHAR(20),     -- Will be 'Division Winner' or 'No'
    wild_card_winner VARCHAR(20),     -- Will be 'Wild Card Winner' or 'No'
    league_winner    VARCHAR(20),     -- Will be 'League Champion' or 'No'
    world_series_status VARCHAR(25),  -- Will be 'World Series Champion' or 'No'
    team_name        VARCHAR(50),
    park_name        VARCHAR(255),
    CONSTRAINT PK_org_teams PRIMARY KEY (year_id, team_id)
);
PRINT 'Table silver.org_teams created successfully.';
GO

-- 7. PARKS (Source: org)
IF OBJECT_ID ('silver.org_parks', 'U') IS NOT NULL DROP TABLE silver.org_parks;
GO
CREATE TABLE silver.org_parks (
    park_id    VARCHAR(10) NOT NULL,
    park_name  VARCHAR(255),
    park_alias VARCHAR(255),
    city       VARCHAR(50),
    state      VARCHAR(50),
    country    VARCHAR(50),
    CONSTRAINT PK_org_parks PRIMARY KEY (park_id)
);
PRINT 'Table silver.org_parks created successfully.';
GO

-- 8. AWARDS PLAYERS (Source: psa)
IF OBJECT_ID ('silver.psa_awards_players', 'U') IS NOT NULL DROP TABLE silver.psa_awards_players;
GO
CREATE TABLE silver.psa_awards_players (
    player_id    VARCHAR(10) NOT NULL,
    award_id     VARCHAR(50) NOT NULL,
    year_id      SMALLINT NOT NULL,
    league_id    CHAR(3) NOT NULL,
    is_tie       VARCHAR(3), -- Will be 'Yes' or 'No'
    notes        VARCHAR(100),
    CONSTRAINT PK_psa_awards PRIMARY KEY (player_id, award_id, year_id)
);
PRINT 'Table silver.psa_awards_players created successfully.';
GO

-- 9. BATTING POSTSEASON (Source: perf)
IF OBJECT_ID ('silver.perf_batting_post', 'U') IS NOT NULL DROP TABLE silver.perf_batting_post;
GO
CREATE TABLE silver.perf_batting_post (
    year_id       SMALLINT NOT NULL,
    playoff_round VARCHAR(30) NOT NULL, -- Will be 'World Series', 'Division Series', etc.
    player_id     VARCHAR(10) NOT NULL,
    team_id       CHAR(3),
    league_id     CHAR(3),
    games         INT DEFAULT 0, 
    at_bats       INT DEFAULT 0, 
    runs          INT DEFAULT 0, 
    hits          INT DEFAULT 0, 
    home_runs     INT DEFAULT 0, 
    rbi           INT DEFAULT 0,
    CONSTRAINT PK_perf_batting_post PRIMARY KEY (year_id, playoff_round, player_id)
);
PRINT 'Table silver.perf_batting_post created successfully.';
GO

-- 10. HALL OF FAME (Source: psa)
IF OBJECT_ID ('silver.psa_hall_of_fame', 'U') IS NOT NULL DROP TABLE silver.psa_hall_of_fame;
GO
CREATE TABLE silver.psa_hall_of_fame (
    player_id    VARCHAR(10) NOT NULL,
    year_id      SMALLINT NOT NULL,
    voted_by     VARCHAR(50), 
    status       VARCHAR(15), -- Will be 'Inducted' or 'Not Inducted'
    category     VARCHAR(20), 
    CONSTRAINT PK_psa_hof PRIMARY KEY (player_id, year_id, voted_by)
);
PRINT 'Table silver.psa_hall_of_fame created successfully.';
GO

-- 11. SALARIES (Source: psa)
IF OBJECT_ID ('silver.psa_salaries', 'U') IS NOT NULL DROP TABLE silver.psa_salaries;
GO
CREATE TABLE silver.psa_salaries (
    year_id   SMALLINT NOT NULL,
    team_id   CHAR(3) NOT NULL,
    league_id CHAR(3) NOT NULL,
    player_id VARCHAR(10) NOT NULL,
    salary    DECIMAL(18, 2),
    CONSTRAINT PK_psa_salaries PRIMARY KEY (year_id, team_id, player_id)
);
PRINT 'Table silver.psa_salaries created successfully.';
GO
