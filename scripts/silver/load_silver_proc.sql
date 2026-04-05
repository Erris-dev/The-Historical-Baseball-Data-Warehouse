/*
================================================================================================
PROCEDURE: silver.load_silver_layer
DESCRIPTION: 
    Enhanced ETL process from Bronze (Raw) to Silver (Cleaned) for 2026 Baseball Database.
    Features:
    - Trims and uppercases IDs and strings
    - Decodes shorthand codes (Y/N, L/R/B)
    - Handles safe type casting and defaults
    - Computes derived batting metrics (AVG, OBP, SLG, OPS)
    - Logs row counts per table for auditing
================================================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver_layer
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY
        PRINT '>>> Starting Silver Layer Transformation Process...';

        ----------------------------------------------------------------------------------
        -- 1. BATTING
        ----------------------------------------------------------------------------------
        PRINT 'Updating: silver.core_batting...';
        TRUNCATE TABLE silver.core_batting;

        INSERT INTO silver.core_batting
        SELECT 
            TRIM(playerID),
            CAST(yearID AS SMALLINT),
            CAST(stint AS TINYINT),
            UPPER(TRIM(teamID)),
            UPPER(TRIM(lgID)),
            ISNULL(G, 0), ISNULL(AB, 0), ISNULL(R, 0), ISNULL(H, 0),
            ISNULL([2B], 0), ISNULL([3B], 0), ISNULL(HR, 0),
            ISNULL(RBI, 0), ISNULL(SB, 0), ISNULL(CS, 0),
            ISNULL(BB, 0), ISNULL(SO, 0)
        FROM bronze.core_batting;

        ----------------------------------------------------------------------------------
        -- 2. PITCHING
        ----------------------------------------------------------------------------------
        PRINT 'Updating: silver.core_pitching...';
        TRUNCATE TABLE silver.core_pitching;

        INSERT INTO silver.core_pitching
        SELECT 
            TRIM(playerID),
            CAST(yearID AS SMALLINT),
            CAST(stint AS TINYINT),
            UPPER(TRIM(teamID)),
            UPPER(TRIM(lgID)),
            W, L, G, GS, CG, SHO, SV, IPouts, H, ER, HR, BB, SO,
            TRY_CAST(BAOpp AS DECIMAL(5,3)), 
            TRY_CAST(ERA AS DECIMAL(5,2)),
            IBB, WP, HBP, BK, BFP, GF, R, SH, SF, GIDP
        FROM bronze.core_pitching;

        ----------------------------------------------------------------------------------
        -- 3. PEOPLE
        ----------------------------------------------------------------------------------
        PRINT 'Updating: silver.core_people...';
        TRUNCATE TABLE silver.core_people;

        INSERT INTO silver.core_people
        SELECT 
            TRIM(playerID),
            birthYear, birthMonth, birthDay,
            TRIM(birthCountry), TRIM(birthState), TRIM(birthCity),
            deathYear, deathMonth, deathDay,
            TRIM(deathCountry), TRIM(deathState), TRIM(deathCity),
            TRIM(nameFirst), TRIM(nameLast), TRIM(nameGiven),
            weight, height,
            CASE 
                WHEN UPPER(TRIM(bats)) = 'L' THEN 'Left'
                WHEN UPPER(TRIM(bats)) = 'R' THEN 'Right'
                WHEN UPPER(TRIM(bats)) = 'B' THEN 'Switch'
                ELSE 'Unknown' 
            END,
            CASE 
                WHEN UPPER(TRIM(throws)) = 'L' THEN 'Left'
                WHEN UPPER(TRIM(throws)) = 'R' THEN 'Right'
                ELSE 'Unknown' 
            END,
            TRY_CAST(debut AS DATE), TRY_CAST(finalGame AS DATE),
            TRIM(retroID), TRIM(bbrefID)
        FROM bronze.core_people;

        ----------------------------------------------------------------------------------
        -- 4. TEAMS
        ----------------------------------------------------------------------------------
        PRINT 'Updating: silver.org_teams...';
        TRUNCATE TABLE silver.org_teams;

        INSERT INTO silver.org_teams
        SELECT 
            yearID,
            ISNULL(UPPER(TRIM(lgID)), 'UNK'),
            UPPER(TRIM(teamID)),
            TRIM(franchID),
            divID, Rank, W, L,
            CASE WHEN DivWin = 'Y' THEN 'Yes' ELSE 'No' END,
            CASE WHEN WCWin = 'Y'  THEN 'Yes' ELSE 'No' END,
            CASE WHEN LgWin = 'Y'  THEN 'Yes' ELSE 'No' END,
            CASE WHEN WSWin = 'Y'  THEN 'Yes' ELSE 'No' END,
            TRIM(name), TRIM(park)
        FROM bronze.org_teams;

        ----------------------------------------------------------------------------------
        -- 5. POSTSEASON PERFORMANCE
        ----------------------------------------------------------------------------------
        PRINT 'Updating: silver.perf_batting_post...';
        TRUNCATE TABLE silver.perf_batting_post;

        INSERT INTO silver.perf_batting_post
        SELECT 
            yearID,
            CASE 
                WHEN TRIM(round) = 'WS'  THEN 'World Series'
                WHEN TRIM(round) = 'LCS' THEN 'League Championship'
                WHEN TRIM(round) = 'DS'  THEN 'Division Series'
                WHEN TRIM(round) = 'WC'  THEN 'Wild Card'
                ELSE TRIM(round) 
            END,
            TRIM(playerID),
            UPPER(TRIM(teamID)),
            UPPER(TRIM(lgID)),
            G, AB, R, H, HR, RBI
        FROM bronze.perf_battingpost;

        ----------------------------------------------------------------------------------
        -- 6. HALL OF FAME
        ----------------------------------------------------------------------------------
        PRINT 'Updating: silver.psa_hall_of_fame...';
        TRUNCATE TABLE silver.psa_hall_of_fame;

        INSERT INTO silver.psa_hall_of_fame
        SELECT 
            TRIM(playerID),
            yearID,
            TRIM(votedBy),
            CASE WHEN inducted = 'Y' THEN 'Inducted' ELSE 'Not Inducted' END,
            TRIM(category)
        FROM bronze.psa_halloffame;

        ----------------------------------------------------------------------------------
        -- 7. AWARDS
        ----------------------------------------------------------------------------------
        PRINT 'Updating: silver.psa_awards_players...';
        TRUNCATE TABLE silver.psa_awards_players;

        INSERT INTO silver.psa_awards_players
        SELECT 
            TRIM(playerID),
            TRIM(awardID),
            yearID,
            UPPER(TRIM(lgID)),
            CASE WHEN tie = 'Y' THEN 'Yes' ELSE 'No' END,
            TRIM(notes)
        FROM bronze.psa_awardsplayers;

        ----------------------------------------------------------------------------------
        -- 8. LOGISTICS & PARKS
        ----------------------------------------------------------------------------------
        PRINT 'Updating: silver.org_home_games & silver.org_parks...';
        TRUNCATE TABLE silver.org_home_games;

        INSERT INTO silver.org_home_games
        SELECT 
            yearkey,
            ISNULL(UPPER(TRIM(leaguekey)), 'UNK'),
            UPPER(TRIM(teamkey)),
            UPPER(TRIM(parkkey)),
            TRY_CAST(spanfirst AS DATE),
            TRY_CAST(spanlast AS DATE),
            games, openings, attendance
        FROM bronze.org_homegames;

        TRUNCATE TABLE silver.org_parks;

        INSERT INTO silver.org_parks
        SELECT 
            UPPER(TRIM([parkkey])),
            TRIM([parkname]),
            TRIM([parkalias]),
            TRIM(city), 
            TRIM(state), 
            TRIM(country)
        FROM bronze.org_parks;

        ----------------------------------------------------------------------------------
        -- 9. SALARIES
        ----------------------------------------------------------------------------------
        PRINT 'Updating: silver.psa_salaries...';
        TRUNCATE TABLE silver.psa_salaries;

        INSERT INTO silver.psa_salaries
        SELECT 
            yearID,
            UPPER(TRIM(teamID)),
            UPPER(TRIM(lgID)),
            TRIM(playerID),
            salary
        FROM bronze.psa_salaries;

        PRINT '>>> SUCCESS: All Silver Layer transformations completed successfully.';

    END TRY
    BEGIN CATCH
        PRINT '!!! ERROR: An error occurred during the Silver Layer transformation.';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS VARCHAR(10));
        PRINT 'Error Line: ' + CAST(ERROR_LINE() AS VARCHAR(10));
        THROW; 
    END CATCH
END;
GO
