-- Highest Scorer
with PlayerGoals as (
select Stats.player_id, Players.player_name, sum(Stats.goals) as TotalGoals
from cleaned_player_stats as Stats join cleaned_players as Players
on Stats.player_id = Players.player_id
group by Stats.player_id, Players.player_name
)
select player_id, player_name, TotalGoals 
from PlayerGoals
where TotalGoals = (select max(TotalGoals) from PlayerGoals);


-- Top player contributors
WITH PlayerContributions AS (
    SELECT 
        Stats.player_id,
        Players.player_name,
        SUM(Stats.goals) + SUM(Stats.assists) AS TotalContributions
    FROM cleaned_player_stats AS Stats
    JOIN cleaned_players AS Players
        ON Stats.player_id = Players.player_id
    GROUP BY Stats.player_id, Players.player_name
)
SELECT player_id, player_name, TotalContributions
FROM PlayerContributions
WHERE TotalContributions = (
    SELECT MAX(TotalContributions) 
    FROM PlayerContributions
)

-- Most minutes played
WITH PlayerMinutes AS (
    SELECT Stats.player_id, Players.player_name, SUM(Stats.mins_played) AS TotalMinutes
    FROM cleaned_player_stats AS Stats JOIN cleaned_players AS Players
    ON Stats.player_id = Players.player_id
    GROUP BY Stats.player_id, Players.player_name
),
MaxMinutes AS (
    SELECT MAX(TotalMinutes) AS MaxMins
    FROM PlayerMinutes
)
SELECT PM.player_name, PM.TotalMinutes
FROM PlayerMinutes AS PM
JOIN MaxMinutes AS MM
ON PM.TotalMinutes = MM.MaxMins;


-- Player who played over 300 minutes
WITH TotalMins AS (
    SELECT P.player_id, P.player_name, COALESCE(SUM(S.mins_played), 0) AS Minutes
    FROM cleaned_players AS P LEFT JOIN cleaned_player_stats AS S
    ON P.player_id = S.player_id
    GROUP BY P.player_id, P.player_name
),
Counts AS (
    SELECT COUNT(CASE WHEN Minutes > 300 THEN 1 END) AS Over300Mins,
	       COUNT(CASE WHEN Minutes <= 300 THEN 1 END) AS Under300Mins,
	       COUNT(*) AS TotalPlayers
    FROM TotalMins
)
SELECT 
    Over300Mins,
    Under300Mins,
    ROUND((Over300Mins * 100.0) / TotalPlayers, 2) AS Over300MinsPercentage,
    ROUND((Under300Mins * 100.0) / TotalPlayers, 2) AS Under300MinsPercentage
FROM 
    Counts;

--Average age per team
SELECT T.team_name, round(AVG(EXTRACT(YEAR FROM AGE(P.birthdate))), 1) AS average_age
FROM cleaned_players P JOIN cleaned_teams T
ON P.team_id = T.team_id
GROUP BY T.team_name
ORDER BY average_age DESC;

--Total wins per team
SELECT team_name,
	   COUNT(*) FILTER (WHERE cleaned_matches.home_team_id = cleaned_teams.team_id AND home_team_score > away_team_score) AS TotalHomeWins,
	   COUNT(*) FILTER (WHERE cleaned_matches.away_team_id = cleaned_teams.team_id AND away_team_score > home_team_score) AS TotalAwayWins
FROM cleaned_matches JOIN cleaned_teams
ON cleaned_matches.home_team_id = cleaned_teams.team_id 
   OR cleaned_matches.away_team_id = cleaned_teams.team_id
GROUP BY team_id, team_name
ORDER BY TotalHomeWins DESC, TotalAwayWins DESC;

-- Most expensive player transfers
SELECT P.player_name, T.trans_fee,F.team_id, F.team_name AS from_team_name,ToT.team_id, ToT.team_name AS to_team_name
FROM cleaned_transfer_history T 
JOIN cleaned_players P
	ON T.player_id = P.player_id
LEFT JOIN 
    cleaned_teams F
    ON T.from_team_id = F.team_id
LEFT JOIN 
    cleaned_teams ToT
    ON T.to_team_id = ToT.team_id
WHERE 
    T.trans_fee = (SELECT MAX(trans_fee) FROM cleaned_transfer_history)
ORDER BY P.player_name;


-- Total clean sheets per team 
SELECT 
    T.team_name,
    COUNT(*) FILTER (
        WHERE (M.home_team_id = T.team_id AND M.away_team_score = 0)
           OR (M.away_team_id = T.team_id AND M.home_team_score = 0)
    ) AS clean_sheets
FROM 
    cleaned_matches M
JOIN 
    cleaned_teams T
    ON M.home_team_id = T.team_id OR M.away_team_id = T.team_id
GROUP BY 
    T.team_name
ORDER BY 
    clean_sheets DESC;








