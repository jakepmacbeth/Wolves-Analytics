-- Creates standard team and player box score tables,
-- team and player dimension tables, 


-- STANDARD GAME TABLE

CREATE TABLE IF NOT EXISTS nba.fact_games (
  game_id           VARCHAR(10) PRIMARY KEY,
  season            VARCHAR(7)  NOT NULL,
  game_date         DATE        NOT NULL,
  game_datetime_utc TIMESTAMPTZ NULL,

  home_team_id      INT         NOT NULL,
  away_team_id      INT         NOT NULL,

  home_points       INT         NULL,
  away_points       INT         NULL,

  game_status       TEXT        NULL,
  arena_name        TEXT        NULL,
  arena_city        TEXT        NULL,
  arena_state       TEXT        NULL,

  last_updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fact_games_season
  ON nba.fact_games (season);

CREATE INDEX IF NOT EXISTS idx_fact_games_game_date
  ON nba.fact_games (game_date);


-- TEAM DIMENSION 

CREATE TABLE IF NOT EXISTS nba.dim_teams (
  team_id         INT PRIMARY KEY,
  abbreviation    TEXT NULL,
  team_name       TEXT NULL,
  city            TEXT NULL,
  full_name       TEXT NULL,
  conference      TEXT NULL,
  division        TEXT NULL,
  is_active       BOOLEAN NULL,
  last_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


-- PLAYER DIMENSION 
-- note players team is subject to change and is not included here

CREATE TABLE IF NOT EXISTS nba.dim_players (
  player_id       INT PRIMARY KEY,

  full_name       TEXT NULL,
  first_name      TEXT NULL,
  last_name       TEXT NULL,

  position        TEXT NULL,
  height          TEXT NULL,
  weight          TEXT NULL,

  birthdate       DATE NULL,
  country         TEXT NULL,

  is_active       BOOLEAN NULL,
  last_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);



-- TEAM BOX SCORE PER GAME 

CREATE TABLE IF NOT EXISTS nba.teambox_pergame (
  game_id         VARCHAR(10) NOT NULL,
  team_id         INT         NOT NULL,
  season          VARCHAR(7)  NOT NULL,

  is_home         BOOLEAN     NULL,
  opponent_team_id INT       NULL,

  -- Traditional boxscore team totals
  minutes         TEXT NULL,
  pts             INT  NULL,

  fgm             INT  NULL,
  fga             INT  NULL,
  fg3m            INT  NULL,
  fg3a            INT  NULL,
  ftm             INT  NULL,
  fta             INT  NULL,

  oreb            INT  NULL,
  dreb            INT  NULL,
  reb             INT  NULL,
  ast             INT  NULL,
  stl             INT  NULL,
  blk             INT  NULL,
  tov             INT  NULL,
  pf              INT  NULL,


-- ADVANCED BOX SCORE

  off_rating      NUMERIC NULL,
  def_rating      NUMERIC NULL,
  net_rating      NUMERIC NULL,
  pace            NUMERIC NULL,
  ts_pct          NUMERIC NULL,

  last_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  PRIMARY KEY (game_id, team_id),

  CONSTRAINT fk_teambox_game
    FOREIGN KEY (game_id) REFERENCES nba.fact_games (game_id)
      ON DELETE CASCADE,

  CONSTRAINT fk_teambox_team
    FOREIGN KEY (team_id) REFERENCES nba.dim_teams (team_id)
      DEFERRABLE INITIALLY DEFERRED
);

CREATE INDEX IF NOT EXISTS idx_teambox_team
  ON nba.teambox_pergame (team_id);

CREATE INDEX IF NOT EXISTS idx_teambox_season
  ON nba.teambox_pergame (season);



-- PLAYER BOX SCORE PER GAME 

CREATE TABLE IF NOT EXISTS nba.playerbox_pergame (
  game_id         VARCHAR(10) NOT NULL,
  player_id       INT         NOT NULL,

  team_id         INT         NOT NULL,
  season          VARCHAR(7)  NOT NULL,

  is_home         BOOLEAN     NULL,
  opponent_team_id INT       NULL,

  starter_flag    BOOLEAN     NULL,
  minutes         TEXT        NULL,

  pts             INT  NULL,
  reb             INT  NULL,
  ast             INT  NULL,
  stl             INT  NULL,
  blk             INT  NULL,
  tov             INT  NULL,
  pf              INT  NULL,

  fgm             INT  NULL,
  fga             INT  NULL,
  fg3m            INT  NULL,
  fg3a            INT  NULL,
  ftm             INT  NULL,
  fta             INT  NULL,

  plus_minus      INT  NULL,

  last_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  PRIMARY KEY (game_id, player_id),

  CONSTRAINT fk_playerbox_game
    FOREIGN KEY (game_id) REFERENCES nba.fact_games (game_id)
      ON DELETE CASCADE,

  CONSTRAINT fk_playerbox_player
    FOREIGN KEY (player_id) REFERENCES nba.dim_players (player_id)
      DEFERRABLE INITIALLY DEFERRED,

  CONSTRAINT fk_playerbox_team
    FOREIGN KEY (team_id) REFERENCES nba.dim_teams (team_id)
      DEFERRABLE INITIALLY DEFERRED
);

CREATE INDEX IF NOT EXISTS idx_playerbox_player
  ON nba.playerbox_pergame (player_id);

CREATE INDEX IF NOT EXISTS idx_playerbox_team
  ON nba.playerbox_pergame (team_id);

CREATE INDEX IF NOT EXISTS idx_playerbox_season
  ON nba.playerbox_pergame (season);
