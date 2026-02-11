
-- Season-level 2/3/4/5-man lineup stats by TEAM (NO GameID filter)


CREATE TABLE IF NOT EXISTS nba.lineupbox_season (
  season         VARCHAR(7)  NOT NULL,
  team_id        INT         NOT NULL,
  group_quantity INT         NOT NULL,   -- 2,3,4,5

  group_set      TEXT        NULL,
  group_id       TEXT        NOT NULL,
  group_name     TEXT        NULL,

  gp             INT         NULL,
  w              INT         NULL,
  l              INT         NULL,
  w_pct          NUMERIC     NULL,

  min            NUMERIC     NULL,

  fgm            INT         NULL,
  fga            INT         NULL,
  fg_pct         NUMERIC     NULL,

  fg3m           INT         NULL,
  fg3a           INT         NULL,
  fg3_pct        NUMERIC     NULL,

  ftm            INT         NULL,
  fta            INT         NULL,
  ft_pct         NUMERIC     NULL,

  oreb           INT         NULL,
  dreb           INT         NULL,
  reb            INT         NULL,
  ast            INT         NULL,
  tov            INT         NULL,
  stl            INT         NULL,
  blk            INT         NULL,
  blka           INT         NULL,
  pf             INT         NULL,
  pfd            INT         NULL,
  pts            INT         NULL,
  plus_minus     INT         NULL,

  last_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  PRIMARY KEY (season, team_id, group_quantity, group_id),

  CONSTRAINT fk_lineupseason_team
    FOREIGN KEY (team_id) REFERENCES nba.dim_teams (team_id)
      DEFERRABLE INITIALLY DEFERRED
);

CREATE INDEX IF NOT EXISTS idx_lineupseason_team
  ON nba.lineupbox_season (team_id);

CREATE INDEX IF NOT EXISTS idx_lineupseason_season
  ON nba.lineupbox_season (season);

CREATE INDEX IF NOT EXISTS idx_lineupseason_groupqty
  ON nba.lineupbox_season (group_quantity);
