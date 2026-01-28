CREATE SCHEMA IF NOT EXISTS nba;

CREATE TABLE IF NOT EXISTS nba.spine (
  game_id       VARCHAR(10) PRIMARY KEY,
  season        VARCHAR(7)  NOT NULL,
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_spine_season
  ON nba.spine(season);
