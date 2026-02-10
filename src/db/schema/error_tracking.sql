-- Error tracking table for ETL pipeline monitoring
-- Allows tracking and retry of failed operations

CREATE TABLE IF NOT EXISTS nba.etl_errors (
  id              SERIAL PRIMARY KEY,
  process_name    TEXT NOT NULL,        -- e.g., 'load_game_structure', 'load_teambox'
  game_id         VARCHAR(10),          -- Associated game_id if applicable
  error_type      TEXT NOT NULL,        -- Exception class name
  error_message   TEXT NOT NULL,        -- Exception message
  stack_trace     TEXT,                 -- Full traceback for debugging
  retry_count     INT DEFAULT 0,        -- Number of retry attempts
  is_resolved     BOOLEAN DEFAULT FALSE,-- Whether error was eventually resolved
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_retry_at   TIMESTAMPTZ,          -- Timestamp of last retry attempt
  resolved_at     TIMESTAMPTZ           -- Timestamp when resolved
);

-- Index for finding unresolved errors to retry
CREATE INDEX IF NOT EXISTS idx_etl_errors_unresolved 
  ON nba.etl_errors (is_resolved, created_at)
  WHERE is_resolved = FALSE;

-- Index for finding errors by process
CREATE INDEX IF NOT EXISTS idx_etl_errors_process
  ON nba.etl_errors (process_name, created_at DESC);

-- Index for finding errors by game_id
CREATE INDEX IF NOT EXISTS idx_etl_errors_game_id
  ON nba.etl_errors (game_id)
  WHERE game_id IS NOT NULL;

COMMENT ON TABLE nba.etl_errors IS 'Tracks ETL pipeline errors for monitoring and retry';
COMMENT ON COLUMN nba.etl_errors.retry_count IS 'Number of retry attempts made';
COMMENT ON COLUMN nba.etl_errors.is_resolved IS 'True if subsequent run successfully processed this item';