-- Error tracking table for ETL pipeline monitoring

CREATE TABLE IF NOT EXISTS nba.etl_errors (
  id              SERIAL PRIMARY KEY,
  process_name    TEXT NOT NULL,   
  game_id         VARCHAR(10),          
  error_type      TEXT NOT NULL,        
  error_message   TEXT NOT NULL,       
  stack_trace     TEXT,                 
  retry_count     INT DEFAULT 0,        
  is_resolved     BOOLEAN DEFAULT FALSE,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_retry_at   TIMESTAMPTZ,          
  resolved_at     TIMESTAMPTZ           
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
