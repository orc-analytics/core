ALTER TABLE algorithm_dependency ADD COLUMN lookback_count BIGINT NOT NULL DEFAULT 0 CHECK (lookback_count >= 0);
ALTER TABLE algorithm_dependency ADD COLUMN lookback_timedelta BIGINT NOT NULL DEFAULT 0 CHECK (lookback_timedelta >= 0);
