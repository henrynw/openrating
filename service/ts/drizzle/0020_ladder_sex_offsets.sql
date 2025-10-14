CREATE TABLE IF NOT EXISTS ladder_sex_offsets (
    ladder_id TEXT NOT NULL REFERENCES rating_ladders(ladder_id) ON DELETE CASCADE,
    sex TEXT NOT NULL CHECK (sex IN ('M', 'F', 'X')),
    bias DOUBLE PRECISION NOT NULL DEFAULT 0,
    matches INTEGER NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (ladder_id, sex)
);
