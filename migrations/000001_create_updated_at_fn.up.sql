-- Generic trigger function used by every table that has an updated_at
-- column. Keeps "when was this row last touched" accurate without
-- relying on every service to set it manually on every UPDATE.
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
