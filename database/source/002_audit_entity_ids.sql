-- Repair the reference field from the immutable operation response. These audit
-- metadata changes go through real WAL/CDC; no ledger or monetary value changes.
BEGIN;
CREATE TABLE IF NOT EXISTS public.platform_migrations(version integer PRIMARY KEY,applied_at timestamptz NOT NULL DEFAULT now());
INSERT INTO public.platform_migrations(version) VALUES(1) ON CONFLICT DO NOTHING;
DO $$ BEGIN
 IF NOT EXISTS(SELECT 1 FROM public.platform_migrations WHERE version=2) THEN
  UPDATE trading.audit_events SET entity_id=(details->>'id')::uuid
  WHERE details->>'id' IS NOT NULL AND entity_id::text<>details->>'id';
  INSERT INTO public.platform_migrations(version) VALUES(2);
 END IF;
END $$;
COMMIT;
