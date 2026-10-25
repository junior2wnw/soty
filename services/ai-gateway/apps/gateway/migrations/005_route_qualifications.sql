ALTER TABLE health_observations ADD COLUMN profile text
  CHECK(profile IS NULL OR profile IN ('text','json_object','tools'));
ALTER TABLE health_observations ADD COLUMN tools_verified boolean NOT NULL DEFAULT false;
CREATE INDEX health_qualifications_recent ON health_observations(provider_id,model,recorded_at DESC)
  WHERE outcome='success' AND (profile='json_object' OR (profile='tools' AND tools_verified=true));
CREATE INDEX service_probe_budget_day ON service_budget_holds(day) WHERE purpose='probe';
