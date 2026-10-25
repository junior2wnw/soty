CREATE TABLE response_cache (
  request_id uuid PRIMARY KEY REFERENCES inference_requests(id),
  encrypted bytea NOT NULL, iv bytea NOT NULL, tag bytea NOT NULL,
  expires_at timestamptz NOT NULL DEFAULT (now()+interval '15 minutes')
);
CREATE INDEX response_cache_expiry ON response_cache(expires_at);
