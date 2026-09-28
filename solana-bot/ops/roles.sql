-- Optional DB role separation (run as a superuser). PAPER has no signer tables at all;
-- the worker/api role gets DML on the schema but no DDL and no role management.
CREATE ROLE solbot_paper LOGIN PASSWORD :'paper_password';
GRANT CONNECT ON DATABASE solbot TO solbot_paper;
GRANT USAGE ON SCHEMA public TO solbot_paper;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO solbot_paper;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO solbot_paper;
-- append-only tables are additionally protected by triggers (UPDATE/DELETE raise)
REVOKE DELETE ON ALL TABLES IN SCHEMA public FROM solbot_paper;
