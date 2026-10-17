-- Example BUSINESS tables only. Conductor, Pi and Chat migrate their own state.
CREATE TABLE IF NOT EXISTS acme_accounts (
    organisation_id text PRIMARY KEY,
    balance integer NOT NULL
);
CREATE TABLE IF NOT EXISTS acme_changelog (
    repo text NOT NULL,
    number integer NOT NULL,
    title text NOT NULL,
    PRIMARY KEY (repo, number)
);
-- Apply tenant RLS and least-privilege grants for your actual deployment.
-- acme.organisation_id is transaction-local context, NOT an authorization policy.
