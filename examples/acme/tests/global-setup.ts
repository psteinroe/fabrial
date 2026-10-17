import { PostgreSqlContainer } from "@testcontainers/postgresql";
import postgres from "postgres";

/** Set definition-time values before Vitest imports any example modules. */
export default async function setup() {
	const container = await new PostgreSqlContainer("postgres:17-alpine").start();
	const admin = postgres(container.getConnectionUri());
	try {
		await admin`CREATE DATABASE acme_application`;
		const application = new URL(container.getConnectionUri());
		application.pathname = "acme_application";
		Object.assign(process.env, {
			DATABASE_URL: container.getConnectionUri(),
			APP_DATABASE_URL: application.toString(),
			SLACK_WORKSPACE: "acme",
			SLACK_TEAM_ID: "T_ACME",
			SLACK_BOT_TOKEN: "xoxb-test",
			SLACK_SIGNING_SECRET: "slack-secret",
			GITHUB_OWNER: "acme",
			GITHUB_INSTALLATION_ID: "42",
			GITHUB_BOT_USER_ID: "99",
			GITHUB_TOKEN: "github-test",
			GITHUB_WEBHOOK_SECRET: "github-secret",
			LINEAR_ORGANIZATION_ID: "acme",
			LINEAR_API_KEY: "linear-test",
			LINEAR_WEBHOOK_SECRET: "linear-secret",
		});
	} catch (error) {
		await container.stop();
		throw error;
	} finally {
		await admin.end();
	}
	return async () => {
		await container.stop();
	};
}
