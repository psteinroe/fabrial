import { createHmac } from "node:crypto";
import { alice, repository } from "./helper.ts";

export const outsider = { id: 7, login: "mallory", type: "User" };
export const bot = { id: 99, login: "fabrial[bot]", type: "Bot" };
const pr = {
	id: 3,
	number: 5,
	title: "Fix it",
	body: null,
	html_url: "https://github.com/acme/app/pull/5",
	user: alice,
};

/** Signed `/github/webhook` deliveries for pull request events. */
export const webhooks = {
	review: (state: string, association = "MEMBER", user = alice) => ({
		event: "pull_request_review",
		payload: {
			action: "submitted",
			repository,
			sender: user,
			pull_request: pr,
			review: { id: 11, state, body: "Please add tests", user, author_association: association },
		},
	}),
	comment: (association = "MEMBER", user = alice, body = "Looks close") => ({
		event: "issue_comment",
		payload: {
			action: "created",
			repository,
			sender: user,
			issue: { ...pr, pull_request: {} },
			comment: { id: 12, body, user, author_association: association },
		},
	}),
	reviewComment: (association = "COLLABORATOR") => ({
		event: "pull_request_review_comment",
		payload: {
			action: "created",
			repository,
			sender: alice,
			pull_request: pr,
			comment: {
				id: 13,
				body: "Nit",
				user: alice,
				author_association: association,
				path: "src/a.ts",
			},
		},
	}),
	closed: (merged: boolean) => ({
		event: "pull_request",
		payload: { action: "closed", repository, sender: alice, pull_request: { ...pr, merged } },
	}),
	reopened: () => ({
		event: "pull_request",
		payload: { action: "reopened", repository, sender: alice, pull_request: pr },
	}),
	labeled: (name = "needs-tests") => ({
		event: "pull_request",
		payload: { action: "labeled", repository, sender: alice, pull_request: pr, label: { name } },
	}),
	synchronize: () => ({
		event: "pull_request",
		payload: {
			action: "synchronize",
			repository,
			sender: alice,
			pull_request: pr,
			after: "0123456789abcdef",
		},
	}),
	checkRun: (conclusion = "failure") => ({
		event: "check_run",
		payload: {
			action: "completed",
			repository,
			sender: alice,
			check_run: {
				name: "ci",
				head_sha: "0123456789abcdef",
				conclusion,
				html_url: "https://github.com/acme/app/runs/1",
				pull_requests: [{ number: 5 }],
			},
		},
	}),
	checkSuite: (conclusion = "success") => ({
		event: "check_suite",
		payload: {
			action: "completed",
			repository,
			sender: alice,
			check_suite: {
				head_sha: "0123456789abcdef",
				conclusion,
				app: { name: "Actions" },
				pull_requests: [{ number: 5 }],
			},
		},
	}),
};

export function deliver(
	hook: { event: string; payload: unknown },
	delivery: string,
	secret = "secret",
	url = "https://example.com/github/webhook",
): Request {
	const body = JSON.stringify(hook.payload);
	return new Request(url, {
		method: "POST",
		body,
		headers: {
			"x-github-event": hook.event,
			"x-github-delivery": delivery,
			"x-hub-signature-256": `sha256=${createHmac("sha256", secret).update(body).digest("hex")}`,
		},
	});
}
