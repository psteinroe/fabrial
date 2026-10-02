import type { Clients } from "./register.ts";

/** An identity in an external system, scoped to a provider installation. */
export interface ExternalIdentity {
	/** Plugin id, e.g. "slack". */
	provider: string;
	/** Workspace / org / installation the subject belongs to. */
	installationId: string;
	/** The provider's user id, e.g. Slack "U123". */
	subjectId: string;
}

/** A resolved person or service acting in Fabrial. */
export interface Principal {
	/** Stable Fabrial user id; for unknown people a provider-scoped id (`slack:T1:U123`). */
	id: string;
	name?: string;
	identities: ExternalIdentity[];
	/** False when the external identity matched no defined user. */
	known: boolean;
}

export interface UserDefinition {
	readonly kind: "fabrial.user";
	readonly id: string;
	readonly name?: string;
	readonly identities: readonly ExternalIdentity[];
}

export function defineUser(definition: {
	id: string;
	name?: string;
	identities: ExternalIdentity[];
}): UserDefinition {
	return { kind: "fabrial.user", ...definition };
}

/** Context available to dynamic group resolvers. */
export interface GroupResolveContext {
	readonly clients: Clients;
	readonly now: Date;
	/** Resolve a user definition or external identity to a principal. */
	principal(user: UserDefinition | ExternalIdentity): Promise<Principal>;
}

export interface GroupDefinition {
	readonly kind: "fabrial.group";
	readonly id: string;
	/** Static members, or a resolver for dynamic membership (rotations, Slack user groups, …). */
	readonly members?: readonly UserDefinition[];
	readonly resolve?: (
		ctx: GroupResolveContext,
	) =>
		| Promise<readonly (UserDefinition | ExternalIdentity | Principal)[]>
		| readonly (UserDefinition | ExternalIdentity | Principal)[];
}

export function defineGroup(
	definition:
		| { id: string; members: UserDefinition[] }
		| { id: string; resolve: GroupDefinition["resolve"] & {} },
): GroupDefinition {
	return { kind: "fabrial.group", ...definition };
}

/**
 * Pick one member per week, round robin, starting at `start` (ISO date, a Monday by convention).
 * Usable inside `defineGroup({ resolve })`.
 */
export function weeklyRotation<T>(
	members: readonly T[],
	options: { start: string; now?: Date },
): T {
	if (members.length === 0) throw new Error("weeklyRotation needs at least one member");
	const start = Date.parse(options.start);
	if (Number.isNaN(start)) throw new Error(`weeklyRotation: invalid start date "${options.start}"`);
	const now = (options.now ?? new Date()).getTime();
	const weeks = Math.floor((now - start) / (7 * 24 * 60 * 60 * 1000));
	const index = ((weeks % members.length) + members.length) % members.length;
	return members[index]!;
}

/** Resolves identities and groups. Implemented by Fabrial core from defined users, groups, and plugins. */
export interface IdentityDirectory {
	resolveIdentity(identity: ExternalIdentity): Promise<Principal>;
	identitiesFor(userId: string): Promise<ExternalIdentity[]>;
	members(group: GroupDefinition): Promise<Principal[]>;
	isMember(principal: Principal, group: GroupDefinition): Promise<boolean>;
}

/** Identity facts of one invocation. */
export interface InvocationIdentity {
	/** Human or service that initiated the work. */
	requestedBy: Principal | null;
	/** Agent/service performing the current operation, e.g. `agent:bug-investigator`. */
	executedBy?: string;
}
