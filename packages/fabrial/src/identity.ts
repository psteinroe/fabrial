// Users, groups and rotations, and the directory that resolves identities and current group membership.
import type { AnyPlugin } from "./plugin.ts";

export function chatIdentity(
	provider: string,
	installationId: string,
	subjectId: string,
): ExternalIdentity {
	return { provider, installationId, subjectId };
}
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
export interface GroupResolveContext<TClients = Record<string, unknown>> {
	readonly clients: TClients;
	readonly now: Date;
	/** Resolve a user definition or external identity to a principal. */
	principal(user: UserDefinition | ExternalIdentity): Promise<Principal>;
}

export interface GroupDefinition<TClients = Record<string, unknown>> {
	readonly kind: "fabrial.group";
	readonly id: string;
	/** Static members, or a resolver for dynamic membership (rotations, Slack user groups, …). */
	readonly members?: readonly UserDefinition[];
	resolve?(
		ctx: GroupResolveContext<TClients>,
	):
		| Promise<readonly (UserDefinition | ExternalIdentity | Principal)[]>
		| readonly (UserDefinition | ExternalIdentity | Principal)[];
}

/**
 * Groups by id, registered when they are defined (also by plugins, e.g. Slack user groups), so any
 * process can check a stored group id against the group's current members. Ids are unique per
 * process: defining one twice throws and leaves the first registration in place.
 */
const definedGroups = new Map<string, GroupDefinition>();

export function defineGroup<TClients = Record<string, unknown>>(
	definition:
		| { id: string; members: UserDefinition[] }
		| { id: string; resolve: GroupDefinition<TClients>["resolve"] & {} },
): GroupDefinition<TClients> {
	if (definedGroups.has(definition.id))
		throw new Error(
			`Group ${definition.id} is already defined: group ids must be unique per process`,
		);
	const group: GroupDefinition<TClients> = { kind: "fabrial.group", ...definition };
	definedGroups.set(group.id, group as GroupDefinition);
	return group;
}

/** The group defined with `id`; undefined when none is. */
export function definedGroup(id: string): GroupDefinition | undefined {
	return definedGroups.get(id);
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

function identityKey(identity: ExternalIdentity): string {
	return `${identity.provider}:${identity.installationId}:${identity.subjectId}`;
}

export function samePrincipal(a: Principal, b: Principal): boolean {
	return (
		a.id === b.id ||
		a.identities.some((identity) =>
			b.identities.some((other) => identityKey(identity) === identityKey(other)),
		)
	);
}

export function createDirectory(
	definitions: readonly UserDefinition[],
	plugins: readonly AnyPlugin[],
	clients: () => Record<string, unknown>,
	now: () => number,
): IdentityDirectory {
	const users = new Map<string, UserDefinition>();
	const identities = new Map<string, UserDefinition>();
	const unknown = new Map<string, ExternalIdentity>();
	function add(user: UserDefinition) {
		const previous = users.get(user.id);
		if (previous && previous !== user) throw new Error(`Duplicate user: ${user.id}`);
		users.set(user.id, user);
		for (const identity of user.identities) {
			const key = identityKey(identity);
			if (identities.has(key) && identities.get(key)!.id !== user.id)
				throw new Error(`Duplicate identity: ${key}`);
			identities.set(key, user);
		}
	}
	for (const user of definitions) add(user);
	const principal = (user: UserDefinition): Principal => {
		add(user);
		return {
			id: user.id,
			...(user.name ? { name: user.name } : {}),
			identities: [...user.identities],
			known: true,
		};
	};
	const directory: IdentityDirectory = {
		async resolveIdentity(identity) {
			const user = identities.get(identityKey(identity));
			if (!user) unknown.set(identityKey(identity), identity);
			const profile = await plugins
				.find((p) => p.id === identity.provider)
				?.identity?.lookup?.(identity, clients());
			return user
				? { ...principal(user), ...(user.name ? {} : profile) }
				: { id: identityKey(identity), identities: [identity], known: false, ...profile };
		},
		async identitiesFor(id) {
			return [...(users.get(id)?.identities ?? (unknown.has(id) ? [unknown.get(id)!] : []))];
		},
		async members(group) {
			const members = group.resolve
				? await group.resolve({
						clients: clients(),
						now: new Date(now()),
						principal: async (user) =>
							"kind" in user ? principal(user) : directory.resolveIdentity(user),
					})
				: (group.members ?? []);
			const resolved = await Promise.all(
				members.map(async (member) => {
					if ("kind" in member) return principal(member);
					if ("known" in member) return member;
					return directory.resolveIdentity(member);
				}),
			);
			return [...new Map(resolved.map((p) => [p.id, p])).values()];
		},
		async isMember(person, group) {
			return (await directory.members(group)).some((member) => samePrincipal(member, person));
		},
	};
	return directory;
}
