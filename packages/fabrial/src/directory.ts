import type {
	ExternalIdentity,
	GroupDefinition,
	IdentityDirectory,
	Principal,
	UserDefinition,
} from "./identity.ts";
import type { AnyPlugin } from "./plugin.ts";
import type { Clients } from "./register.ts";

export function identityKey(identity: ExternalIdentity): string {
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
	definitions: readonly (UserDefinition | GroupDefinition)[],
	plugins: readonly AnyPlugin[],
	clients: () => Clients,
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
	for (const definition of definitions) {
		if (definition.kind === "fabrial.user") add(definition);
		else for (const user of definition.members ?? []) add(user);
	}
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
