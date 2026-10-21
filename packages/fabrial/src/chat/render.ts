import {
	Actions,
	Button,
	Card as SDKCard,
	CardText,
	Field,
	Fields,
	type AdapterPostableMessage,
	type ChatElement,
	toCardElement,
} from "chat";
import type { Card, OutboundMessage } from "../thread.ts";

/** Keyed globally, so a plugin with its own copy of Fabrial still finds it. */
const LITERAL_CARD = Symbol.for("fabrial.card");

/**
 * The Fabrial card a rendered message came from. Adapters that would transform card text (emoji,
 * markdown, mentions) render this instead, so what it shows stays literal.
 */
export function literalCard(message: unknown): Card | undefined {
	if (!message || typeof message !== "object") return undefined;
	const card = (message as { [LITERAL_CARD]?: Card })[LITERAL_CARD];
	if (card) return card;
	const inner = (message as { card?: unknown }).card;
	return inner && typeof inner === "object" ? literalCard(inner) : undefined;
}

export function renderMessage(message: OutboundMessage): AdapterPostableMessage {
	if (typeof message === "string" || "markdown" in message) return message;
	const card = message.card;
	const children: ChatElement[] = [];
	if (card.text) children.push(CardText(card.text));
	if (card.fields?.length) children.push(Fields(card.fields.map((field) => Field(field))));
	if (card.code) {
		const fence = "`".repeat(
			Math.max(3, ...[...card.code.content.matchAll(/`+/g)].map((m) => m[0].length + 1)),
		);
		children.push(CardText(`${fence}${card.code.language ?? ""}\n${card.code.content}\n${fence}`));
	}
	if (card.section) {
		children.push(CardText(card.section.title, { style: "bold" }));
		if (card.section.text) children.push(CardText(card.section.text));
		if (card.section.fields?.length)
			children.push(Fields(card.section.fields.map((field) => Field(field))));
	}
	if (card.footer) children.push(CardText(card.footer));
	if (card.actions?.length) children.push(Actions(card.actions.map((action) => Button(action))));
	const rendered = toCardElement(SDKCard({ title: card.title, children }));
	if (!rendered) throw new Error("Invalid card");
	// Enumerable, so the SDK's copies keep it; a symbol, so it never serializes.
	return Object.assign(rendered, { [LITERAL_CARD]: card });
}
