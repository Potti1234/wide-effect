import { normalizeAttributes, type SafeAttributes } from "@wide-effect/core";
import { Effect } from "effect";
import { Command } from "foldkit";

export interface InstrumentApplicationOptions<Message> {
  readonly interactionId: () => string;
  readonly summarizeMessage?: (message: Message) => Readonly<Record<string, unknown>>;
  readonly allowedSummaryPrefixes?: ReadonlyArray<string>;
  readonly interactions?: InteractionRegistry;
}
export interface InteractionRegistry {
  readonly register: (message: object, interactionId: string) => void;
  readonly lookup: (message: unknown) => string | undefined;
}
/** Uses weak keys: correlation never keeps a Message or its payload alive. */
export const makeInteractionRegistry = (): InteractionRegistry => {
  const values = new WeakMap<object, string>();
  return {
    register: (message, interactionId) => values.set(message, interactionId),
    lookup: (message) =>
      typeof message === "object" && message !== null ? values.get(message) : undefined,
  };
};
type ApplicationShape<Model, Message, InitArgs extends ReadonlyArray<unknown>, R> = Readonly<{
  init: (...args: InitArgs) => readonly [Model, ReadonlyArray<Command.Command<Message, never, R>>];
  update: (
    model: Model,
    message: Message,
  ) => readonly [Model, ReadonlyArray<Command.Command<Message, never, R>>];
}>;

/** Wraps the configuration boundary; the user's functions remain synchronous and pure. */
export const instrumentApplication = <
  Model,
  Message extends Readonly<{ _tag: string }>,
  InitArgs extends ReadonlyArray<unknown>,
  R,
  App extends ApplicationShape<Model, Message, InitArgs, R>,
>(
  application: App,
  options: InstrumentApplicationOptions<Message>,
): Omit<App, "init" | "update"> & ApplicationShape<Model, Message, InitArgs, R> => {
  const instrument = (
    commands: ReadonlyArray<Command.Command<Message, never, R>>,
    messageTag: string,
    interactionId: string,
    summary: SafeAttributes,
  ) =>
    commands.map((command) =>
      Command.mapEffect(command, (effect) =>
        Effect.annotateCurrentSpan({
          "foldkit.interaction.id": interactionId,
          "foldkit.message.tag": messageTag,
          ...prefixSummary(summary),
        }).pipe(Effect.andThen(effect)),
      ),
    );
  const summarize = (message: Message): SafeAttributes =>
    normalizeAttributes(options.summarizeMessage?.(message) ?? {}, {
      allowedPrefixes: options.allowedSummaryPrefixes ?? [],
    });
  return {
    ...application,
    init: (...args: InitArgs) => {
      const [model, commands] = application.init(...args);
      return [model, instrument(commands, "Init", options.interactionId(), {})];
    },
    update: (model: Model, message: Message) => {
      const interactionId = options.interactionId();
      options.interactions?.register(message, interactionId);
      const [next, commands] = application.update(model, message);
      return [next, instrument(commands, message._tag, interactionId, summarize(message))];
    },
  };
};
const prefixSummary = (summary: SafeAttributes): SafeAttributes =>
  Object.fromEntries(
    Object.entries(summary).map(([key, value]) => [`foldkit.message.${key}`, value]),
  );
