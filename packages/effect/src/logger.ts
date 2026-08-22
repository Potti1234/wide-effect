import { Logger } from "effect";

export interface LoggerLayerOptions {
  readonly humanLogger?: Logger.Logger<unknown, void> | undefined;
}

export const loggerLayer = (options: LoggerLayerOptions = {}) =>
  Logger.layer([options.humanLogger ?? Logger.defaultLogger, Logger.tracerLogger]);
