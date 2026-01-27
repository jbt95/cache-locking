import { createServer } from 'node:net';
import { describe } from 'vitest';
import { getContainerRuntimeClient } from 'testcontainers';

export const integrationEnabled = process.env.INTEGRATION_TESTS === '1';
const miniflareEnabled = integrationEnabled && process.env.MINIFLARE_TESTS === '1';

const canUseContainers = async (): Promise<boolean> => {
  if (!integrationEnabled) {
    return false;
  }
  try {
    await getContainerRuntimeClient();
    return true;
  } catch {
    return false;
  }
};

const canListenOnLocalhost = async (): Promise<boolean> =>
  new Promise((resolve) => {
    let settled = false;
    const server = createServer();

    const finalize = (result: boolean) => {
      if (settled) {
        return;
      }
      settled = true;
      server.removeAllListeners();
      resolve(result);
    };

    server.once('error', () => finalize(false));

    try {
      server.listen(0, '127.0.0.1', () => {
        server.close(() => finalize(true));
      });
      server.unref();
    } catch {
      finalize(false);
    }
  });

const containerRuntimeAvailable = await canUseContainers();
const miniflareRuntimeAvailable = miniflareEnabled ? await canListenOnLocalhost() : false;

export const describeIntegration = integrationEnabled ? describe : describe.skip;
export const describeContainerIntegration =
  integrationEnabled && containerRuntimeAvailable ? describe : describe.skip;
export const describeMiniflareIntegration =
  miniflareEnabled && miniflareRuntimeAvailable ? describe : describe.skip;

export const env = (key: string, fallback: string): string => process.env[key] ?? fallback;

export const wait = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

export const makeTestPrefix = (label: string): string =>
  `test:${label}:${Date.now()}:${Math.random().toString(36).slice(2)}:`;
