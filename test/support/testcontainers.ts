import { GenericContainer, Wait } from 'testcontainers';
import type { StartedTestContainer } from 'testcontainers';

type StartContainerOptions = {
  port: number;
  env?: Record<string, string>;
  command?: string[];
  startupTimeoutMs?: number;
};

export const startContainer = async (
  image: string,
  { port, env, command, startupTimeoutMs }: StartContainerOptions,
): Promise<StartedTestContainer> => {
  let container = new GenericContainer(image)
    .withExposedPorts(port)
    .withWaitStrategy(Wait.forListeningPorts())
    .withStartupTimeout(startupTimeoutMs ?? 120_000);

  if (env) {
    container = container.withEnvironment(env);
  }

  if (command) {
    container = container.withCommand(command);
  }

  return container.start();
};
