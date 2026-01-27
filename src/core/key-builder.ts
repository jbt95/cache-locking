import type { GetOrSetFn } from '@core/types';

export type KeyPart = string | number | boolean | null | undefined;

export type KeyBuilderOptions = {
  prefix?: string;
  separator?: string;
};

const normalizePart = (part: KeyPart): string => (part === undefined || part === null ? '' : String(part));

export class KeyBuilder {
  private readonly separator: string;
  private readonly prefix: string;

  constructor(options: KeyBuilderOptions = {}) {
    this.separator = options.separator ?? ':';
    this.prefix = options.prefix ?? '';
  }

  build(...parts: KeyPart[]): string {
    const filtered = parts.map(normalizePart).filter((part) => part.length > 0);
    const core = filtered.join(this.separator);
    if (!this.prefix) {
      return core;
    }
    if (!core) {
      return this.prefix;
    }
    return this.prefix.endsWith(this.separator) ? `${this.prefix}${core}` : `${this.prefix}${this.separator}${core}`;
  }
}

export const createKeyBuilder = (options: KeyBuilderOptions = {}): ((...parts: KeyPart[]) => string) => {
  const builder = new KeyBuilder(options);
  return (...parts: KeyPart[]): string => builder.build(...parts);
};

export const withKeyPrefix = <V>(getOrSet: GetOrSetFn<V>, prefix: string, separator = ':'): GetOrSetFn<V> => {
  const builder = new KeyBuilder({ prefix, separator });
  return (
    key: string,
    fetcher: Parameters<GetOrSetFn<V>>[1],
    options?: Parameters<GetOrSetFn<V>>[2],
  ): ReturnType<GetOrSetFn<V>> => getOrSet(builder.build(key), fetcher, options);
};
