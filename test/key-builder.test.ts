import { describe, expect, it, vi } from 'vitest';
import type { GetOrSetFn } from '@core/types';
import { CacheOutcome } from '@core/types';
import { KeyBuilder, createKeyBuilder, withKeyPrefix } from '@core/key-builder';

describe('KeyBuilder', () => {
  it('joins parts with separators and filters empty values', () => {
    const builder = new KeyBuilder({ prefix: 'app', separator: ':' });

    const key = builder.build('', 'users', undefined, null, 0, false, 'x');

    expect(key).toBe('app:users:0:false:x');
  });

  it('handles prefixes with trailing separators and empty cores', () => {
    const builder = new KeyBuilder({ prefix: 'scope:' });

    expect(builder.build('item')).toBe('scope:item');
    expect(builder.build()).toBe('scope:');
  });

  it('creates a builder function', () => {
    const build = createKeyBuilder({ prefix: 'svc', separator: '-' });

    expect(build('a', 'b')).toBe('svc-a-b');
  });

  it('prefixes keys for wrapped getOrSet calls', async () => {
    const base = vi.fn<GetOrSetFn<string>>(async () => ({ value: 'ok', meta: { cache: CacheOutcome.HIT } }));
    const wrapped = withKeyPrefix(base, 'ns', '::');

    await wrapped('key', async () => 'value', { cacheTtl: 5 });

    expect(base).toHaveBeenCalledTimes(1);
    expect(base.mock.calls[0][0]).toBe('ns::key');
    expect(base.mock.calls[0][2]).toEqual({ cacheTtl: 5 });
  });
});
