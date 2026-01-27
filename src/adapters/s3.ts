import { Duration } from 'effect';
import type { Cache, CoreClock, LeaseAcquireResult, LeaseReadyState, Leases } from '@core/types';
import {
  DeleteObjectCommand,
  GetObjectCommand,
  HeadObjectCommand,
  PutObjectCommand,
  type S3Client,
} from '@aws-sdk/client-s3';
import { Readable } from 'node:stream';

export type S3CacheOptions<V> = {
  client: S3Client;
  bucket: string;
  keyPrefix?: string;
  clock?: CoreClock;
  serialize?: (value: V) => string;
  deserialize?: (value: string) => V;
  expiresAtMetadataKey?: string;
  contentType?: string;
};

export type S3LeasesOptions = {
  client: S3Client;
  bucket: string;
  keyPrefix?: string;
  readyKeyPrefix?: string;
  readyTtl?: Duration.DurationInput;
  clock?: CoreClock;
  expiresAtMetadataKey?: string;
  ownerMetadataKey?: string;
};

const DEFAULT_READY_TTL = Duration.seconds(5);

const isNotFoundError = (error: unknown): boolean => {
  const status = (error as { $metadata?: { httpStatusCode?: number } }).$metadata?.httpStatusCode;
  const name = (error as { name?: string }).name;
  return status === 404 || name === 'NoSuchKey' || name === 'NotFound';
};

const isPreconditionFailed = (error: unknown): boolean => {
  const status = (error as { $metadata?: { httpStatusCode?: number } }).$metadata?.httpStatusCode;
  const name = (error as { name?: string }).name;
  return status === 412 || name === 'PreconditionFailed' || name === 'ConditionalCheckFailed';
};

const readEpochMillis = (value: unknown): number | null => {
  if (typeof value === 'number') {
    return Number.isFinite(value) ? value : null;
  }
  if (typeof value === 'string') {
    const asNumber = Number(value);
    if (Number.isFinite(asNumber)) {
      return asNumber;
    }
    const parsed = Date.parse(value);
    return Number.isNaN(parsed) ? null : parsed;
  }
  return null;
};

const readBody = async (body: unknown): Promise<string | null> => {
  if (!body) {
    return null;
  }

  if (typeof (body as { transformToString?: unknown }).transformToString === 'function') {
    return (body as { transformToString: (encoding?: string) => Promise<string> }).transformToString();
  }

  if (typeof body === 'string') {
    return body;
  }

  if (body instanceof Uint8Array) {
    return Buffer.from(body).toString('utf-8');
  }

  if (body instanceof Readable) {
    return new Promise((resolve, reject) => {
      const chunks: Buffer[] = [];
      body.on('data', (chunk) => {
        chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
      });
      body.on('error', reject);
      body.on('end', () => resolve(Buffer.concat(chunks).toString('utf-8')));
    });
  }

  if (typeof (body as { getReader?: unknown }).getReader === 'function') {
    const reader = (body as ReadableStream).getReader();
    const chunks: Uint8Array[] = [];
    let done = false;
    while (!done) {
      const result = await reader.read();
      if (result.value) {
        chunks.push(result.value);
      }
      done = result.done;
    }
    const size = chunks.reduce((total, chunk) => total + chunk.byteLength, 0);
    const merged = new Uint8Array(size);
    let offset = 0;
    for (const chunk of chunks) {
      merged.set(chunk, offset);
      offset += chunk.byteLength;
    }
    return Buffer.from(merged).toString('utf-8');
  }

  if (typeof (body as { text?: unknown }).text === 'function') {
    return (body as Blob).text();
  }

  return null;
};

export class S3Cache<V> implements Cache<V> {
  private readonly client: S3Client;
  private readonly bucket: string;
  private readonly keyPrefix: string;
  private readonly clock: CoreClock;
  private readonly serialize: (value: V) => string;
  private readonly deserialize: (value: string) => V;
  private readonly expiresAtMetadataKey: string;
  private readonly contentType?: string;

  constructor(options: S3CacheOptions<V>) {
    this.client = options.client;
    this.bucket = options.bucket;
    this.keyPrefix = options.keyPrefix ?? '';
    this.clock = options.clock ?? { now: () => Date.now() };
    this.serialize = options.serialize ?? ((value) => JSON.stringify(value));
    this.deserialize = options.deserialize ?? ((value) => JSON.parse(value) as V);
    this.expiresAtMetadataKey = (options.expiresAtMetadataKey ?? 'expires_at').toLowerCase();
    this.contentType = options.contentType;
  }

  private cacheKey(key: string): string {
    return `${this.keyPrefix}${key}`;
  }

  async get(key: string): Promise<V | null> {
    const cacheKey = this.cacheKey(key);
    try {
      const response = await this.client.send(
        new GetObjectCommand({
          Bucket: this.bucket,
          Key: cacheKey,
        }),
      );

      const expiresAt = readEpochMillis(response.Metadata?.[this.expiresAtMetadataKey]);
      if (expiresAt !== null && expiresAt <= this.clock.now()) {
        await this.client.send(new DeleteObjectCommand({ Bucket: this.bucket, Key: cacheKey }));
        return null;
      }

      const body = await readBody(response.Body);
      if (body === null) {
        return null;
      }
      return this.deserialize(body);
    } catch (error) {
      if (isNotFoundError(error)) {
        return null;
      }
      throw error;
    }
  }

  async set(key: string, value: V, ttl?: Duration.DurationInput): Promise<void> {
    const cacheKey = this.cacheKey(key);
    const serialized = this.serialize(value);

    const metadata: Record<string, string> = {};
    if (ttl !== undefined) {
      const ttlMs = Duration.toMillis(ttl);
      const expiresAt = this.clock.now() + Math.max(0, ttlMs);
      metadata[this.expiresAtMetadataKey] = String(expiresAt);
    }

    await this.client.send(
      new PutObjectCommand({
        Bucket: this.bucket,
        Key: cacheKey,
        Body: serialized,
        ContentType: this.contentType ?? 'application/json',
        Metadata: Object.keys(metadata).length > 0 ? metadata : undefined,
      }),
    );
  }
}

export class S3Leases implements Leases {
  private readonly client: S3Client;
  private readonly bucket: string;
  private readonly keyPrefix: string;
  private readonly readyKeyPrefix: string;
  private readonly readyTtl: Duration.DurationInput;
  private readonly clock: CoreClock;
  private readonly expiresAtMetadataKey: string;
  private readonly ownerMetadataKey: string;

  constructor(options: S3LeasesOptions) {
    this.client = options.client;
    this.bucket = options.bucket;
    this.keyPrefix = options.keyPrefix ?? 'lease:';
    this.readyKeyPrefix = options.readyKeyPrefix ?? `${this.keyPrefix}ready:`;
    this.readyTtl = options.readyTtl ?? DEFAULT_READY_TTL;
    this.clock = options.clock ?? { now: () => Date.now() };
    this.expiresAtMetadataKey = (options.expiresAtMetadataKey ?? 'expires_at').toLowerCase();
    this.ownerMetadataKey = (options.ownerMetadataKey ?? 'owner').toLowerCase();
  }

  private leaseKey(key: string): string {
    return `${this.keyPrefix}${key}`;
  }

  private readyKey(key: string): string {
    return `${this.readyKeyPrefix}${key}`;
  }

  private async headObject(key: string) {
    try {
      return await this.client.send(new HeadObjectCommand({ Bucket: this.bucket, Key: key }));
    } catch (error) {
      if (isNotFoundError(error)) {
        return null;
      }
      throw error;
    }
  }

  private async putLeaseObject(
    key: string,
    owner: string,
    leaseUntil: number,
    options?: { ifNoneMatch?: boolean; ifMatch?: string },
  ): Promise<void> {
    const metadata: Record<string, string> = {
      [this.expiresAtMetadataKey]: String(leaseUntil),
      [this.ownerMetadataKey]: owner,
    };

    await this.client.send(
      new PutObjectCommand({
        Bucket: this.bucket,
        Key: key,
        Body: owner,
        ContentType: 'text/plain',
        Metadata: metadata,
        IfNoneMatch: options?.ifNoneMatch ? '*' : undefined,
        IfMatch: options?.ifMatch,
      }),
    );
  }

  async acquire(key: string, owner: string, ttl: Duration.DurationInput): Promise<LeaseAcquireResult> {
    const now = this.clock.now();
    const ttlMs = Duration.toMillis(ttl);
    const leaseUntil = now + Math.max(0, ttlMs);
    const leaseKey = this.leaseKey(key);

    try {
      await this.putLeaseObject(leaseKey, owner, leaseUntil, { ifNoneMatch: true });
      return { role: 'leader', leaseUntil };
    } catch (error) {
      if (!isPreconditionFailed(error)) {
        throw error;
      }
    }

    const existing = await this.headObject(leaseKey);
    if (!existing) {
      return { role: 'follower', leaseUntil: now };
    }

    const currentExpiresAt = readEpochMillis(existing.Metadata?.[this.expiresAtMetadataKey]);
    const expired = currentExpiresAt !== null && currentExpiresAt <= now;

    if (expired && existing.ETag) {
      try {
        await this.putLeaseObject(leaseKey, owner, leaseUntil, { ifMatch: existing.ETag });
        return { role: 'leader', leaseUntil };
      } catch (error) {
        if (!isPreconditionFailed(error)) {
          throw error;
        }
      }
    }

    return { role: 'follower', leaseUntil: currentExpiresAt ?? now };
  }

  async release(key: string, owner: string): Promise<void> {
    const leaseKey = this.leaseKey(key);
    const existing = await this.headObject(leaseKey);
    if (!existing) {
      return;
    }
    if (existing.Metadata?.[this.ownerMetadataKey] !== owner) {
      return;
    }
    await this.client.send(new DeleteObjectCommand({ Bucket: this.bucket, Key: leaseKey }));
  }

  async markReady(key: string): Promise<void> {
    const ttlMs = Duration.toMillis(this.readyTtl);
    const expiresAt = this.clock.now() + Math.max(0, ttlMs);
    const metadata: Record<string, string> = {
      [this.expiresAtMetadataKey]: String(expiresAt),
    };
    await this.client.send(
      new PutObjectCommand({
        Bucket: this.bucket,
        Key: this.readyKey(key),
        Body: '1',
        ContentType: 'text/plain',
        Metadata: metadata,
      }),
    );
  }

  async isReady(key: string): Promise<LeaseReadyState> {
    const readyKey = this.readyKey(key);
    const readyHead = await this.headObject(readyKey);
    if (readyHead) {
      const readyExpiresAt = readEpochMillis(readyHead.Metadata?.[this.expiresAtMetadataKey]);
      if (readyExpiresAt === null || readyExpiresAt > this.clock.now()) {
        return { ready: true, expired: false };
      }
      await this.client.send(new DeleteObjectCommand({ Bucket: this.bucket, Key: readyKey }));
    }

    const leaseHead = await this.headObject(this.leaseKey(key));
    if (!leaseHead) {
      return { ready: false, expired: true };
    }
    const leaseExpiresAt = readEpochMillis(leaseHead.Metadata?.[this.expiresAtMetadataKey]);
    if (leaseExpiresAt !== null && leaseExpiresAt <= this.clock.now()) {
      return { ready: false, expired: true };
    }
    return { ready: false, expired: false };
  }
}

export const createS3Cache = <V>(options: S3CacheOptions<V>): Cache<V> => new S3Cache(options);
export const createS3Leases = (options: S3LeasesOptions): Leases => new S3Leases(options);
