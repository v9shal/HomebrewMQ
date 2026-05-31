import Redis from 'ioredis';
import { CircuitBreaker } from '../../utils/circuitBreaker';

const redis = new Redis();
const cb = new CircuitBreaker(redis);
const Q = 'cb-unit-test';

beforeEach(async () => {
  await redis.del(
    `{homebrewmq}:cb:state:${Q}`,
    `{homebrewmq}:cb:openedAt:${Q}`,
    `{homebrewmq}:cb:success:${Q}`,
    `{homebrewmq}:cb:failure:${Q}`
  );
});

afterAll(() => redis.quit());

describe('CircuitBreaker state machine', () => {
  it('defaults to closed', async () => {
    expect(await cb.getState(Q)).toBe('closed');
  });

  it('trips to open', async () => {
    await cb.trip(Q);
    expect(await cb.getState(Q)).toBe('open');
  });

  it('stores openedAt when tripped', async () => {
    const before = Date.now();
    await cb.trip(Q);
    const openedAt = await cb.getOpenedAt(Q);
    expect(openedAt).toBeGreaterThanOrEqual(before);
    expect(openedAt).toBeLessThanOrEqual(Date.now());
  });

  it('transitions open → half-open', async () => {
    await cb.trip(Q);
    await cb.halfOpen(Q);
    expect(await cb.getState(Q)).toBe('half-open');
  });

  it('resets half-open → closed', async () => {
    await cb.trip(Q);
    await cb.halfOpen(Q);
    await cb.reset(Q);
    expect(await cb.getState(Q)).toBe('closed');
  });

  it('deletes openedAt on reset', async () => {
    await cb.trip(Q);
    await cb.reset(Q);
    expect(await cb.getOpenedAt(Q)).toBe(0);
  });

  it('returns null failure rate below MIN_SAMPLES', async () => {
    const now = Date.now();
    await redis.zadd(`{homebrewmq}:cb:failure:${Q}`, now, 'j1');
    expect(await cb.getFailureRate(Q)).toBeNull();
  });

  it('calculates failure rate correctly', async () => {
    const now = Date.now();
    for (let i = 0; i < 7; i++) {
      await redis.zadd(
        `{homebrewmq}:cb:failure:${Q}`, now, `fail-${i}`
      );
    }
    for (let i = 0; i < 3; i++) {
      await redis.zadd(
        `{homebrewmq}:cb:success:${Q}`, now, `ok-${i}`
      );
    }
    const rate = await cb.getFailureRate(Q);
    expect(rate).toBeCloseTo(0.7, 1);
  });

  it('ignores events outside 60s window', async () => {
    const old = Date.now() - 70_000;
    for (let i = 0; i < 20; i++) {
      await redis.zadd(
        `{homebrewmq}:cb:failure:${Q}`, old, `old-${i}`
      );
    }
    expect(await cb.getFailureRate(Q)).toBeNull();
  });
});