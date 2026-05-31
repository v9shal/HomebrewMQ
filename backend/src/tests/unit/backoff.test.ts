import { backoff } from '../../utils/backoff';

describe('backoff', () => {
  it('starts above base delay', () => {
    const delay = backoff(1);
    expect(delay).toBeGreaterThanOrEqual(1000);
  });

  it('grows exponentially', () => {
    const d1 = backoff(1);
    const d2 = backoff(2);
    const d3 = backoff(3);
    expect(d2).toBeGreaterThan(d1);
    expect(d3).toBeGreaterThan(d2);
  });

  it('never exceeds cap + max jitter', () => {
    for (let i = 1; i <= 20; i++) {
      expect(backoff(i)).toBeLessThanOrEqual(31000);
    }
  });

  it('caps at 30s base before jitter', () => {
    // at attempt 10, base = min(30000, 1000*2^10) = 30000
    const samples = Array.from({ length: 100 }, () => backoff(10));
    samples.forEach(d => {
      expect(d).toBeGreaterThanOrEqual(30000);
      expect(d).toBeLessThanOrEqual(31000);
    });
  });

  it('jitter makes no two results identical', () => {
    const results = new Set(
      Array.from({ length: 50 }, () => backoff(3))
    );
    expect(results.size).toBeGreaterThan(1);
  });
});