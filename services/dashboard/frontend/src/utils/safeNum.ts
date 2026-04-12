/**
 * Safe number formatting — prevents "Cannot read properties of
 * undefined (reading 'toFixed')" crashes across the entire dashboard.
 *
 * Every component should use these instead of raw .toFixed() / .toLocaleString().
 */

/** Safe toFixed — returns "—" for null/undefined/NaN, otherwise n.toFixed(d). */
export function sf(n: number | null | undefined, d = 2): string {
  if (n == null || !Number.isFinite(n)) return "—";
  return n.toFixed(d);
}

/** Safe toLocaleString with max fraction digits. */
export function sn(n: number | null | undefined, d = 0): string {
  if (n == null || !Number.isFinite(n)) return "—";
  return n.toLocaleString(undefined, {
    maximumFractionDigits: d,
    minimumFractionDigits: d,
  });
}

/** Safe number — returns 0 for null/undefined, otherwise the number. */
export function n0(n: number | null | undefined): number {
  return n ?? 0;
}
