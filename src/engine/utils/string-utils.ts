const maxCodePoint = 0x10ffff;

function toCodePoints(str: string): number[] {
  const cps: number[] = [];
  for (let i = 0; i < str.length;) {
    const cp = str.codePointAt(i)!;
    const isBMP = cp <= 0xffff;
    cps.push(cp);
    i += isBMP ? 1 : 2;
  }
  return cps;
}

function fromCodePoints(cps: number[]): string {
  return String.fromCodePoint(...cps);
}

export function nextLexicographicString(prefix: string): string | null {
  const cps = toCodePoints(prefix);
  let i = cps.length - 1;
  while (i >= 0 && cps[i] === maxCodePoint) i--;
  if (i < 0) return null;
  cps[i]++;
  return fromCodePoints(cps.slice(0, i + 1));
}
