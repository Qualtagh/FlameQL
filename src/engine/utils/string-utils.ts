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

export function toCamelCase(str: string): string {
  // Replace separators followed by a character with the uppercase character
  const camel = str.replace(/[-_ ]+(.)/g, (_, c) => c.toUpperCase());
  // Ensure the first character is lowercase
  return camel.charAt(0).toLowerCase() + camel.slice(1);
}

export function indent(text: string, spaces: number): string {
  if (spaces < 1) return text;
  const prefix = ' '.repeat(spaces);
  return text.split('\n').map(line => line.trim().length > 0 ? `${prefix}${line}` : '').join('\n');
}

function indentBetween(text: string, spaces: number): string {
  if (spaces < 1) return text;
  const prefix = ' '.repeat(spaces);
  return text.split('\n').join(`\n${prefix}`);
}

export function align(template: TemplateStringsArray, ...values: unknown[]): string;
export function align(text: string): string;
export function align(templateOrString: TemplateStringsArray | string, ...values: unknown[]): string {
  const texts = typeof templateOrString === 'string' ? [templateOrString] : templateOrString;
  let minShift = Number.POSITIVE_INFINITY;
  for (const text of texts) {
    const lines = text.split('\n');
    const skip = 1;
    for (let i = skip; i < lines.length; i++) {
      const line = lines[i];
      const match = line.match(/^\s*/);
      const leading = match ? match[0].length : 0;
      if (leading === line.length) continue;
      minShift = Math.min(minShift, leading);
    }
  }
  let combined = '';
  for (let i = 0; i < texts.length; i++) {
    combined += texts[i];
    if (i >= values.length) continue;
    const lines = texts[i].split('\n');
    const lastLine = lines[lines.length - 1];
    const onEmpty = /^\s*$/.test(lastLine) ? '\b' : '';
    const shift = onEmpty ? lastLine.length : minShift;
    combined += indentBetween(`${values[i]}`, shift) || onEmpty;
  }
  const lines = combined.split('\n');
  const skip = lines.length > 0 && lines[0].trim().length === 0 ? 1 : 0;
  return lines.slice(skip)
    .filter(line => !line.endsWith('\b'))
    .map(line => line.slice(minShift))
    .join('\n');
}
