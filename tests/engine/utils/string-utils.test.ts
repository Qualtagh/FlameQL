import { align, indent } from '../../../src/engine/utils/string-utils';

describe('string-utils', () => {
  describe('align', () => {
    it('should align text', () => {
      const text = align`
        Hello
        World
      `;
      expect(text).toBe('Hello\nWorld\n');
    });
    it('should keep template strings indentation', () => {
      const dynamicText = 'dynamic 1\ndynamic 2';
      const text = align`
        Hello
          ${dynamicText}
        World
      `;
      expect(text).toBe('Hello\n  dynamic 1\n  dynamic 2\nWorld\n');
    });
    it('ignore template strings indentation on non-empty lines', () => {
      const dynamicText = 'dynamic 1\n dynamic 2';
      const text = align`
        Hello
          (${dynamicText})
        World
      `;
      expect(text).toBe('Hello\n  (dynamic 1\n dynamic 2)\nWorld\n');
    });
    it('should remove lines marked with backspace', () => {
      const text = align`
        Hello\b
        World
      `;
      expect(text).toBe('World\n');
    });
  });
  describe('indent', () => {
    it('shouldn\'t indent empty lines', () => {
      const text = align`
        Hello

        World
      `;
      expect(indent(text, 2)).toBe('  Hello\n\n  World\n');
    });
  });
});
