export const createUniqueName = (prefix: string, separator = '-'): string =>
  `${prefix}${separator}${Date.now()}${separator}${Math.random().toString(16).slice(2)}`;
