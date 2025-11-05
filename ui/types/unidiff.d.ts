declare module 'unidiff' {
  export function diffLines(oldStr: string, newStr: string): any;
  export function formatLines(diff: any, options?: any): string;
}
