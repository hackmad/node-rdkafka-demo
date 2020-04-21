export const randomInt = (n: number): number => Math.round(Math.random() * n)
export const randomItem = (a: Array<any>) => a[randomInt(a.length)]
export const randomItems = (a: Array<any>, n: number) =>
  [...Array(n).keys()].map(() => a[randomInt(a.length)])
