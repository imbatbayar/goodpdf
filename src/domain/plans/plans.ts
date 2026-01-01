export type PlanCode = "A" | "B" | "C";

export const PLANS: Array<{ code: PlanCode; days: number; uses: number; priceMnt: number }> = [
  { code: "A", days: 10, uses: 25, priceMnt: 5500 },
  { code: "B", days: 45, uses: 75, priceMnt: 9500 },
  { code: "C", days: 100, uses: 125, priceMnt: 35000 }
];
