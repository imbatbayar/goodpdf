export type PlanCode = "BASIC" | "PRO" | "BUSINESS";

export const PLANS: Array<{
  code: PlanCode;
  name: string;
  fileLimit: number;
  cpuMinutesLimit: number;
  expiryDays: number;
  priceMnt: number;
}> = [
  {
    code: "BASIC",
    name: "Basic",
    fileLimit: 30,
    cpuMinutesLimit: 60,
    expiryDays: 30,
    priceMnt: 5900,
  },
  {
    code: "PRO",
    name: "Pro",
    fileLimit: 100,
    cpuMinutesLimit: 240,
    expiryDays: 60,
    priceMnt: 9900,
  },
  {
    code: "BUSINESS",
    name: "Business",
    fileLimit: 300,
    cpuMinutesLimit: 720,
    expiryDays: 90,
    priceMnt: 19900,
  },
];
