process.env.WORKER_PROFILE = "normal";
process.env.WORKER_ID = process.env.WORKER_ID || "worker-normal";

await import("./worker.mjs");
