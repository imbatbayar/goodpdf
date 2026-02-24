process.env.WORKER_PROFILE = "heavy";
process.env.WORKER_ID = process.env.WORKER_ID || "worker-heavy";

await import("./worker.mjs");
