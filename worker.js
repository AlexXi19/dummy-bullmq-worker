const { Worker } = require("bullmq");
require("dotenv").config(); // Load environment variables from .env

// Load environment variables
const redisUrl = process.env.REDIS_URL;
const queueName = process.env.QUEUE_NAME;

if (!redisUrl) {
  console.error("Environment variable REDIS_URL is not set in .env file.");
  process.exit(1);
}

if (!queueName) {
  console.error("Environment variable QUEUE_NAME is not set in .env file.");
  process.exit(1);
}

// Redis connection options
const connection = {
  url: redisUrl,
};

// Worker to consume jobs
const worker = new Worker(
  queueName,
  async (job) => {
    console.log(`Processing job ${job.id} from queue ${queueName}`);
    console.log("Job data:", job.data);

    // Simulate batch processing
    try {
      // Your batch processing logic here
      console.log("Processing...");
      console.log(`Job ${job.id} processed successfully.`);
    } catch (error) {
      console.error(`Failed to process job ${job.id}:`, error);
      throw error; // Ensure the job is marked as failed
    }
  },
  { connection },
);

// Error handling
worker.on("error", (error) => {
  console.error("Worker encountered an error:", error);
});

worker.on("failed", (job, error) => {
  console.error(`Job ${job.id} failed with error:`, error);
});

worker.on("completed", (job) => {
  console.log(`Job ${job.id} completed successfully.`);
});

console.log(`Worker listening on queue: ${queueName}`);
