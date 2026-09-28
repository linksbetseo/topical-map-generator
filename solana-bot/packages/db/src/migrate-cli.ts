import { createPool, migrate } from "./pool.ts";

const url = process.env.DATABASE_URL;
if (!url) {
  console.error("DATABASE_URL is required");
  process.exit(1);
}
const pool = createPool(url, 1);
try {
  const applied = await migrate(pool);
  console.log(applied.length ? `applied: ${applied.join(", ")}` : "schema up to date");
} finally {
  await pool.end();
}
