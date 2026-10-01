import pg from 'pg';
const { Client } = pg;

const client = new Client({
  connectionString: process.env.SUPABASE_URL
});

async function runMigration() {
  try {
    await client.connect();
    console.log('Connected to database');
    
    const result = await client.query('ALTER TABLE latest_prices ADD COLUMN IF NOT EXISTS price_change_24h NUMERIC;');
    console.log('✓ Migration successful: price_change_24h column added');
    
    await client.end();
    process.exit(0);
  } catch (err) {
    console.error('✗ Migration failed:', err.message);
    await client.end();
    process.exit(1);
  }
}

runMigration();
