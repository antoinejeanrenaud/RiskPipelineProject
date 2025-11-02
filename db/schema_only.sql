CREATE TABLE IF NOT EXISTS "raw_positions" (
"MATURITY" TEXT,
  "CONTRACTTYPE" TEXT,
  "BUSINESS LINE" TEXT,
  "STRATEGY" TEXT,
  "METAL" TEXT,
  "EXCHANGE" TEXT,
  "CURRENCY" TEXT,
  "LONGSHORT" TEXT,
  " VOLUME " TEXT,
  " UNIT " TEXT,
  "business_line" TEXT
);
CREATE TABLE IF NOT EXISTS "raw_prices" (
"Price Date" TEXT,
  "Maturity" TEXT,
  "QuoteValue" TEXT,
  "Metal" TEXT,
  "Exchange" TEXT,
  "Unit" TEXT
);
