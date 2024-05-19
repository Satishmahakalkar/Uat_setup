-- upgrade --
CREATE TABLE IF NOT EXISTS "algo" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    "name" VARCHAR(120) NOT NULL
);
CREATE TABLE IF NOT EXISTS "stock" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    "ticker" VARCHAR(20) NOT NULL UNIQUE,
    "name" VARCHAR(254),
    "isin" VARCHAR(12) NOT NULL UNIQUE,
    "is_index" INT NOT NULL  DEFAULT 0
);
CREATE TABLE IF NOT EXISTS "future" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    "expiry" DATE NOT NULL,
    "lot_size" INT NOT NULL,
    "stock_id" INT NOT NULL REFERENCES "stock" ("id") ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS "option" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    "strike" INT NOT NULL,
    "expiry" DATE NOT NULL,
    "option_type" VARCHAR(2) NOT NULL  /* CALL: CE\nPUT: PE */,
    "lot_size" INT NOT NULL,
    "stock_id" INT NOT NULL REFERENCES "stock" ("id") ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS "instrument" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    "future_id" INT REFERENCES "future" ("id") ON DELETE CASCADE,
    "option_id" INT REFERENCES "option" ("id") ON DELETE CASCADE,
    "stock_id" INT REFERENCES "stock" ("id") ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS "ltp" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    "price" REAL NOT NULL,
    "timestamp" TIMESTAMP NOT NULL  DEFAULT CURRENT_TIMESTAMP,
    "instrument_id" INT NOT NULL REFERENCES "instrument" ("id") ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS "ohlc" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    "timestamp" TIMESTAMP NOT NULL,
    "interval" VARCHAR(5) NOT NULL  /* EOD: eod\nHOUR: hour\nMIN_1: 1min\nMIN_5: 5min\nMIN_30: 30min */,
    "open" REAL NOT NULL,
    "high" REAL NOT NULL,
    "low" REAL NOT NULL,
    "close" REAL NOT NULL,
    "instrument_id" INT NOT NULL REFERENCES "instrument" ("id") ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS "stockgroup" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    "name" VARCHAR(20) NOT NULL
);
CREATE TABLE IF NOT EXISTS "stockgroupmap" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    "stock_id" INT NOT NULL REFERENCES "stock" ("id") ON DELETE CASCADE,
    "stock_group_id" INT NOT NULL REFERENCES "stockgroup" ("id") ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS "stockoldname" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    "ticker" VARCHAR(20) NOT NULL,
    "name" VARCHAR(254),
    "stock_id" INT NOT NULL REFERENCES "stock" ("id") ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS "strategy" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    "name" VARCHAR(120) NOT NULL
);
CREATE TABLE IF NOT EXISTS "user" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    "email" VARCHAR(254) NOT NULL,
    "registeration_time" TIMESTAMP NOT NULL  DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS "account" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    "start_date" DATE NOT NULL,
    "user_id" INT NOT NULL REFERENCES "user" ("id") ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS "investment" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    "amount" VARCHAR(40) NOT NULL,
    "timestamp" TIMESTAMP NOT NULL  DEFAULT CURRENT_TIMESTAMP,
    "account_id" INT NOT NULL REFERENCES "account" ("id") ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS "sreaccount" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    "account_id" INT NOT NULL UNIQUE REFERENCES "account" ("id") ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS "subscription" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    "is_hedge" INT NOT NULL  DEFAULT 0,
    "start_date" DATE NOT NULL,
    "active" INT NOT NULL  DEFAULT 1,
    "account_id" INT NOT NULL REFERENCES "account" ("id") ON DELETE CASCADE,
    "algo_id" INT NOT NULL REFERENCES "algo" ("id") ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS "pnl" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    "date" DATE NOT NULL,
    "investment" VARCHAR(40) NOT NULL,
    "unrealised_pnl" VARCHAR(40) NOT NULL,
    "realised_pnl" VARCHAR(40) NOT NULL,
    "subscription_id" INT NOT NULL REFERENCES "subscription" ("id") ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS "position" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    "qty" INT NOT NULL,
    "side" VARCHAR(4) NOT NULL  /* BUY: buy\nSELL: sell */,
    "buy_price" VARCHAR(40),
    "sell_price" VARCHAR(40),
    "eod_price" VARCHAR(40),
    "charges" VARCHAR(40) NOT NULL,
    "pnl" VARCHAR(40) NOT NULL,
    "active" INT NOT NULL  DEFAULT 1,
    "instrument_id" INT NOT NULL REFERENCES "instrument" ("id") ON DELETE CASCADE,
    "subscription_id" INT NOT NULL REFERENCES "subscription" ("id") ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS "trade" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    "timestamp" TIMESTAMP NOT NULL  DEFAULT CURRENT_TIMESTAMP,
    "side" VARCHAR(4) NOT NULL  /* BUY: buy\nSELL: sell */,
    "qty" INT NOT NULL,
    "price" VARCHAR(40) NOT NULL,
    "instrument_id" INT NOT NULL REFERENCES "instrument" ("id") ON DELETE CASCADE,
    "subscription_id" INT NOT NULL REFERENCES "subscription" ("id") ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS "sreorders" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    "timestamp" TIMESTAMP NOT NULL  DEFAULT CURRENT_TIMESTAMP,
    "app_order_id" INT NOT NULL,
    "sre_account_id" INT NOT NULL REFERENCES "sreaccount" ("id") ON DELETE CASCADE,
    "trade_id" INT NOT NULL REFERENCES "trade" ("id") ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS "tradeexit" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    "exit_trade_id" INT NOT NULL UNIQUE REFERENCES "trade" ("id") ON DELETE CASCADE,
    "position_id" INT NOT NULL UNIQUE REFERENCES "position" ("id") ON DELETE CASCADE,
    "entry_trade_id" INT NOT NULL UNIQUE REFERENCES "trade" ("id") ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS "tradesmail" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    "subject" VARCHAR(254) NOT NULL,
    "body" TEXT NOT NULL,
    "html" TEXT NOT NULL,
    "timestamp" TIMESTAMP NOT NULL  DEFAULT CURRENT_TIMESTAMP,
    "account_id" INT NOT NULL REFERENCES "account" ("id") ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS "aerich" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    "version" VARCHAR(255) NOT NULL,
    "app" VARCHAR(100) NOT NULL,
    "content" JSON NOT NULL
);