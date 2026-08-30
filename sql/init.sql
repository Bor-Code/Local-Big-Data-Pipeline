DROP TABLE IF EXISTS daily_summary;
CREATE TABLE daily_summary (
    pickup_date DATE,
    total_trips BIGINT,
    avg_distance DOUBLE PRECISION
);

DROP TABLE IF EXISTS hourly_revenue;
CREATE TABLE hourly_revenue (
    pickup_hour INTEGER,
    total_revenue DOUBLE PRECISION
);