# CECS Data Analysis Internship — Social Posts (DuckDB)

This repo has the analysis outputs from a large social posts dataset (~1.95M rows) linked to school/district entities via NCES IDs. The raw data is way too big for Excel, so everything was done in **DuckDB + SQL**, with results exported to CSV for easy sharing.

## What I did, broadly

1. **Loaded the CSV into DuckDB** (read-only base table).
2. **Cleaned up identifiers and metrics**
   - Converted `nces_id` to a numeric `BIGINT` (dropping anything invalid, blank, or `NA`).
   - Converted `likes`, `comments`, and `shares` from strings to integers.
   - Built **engagement** as:
     `engagement_n = COALESCE(likes_n,0) + COALESCE(comments_n,0) + COALESCE(shares_n,0)`
3. **Parsed timestamps** and pulled out `month = DATE_TRUNC('month', ts)`.
4. **Extracted link domains** from `url` for domain-level analysis.
5. Generated these outputs:
   - Entity summaries (who posts most / gets the most engagement)
   - Monthly trends (platform-wide and entity-level spikes)
   - Domain rankings (which linked domains are tied to more engagement)
   - "Viral" posts (top 1% by engagement)
   - "Controversy/sensitive topic" flags (keyword-based heuristic)

---

## Key metric definitions

- **likes_n / comments_n / shares_n**: numeric versions of the raw fields (string → integer, `NA`/blank → NULL).
- **engagement_n**: `likes_n + comments_n + shares_n` (NULL-safe via COALESCE).
- **viral**: posts with engagement in the **top 1%** (p99). For this run, p99 was **258**.
- **domain**: hostname pulled from `url` — used because full URLs repeat and aren't reliably unique per post.

---

## Output files

### `controversy_summary.csv`
A quick summary of how often posts get flagged as "sensitive/controversial." Built using a keyword regex against `post_text` (case-insensitive). Not a real classifier — more of a lightweight heads-up for topics worth being careful with.

### `domain_metrics.csv`
Domain-level engagement stats — how often domains show up and how engagement looks when they do. Columns typically include `domain`, `n_posts`, `total_engagement`, `avg_engagement`, and `median_engagement`. Good for seeing which linked domains (school sites, news outlets, Google Drive, etc.) tend to perform better or worse.

### `domain_ranking.csv`
A ranked list of domains by frequency and/or engagement. Useful for answering "what link destinations are most common?" and "which domains are associated with higher engagement?"

### `entity_month_spikes.csv`
Flags entity-month combinations with unusually high engagement. Good for hunting down "burst months" for specific NCES entities and digging into what content caused the spike.

### `entity_summary.csv`
Per-entity rollups — the core "leaderboard" table. Typical columns:
- `nces_id`
- `n_posts`
- `total_engagement`, `total_likes`, `total_comments`, `total_shares`
- `avg_engagement`, `median_engagement`, `p90_engagement`, `max_engagement`
- `pct_with_image`, `pct_with_link`, `pct_with_text`
- `viral_rate` (share of posts above the p99 threshold)

### `heated_discussion_posts.csv`
A slice of posts that look like "heated discussion" candidates based on engagement ratios (e.g., unusually high comments relative to likes). Meant for qualitative review — which posts attracted disproportionate commenting that might signal something contentious.

### `monthly_overall.csv`
Platform-wide time series by month. Typical columns: `month`, `n_posts`, `total_engagement`, `avg_engagement`, `median_engagement`. Good for spotting macro-trends, seasonality, and global spikes.

### `monthly_platform_metrics.csv`
A cleaned-up platform-wide monthly export, similar to `monthly_overall.csv` but potentially with extra fields depending on the query. Ready for reporting or plotting.

### `viral_posts.csv`
Posts above the p99 engagement threshold — the top ~1%. Columns typically include `nces_id`, `ts`, `month`, `domain`, `likes_n`, `comments_n`, `shares_n`, `engagement_n`, and `post_text`. Best set for case studies on what actually goes viral.

---

## Data quality notes

- **Scale**: ~1.95M raw rows, ~1.93M after dropping invalid NCES IDs.
- **Missingness**: images and links are frequently absent, so interpret "% with image/link" with that in mind.
- **Heavy tails**: a small number of entities/posts drive a huge share of total engagement. Lean on **median/percentiles** (p90/p99) rather than averages.

---

## How to reproduce

Tools needed:
- DuckDB CLI (or DuckDB Python)
- Source CSV: `processed-joined-facebook-data.csv` (large; not committed here)

```sql
-- 1) Load
CREATE OR REPLACE TABLE t AS
SELECT * FROM read_csv_auto('processed-joined-facebook-data.csv');

-- 2) Clean numeric fields + NCES + engagement
CREATE OR REPLACE VIEW t_clean AS
SELECT
  *,
  TRY_CAST(NULLIF(TRIM(nces_id), 'NA') AS BIGINT) AS nces_id_num,
  TRY_CAST(NULLIF(likes, 'NA') AS BIGINT) AS likes_n,
  TRY_CAST(NULLIF(comments, 'NA') AS BIGINT) AS comments_n,
  TRY_CAST(NULLIF(shares, 'NA') AS BIGINT) AS shares_n,
  COALESCE(TRY_CAST(NULLIF(likes, 'NA') AS BIGINT), 0)
+ COALESCE(TRY_CAST(NULLIF(comments, 'NA') AS BIGINT), 0)
+ COALESCE(TRY_CAST(NULLIF(shares, 'NA') AS BIGINT), 0) AS engagement_n
FROM t
WHERE TRY_CAST(NULLIF(TRIM(nces_id), 'NA') AS BIGINT) IS NOT NULL;

-- 3) Parse timestamps
CREATE OR REPLACE VIEW t_time AS
SELECT *, TRY_STRPTIME(time, '%Y-%m-%d %H:%M:%S') AS ts
FROM t_clean
WHERE TRY_STRPTIME(time, '%Y-%m-%d %H:%M:%S') IS NOT NULL;

-- 4) Extract domain
CREATE OR REPLACE VIEW t_domain AS
SELECT *, lower(regexp_extract(url, '^https?://([^/]+)', 1)) AS domain
FROM t_time;
```