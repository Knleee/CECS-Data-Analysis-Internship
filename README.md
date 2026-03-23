# CECS Data Analysis Internship — Social Posts (DuckDB)

## ALERT: AI Tool Usage 
Used Claude (Anthropic) to assist with writing and debugging 
Python analysis scripts, along with refining/cleaning. All final logic, decisions, and outputs were reviewed, tested, 
and validated by me.




This repo contains the analysis outputs from a large social posts dataset (~1.95M rows) linked to school/district entities via NCES IDs. The raw data is too large for Excel, so everything was done in **DuckDB + Python**, with results exported to CSV and PNG for easy sharing.

---

## How to reproduce

### Tools needed
- Python 3
- Required packages:
  ```bash
  pip install duckdb pandas matplotlib seaborn numpy
  ```
- Source CSV: `processed-joined-facebook-data.csv` (~1.56GB) — **excluded from this repo due to file size**. Place it in `data/` locally before running.

### Running the pipeline

```bash
# Step 1 — generate all CSVs
python3 build_outputs.py

# Step 2 — generate all plots
python3 make_plots.py
```

Outputs are written to `outputs/` (CSVs) and `outputs/plots/` (PNGs).

### Testing without real data

A synthetic test dataset can be generated to validate the scripts end-to-end without needing the real CSV:

```bash
python3 make_test_data.py --test
python3 build_outputs.py --test
python3 make_plots.py --test
```

Test outputs are written to `test_outputs/` and `test_data/` — completely isolated from production files.

---

## What the pipeline does

1. **Loads the CSV into DuckDB** (in-memory, read-only base table)
2. **Cleans identifiers and metrics**
   - Converts `nces_id` to numeric `BIGINT` — drops anything invalid, blank, or `NA`
   - Converts `likes`, `comments`, and `shares` from strings to integers
   - Builds **engagement** as:
     `engagement_n = COALESCE(likes_n, 0) + COALESCE(comments_n, 0) + COALESCE(shares_n, 0)`
3. **Parses timestamps** — drops rows with unparseable times; derives `month` as a clean `DATE`
4. **Extracts link domains** from `url` for domain-level analysis
5. **Generates all output files** (see below)

---

## Key metric definitions

| Metric | Definition |
|---|---|
| `likes_n` / `comments_n` / `shares_n` | String → integer; `NA`/blank → NULL |
| `engagement_n` | `likes_n + comments_n + shares_n` (NULL-safe via COALESCE) |
| `viral` | Posts with engagement in the **top 1% (p99)**. For this run, p99 = **258** |
| `domain` | Hostname extracted from `url` — more reliable than full URLs for grouping |
| `flagged_sensitive` | 1 if `post_text` matches a keyword regex for sensitive topics, else 0 |
| `comment_like_ratio` | `comments_n / likes_n` — proxy for heated/debated content |

---

## Output files

### `analytic_base.csv`
The master analysis table — every post after cleaning and enrichment. All downstream outputs are derived from this. Columns: `nces_id`, `ts`, `month`, `likes_n`, `comments_n`, `shares_n`, `engagement_n`, `domain`, `post_text`, `has_image`. **Excluded from this repo** — at ~500MB it exceeds GitHub's file size limits and contains raw post-level data.

### `entity_summary.csv`
Per-entity rollup — the core leaderboard table, sorted by `total_engagement` descending. Columns:
- `nces_id`
- `n_posts`
- `total_engagement`, `total_likes`, `total_comments`, `total_shares`
- `avg_engagement`, `median_engagement`, `p90_engagement`, `max_engagement`
- `pct_with_image`, `pct_with_link`, `pct_with_text`

### `monthly_overall.csv`
Platform-wide time series by month. Columns: `month`, `n_posts`, `total_engagement`, `median_engagement`. Good for spotting macro-trends, seasonality, and global spikes.

### `monthly_platform_metrics.csv`
Enhanced platform-wide monthly export. Columns: `month`, `n_posts`, `total_engagement`, `avg_engagement`, `median_engagement`, `viral_posts`. Includes a per-month count of viral posts (above p99 threshold).

### `entity_month_spikes.csv`
Per-entity, per-month aggregation. Columns: `nces_id`, `month`, `n_posts`, `total_engagement`, `max_post_engagement`. Good for identifying burst months for specific schools and investigating what drove spikes.

### `domain_metrics.csv`
Domain-level engagement stats across all 6,024 domains. Columns: `domain`, `n_posts`, `total_engagement`, `avg_engagement`, `median_engagement`. Sorted by `total_engagement` descending.

### `domain_ranking.csv`
Same domains as `domain_metrics`, sorted by `n_posts` descending. Columns: `domain`, `n_posts`, `total_engagement`, `median_engagement`. Useful for answering "what link destinations are most common?"

### `viral_posts.csv`
All posts above the p99 engagement threshold (~44,494 posts). Columns: `nces_id`, `ts`, `month`, `domain`, `likes_n`, `comments_n`, `shares_n`, `engagement_n`, `post_text`. Best for case studies on what actually drives high engagement.

### `controversy_summary.csv`
Engagement comparison between sensitive-flagged and non-flagged posts. Always exactly 2 rows (`flagged_sensitive` = 0 or 1). Columns: `flagged_sensitive`, `n_posts`, `avg_engagement`, `median_engagement`, `p90_engagement`. Built using a lightweight keyword regex against `post_text` — not a real classifier.

Keywords matched: `politic`, `election`, `vote`, `protest`, `racis`, `lgbt`, `trans`, `abortion`, `gun`, `vaccine`, `mask`, `immigration`

For this run: ~60,798 flagged posts (3.2% of total). Flagged posts show modestly higher engagement across all metrics.

### `heated_discussion_posts.csv`
Top 500 posts by `comment_like_ratio` — a proxy for content that attracted disproportionate debate. Columns: `nces_id`, `ts`, `month`, `likes_n`, `comments_n`, `shares_n`, `engagement_n`, `domain`, `post_text`, `comment_like_ratio`. Intended for qualitative review.

---

## Plots (`outputs/plots/`)

Generated by `make_plots.py` from the CSV outputs above.

| File | Description |
|---|---|
| `1_engagement_distribution.png` | Log-scale histogram of avg engagement per entity + percentile bar chart (p50–p99) |
| `2_domain_mix.png` | Top 20 domains by post count and by total engagement (side by side) |
| `3_spike_timelines.png` | Overall monthly engagement trend + top 5 entities monthly breakdown |
| `4_flagged_vs_nonflagged.png` | Avg, median, and p90 engagement for flagged vs. non-flagged posts |

---

## Data quality notes

- **Scale**: ~1.95M raw rows; ~1.93M after dropping invalid NCES IDs (~19K rows dropped, <1%)
- **Missingness**: images and links are frequently absent — interpret `pct_with_image` / `pct_with_link` with that in mind
- **Heavy tails**: a small number of entities and posts drive a large share of total engagement — lean on **median and percentiles** (p90, p99) rather than averages
- **Timestamp handling**: rows with unparseable timestamps are dropped; `month` is stored as a clean `DATE` (no time component)
- **Controversy flagging**: keyword-based heuristic only — false positives and false negatives are expected