import os
import argparse
import duckdb


# CLI

parser = argparse.ArgumentParser()
parser.add_argument(
    "--test", action="store_true",
    help="Read from test_data/ and write outputs to test_outputs/ instead of production folders"
)
args = parser.parse_args()

# Config

DATA_CSV = os.path.join(
    "test_data" if args.test else "data",
    "processed-joined-facebook-data.csv"
)
OUT_DIR = "test_outputs" if args.test else "outputs"

# Output file names
OUT_ANALYTIC_BASE         = os.path.join(OUT_DIR, "analytic_base.csv")
OUT_ENTITY_SUMMARY        = os.path.join(OUT_DIR, "entity_summary.csv")
OUT_MONTHLY_PLATFORM      = os.path.join(OUT_DIR, "monthly_platform_metrics.csv")
OUT_MONTHLY_OVERALL       = os.path.join(OUT_DIR, "monthly_overall.csv")
OUT_ENTITY_MONTH_SPIKES   = os.path.join(OUT_DIR, "entity_month_spikes.csv")
OUT_DOMAIN_METRICS        = os.path.join(OUT_DIR, "domain_metrics.csv")
OUT_DOMAIN_RANKING        = os.path.join(OUT_DIR, "domain_ranking.csv")
OUT_VIRAL_POSTS           = os.path.join(OUT_DIR, "viral_posts.csv")
OUT_CONTROVERSY_SUMMARY   = os.path.join(OUT_DIR, "controversy_summary.csv")
OUT_HEATED_DISCUSSION     = os.path.join(OUT_DIR, "heated_discussion_posts.csv")

# Regex for sensitive/controversial topics
SENSITIVE_REGEX = r"(politic|election|vote|protest|racis|lgbt|trans|abortion|gun|vaccine|mask|immigration)"

# Helpers

def ensure_dirs():
    os.makedirs(OUT_DIR, exist_ok=True)

def run(con, sql):
    con.execute(sql)

def export_csv(con, sql, out_path):
    con.execute(f"COPY ({sql}) TO '{out_path}' (HEADER, DELIMITER ',');")

# Main pipeline

def main():
    ensure_dirs()

    if not os.path.exists(DATA_CSV):
        raise FileNotFoundError(
            f"Missing source CSV at: {DATA_CSV}\n"
            "Place processed-joined-facebook-data.csv into ./data/"
        )

    con = duckdb.connect(database=":memory:")

    # 1) Load raw CSV
    run(con, f"""
    CREATE OR REPLACE TABLE t AS
    SELECT * FROM read_csv_auto('{DATA_CSV}');
    """)

    # 2) Clean NCES + cast numeric engagement fields; filter invalid NCES
    run(con, """
    CREATE OR REPLACE VIEW t_clean AS
    SELECT
      *,
      NULLIF(TRIM(nces_id), '') AS nces_raw,
      TRY_CAST(NULLIF(TRIM(nces_id), 'NA') AS BIGINT) AS nces_id_num,

      TRY_CAST(NULLIF(likes,    'NA') AS BIGINT) AS likes_n,
      TRY_CAST(NULLIF(shares,   'NA') AS BIGINT) AS shares_n,
      TRY_CAST(NULLIF(comments, 'NA') AS BIGINT) AS comments_n,

      COALESCE(TRY_CAST(NULLIF(likes,    'NA') AS BIGINT), 0)
      + COALESCE(TRY_CAST(NULLIF(comments,'NA') AS BIGINT), 0)
      + COALESCE(TRY_CAST(NULLIF(shares,  'NA') AS BIGINT), 0) AS engagement_n
    FROM t
    WHERE TRY_CAST(NULLIF(TRIM(nces_id), 'NA') AS BIGINT) IS NOT NULL;
    """)

    # 3) Parse timestamp — drop rows with unparseable times
    run(con, """
    CREATE OR REPLACE VIEW t_time AS
    SELECT
      *,
      TRY_STRPTIME(CAST(time AS VARCHAR), '%Y-%m-%d %H:%M:%S') AS ts
    FROM t_clean
    WHERE TRY_STRPTIME(CAST(time AS VARCHAR), '%Y-%m-%d %H:%M:%S') IS NOT NULL;
    """)

    # 4) Extract domain from URL
    run(con, """
    CREATE OR REPLACE VIEW t_domain AS
    SELECT
      *,
      lower(regexp_extract(url, '^https?://([^/]+)', 1)) AS domain
    FROM t_time;
    """)

    # 5) Analytic base — master analysis table
    run(con, """
    CREATE OR REPLACE TABLE analytic_base AS
    SELECT
      nces_id_num AS nces_id,
      ts,
      CAST(DATE_TRUNC('month', ts) AS DATE) AS month,
      likes_n,
      comments_n,
      shares_n,
      engagement_n,
      domain,
      post_text,
      -- image flag: treat non-null, non-empty, non-'NA' image column as having an image
      CASE
        WHEN image IS NULL OR TRIM(image) IN ('', 'NA') THEN 0
        ELSE 1
      END AS has_image
    FROM t_domain;
    """)

    export_csv(con, "SELECT * FROM analytic_base", OUT_ANALYTIC_BASE)

    # 6) Entity summary
    # Includes pct_with_image; viral_rate based on global p99 of engagement_n
    run(con, """
    CREATE OR REPLACE TABLE entity_summary AS
    WITH p AS (
      SELECT quantile_cont(engagement_n, 0.99) AS p99
      FROM analytic_base
    )
    SELECT
      nces_id,
      COUNT(*)                  AS n_posts,

      SUM(engagement_n)         AS total_engagement,
      SUM(likes_n)              AS total_likes,
      SUM(comments_n)           AS total_comments,
      SUM(shares_n)             AS total_shares,

      AVG(engagement_n)                            AS avg_engagement,
      quantile_cont(engagement_n, 0.50)            AS median_engagement,
      quantile_cont(engagement_n, 0.90)            AS p90_engagement,
      MAX(engagement_n)                            AS max_engagement,

      AVG(has_image)                               AS pct_with_image,
      AVG(CASE WHEN domain IS NULL OR domain = ''
               THEN 0 ELSE 1 END)                  AS pct_with_link,
      AVG(CASE WHEN post_text IS NULL
                 OR post_text IN ('', 'NA')
               THEN 0 ELSE 1 END)                  AS pct_with_text
    FROM analytic_base
    GROUP BY 1;
    """)

    export_csv(con,
        "SELECT * FROM entity_summary ORDER BY total_engagement DESC",
        OUT_ENTITY_SUMMARY)

    # 7) Monthly overall — month, n_posts, total_engagement, median_engagement only
    run(con, """
    CREATE OR REPLACE TABLE monthly_overall AS
    SELECT
      month,
      COUNT(*)                          AS n_posts,
      SUM(engagement_n)                 AS total_engagement,
      quantile_cont(engagement_n, 0.50) AS median_engagement
    FROM analytic_base
    GROUP BY 1
    ORDER BY 1;
    """)

    export_csv(con, "SELECT * FROM monthly_overall", OUT_MONTHLY_OVERALL)

    # 8) Monthly platform metrics — distinct from monthly_overall;
    #    includes avg_engagement and viral_posts count per month
    run(con, """
    CREATE OR REPLACE TABLE monthly_platform_metrics AS
    WITH p AS (
      SELECT quantile_cont(engagement_n, 0.99) AS p99
      FROM analytic_base
    )
    SELECT
      month,
      COUNT(*)                          AS n_posts,
      SUM(engagement_n)                 AS total_engagement,
      AVG(engagement_n)                 AS avg_engagement,
      quantile_cont(engagement_n, 0.50) AS median_engagement,
      COUNT(*) FILTER (
        WHERE engagement_n >= (SELECT p99 FROM p)
      )                                 AS viral_posts
    FROM analytic_base
    GROUP BY 1
    ORDER BY 1;
    """)

    export_csv(con, "SELECT * FROM monthly_platform_metrics", OUT_MONTHLY_PLATFORM)

    # 9) Entity-month spikes
    # Outputs: nces_id, month, n_posts, total_engagement, max_post_engagement
    run(con, """
    CREATE OR REPLACE TABLE entity_month_spikes AS
    SELECT
      nces_id,
      month,
      COUNT(*)              AS n_posts,
      SUM(engagement_n)     AS total_engagement,
      MAX(engagement_n)     AS max_post_engagement
    FROM analytic_base
    GROUP BY 1, 2
    ORDER BY total_engagement DESC;
    """)

    export_csv(con, "SELECT * FROM entity_month_spikes", OUT_ENTITY_MONTH_SPIKES)

    # 10) Domain metrics — frequency + engagement by domain
    run(con, """
    CREATE OR REPLACE TABLE domain_metrics AS
    SELECT
      domain,
      COUNT(*)                          AS n_posts,
      SUM(engagement_n)                 AS total_engagement,
      AVG(engagement_n)                 AS avg_engagement,
      quantile_cont(engagement_n, 0.50) AS median_engagement
    FROM analytic_base
    WHERE domain IS NOT NULL AND domain <> ''
    GROUP BY 1
    ORDER BY total_engagement DESC;
    """)

    export_csv(con, "SELECT * FROM domain_metrics", OUT_DOMAIN_METRICS)

    # domain_ranking drops avg_engagement, ordered by n_posts
    export_csv(con,
        "SELECT domain, n_posts, total_engagement, median_engagement "
        "FROM domain_metrics ORDER BY n_posts DESC",
        OUT_DOMAIN_RANKING)

    # 11) Viral posts (global p99 threshold)
    run(con, """
    CREATE OR REPLACE TABLE viral_posts AS
    WITH p AS (
      SELECT quantile_cont(engagement_n, 0.99) AS p99
      FROM analytic_base
    )
    SELECT
      nces_id,
      ts,
      month,
      domain,
      likes_n,
      comments_n,
      shares_n,
      engagement_n,
      post_text
    FROM analytic_base, p
    WHERE engagement_n >= p.p99
    ORDER BY engagement_n DESC;
    """)

    export_csv(con, "SELECT * FROM viral_posts", OUT_VIRAL_POSTS)

    # 12) Controversy flagging
    # Uses regexp_matches (boolean) for DuckDB compatibility
    run(con, f"""
    CREATE OR REPLACE TABLE controversy_flags AS
    SELECT
      nces_id,
      ts,
      domain,
      engagement_n,
      post_text,
      CASE
        WHEN post_text IS NULL OR post_text IN ('', 'NA') THEN 0
        WHEN regexp_matches(lower(post_text), '{SENSITIVE_REGEX}') THEN 1
        ELSE 0
      END AS flagged_sensitive
    FROM analytic_base;
    """)

    # controversy_summary exports the per-flag breakdown (matches expected output schema:
    # flagged_sensitive, n_posts, avg_engagement, median_engagement, p90_engagement)
    run(con, """
    CREATE OR REPLACE TABLE controversy_by_flag AS
    SELECT
      flagged_sensitive,
      COUNT(*)                          AS n_posts,
      AVG(engagement_n)                 AS avg_engagement,
      quantile_cont(engagement_n, 0.50) AS median_engagement,
      quantile_cont(engagement_n, 0.90) AS p90_engagement
    FROM controversy_flags
    GROUP BY 1
    ORDER BY 1;
    """)

    export_csv(con, "SELECT * FROM controversy_by_flag", OUT_CONTROVERSY_SUMMARY)

    # 13) Heated discussion posts (high comment-to-like ratio)
    # ROW_NUMBER() enforces a hard 500 row cap even when multiple rows share
    # the same comment_like_ratio at the cutoff boundary
    run(con, """
    CREATE OR REPLACE TABLE heated_discussion_posts AS
    SELECT
      nces_id,
      ts,
      month,
      likes_n,
      comments_n,
      shares_n,
      engagement_n,
      domain,
      post_text,
      comment_like_ratio
    FROM (
      SELECT
        nces_id,
        ts,
        month,
        likes_n,
        comments_n,
        shares_n,
        engagement_n,
        domain,
        post_text,
        CASE
          WHEN likes_n IS NULL OR likes_n = 0 THEN NULL
          ELSE (comments_n * 1.0 / likes_n)
        END AS comment_like_ratio,
        ROW_NUMBER() OVER (
          ORDER BY
            CASE WHEN likes_n IS NULL OR likes_n = 0
                 THEN NULL
                 ELSE (comments_n * 1.0 / likes_n)
            END DESC NULLS LAST
        ) AS rn
      FROM analytic_base
      WHERE comments_n IS NOT NULL
    ) ranked
    WHERE rn <= 500;
    """)

    export_csv(con, "SELECT * FROM heated_discussion_posts", OUT_HEATED_DISCUSSION)

    # Run summary
    n_rows  = con.execute("SELECT COUNT(*) FROM t").fetchone()[0]
    n_clean = con.execute("SELECT COUNT(*) FROM t_clean").fetchone()[0]
    mode = "TEST" if args.test else "PRODUCTION"
    print(f"[{mode}] Loaded rows in t:                      {n_rows}")
    print(f"[{mode}] Rows after valid NCES filter (t_clean): {n_clean}")
    print(f"[{mode}] Wrote outputs to: {OUT_DIR}/")

if __name__ == "__main__":
    main()