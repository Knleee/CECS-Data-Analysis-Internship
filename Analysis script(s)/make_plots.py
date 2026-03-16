"""
make_plots.py
-------------
Reads from the outputs/ CSVs produced by build_outputs.py and generates
four report-ready matplotlib/seaborn figures:

  1. Engagement Distribution       — entity_summary.csv
  2. Domain Mix                    — domain_metrics.csv
  3. Spike Timelines               — monthly_overall.csv + entity_month_spikes.csv
  4. Flagged vs. Non-Flagged       — controversy_summary.csv

Usage:
    python make_plots.py           # reads outputs/,      writes outputs/plots/
    python make_plots.py --test    # reads test_outputs/, writes test_outputs/plots/
"""

import os
import argparse
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns
import numpy as np


# CLI
parser = argparse.ArgumentParser()
parser.add_argument(
    "--test", action="store_true",
    help="Read from test_outputs/ and write plots to test_outputs/plots/"
)
args = parser.parse_args()

# Config

OUT_DIR   = "test_outputs" if args.test else "outputs"
PLOTS_DIR = os.path.join(OUT_DIR, "plots")
os.makedirs(PLOTS_DIR, exist_ok=True)

# Input CSVs (all produced by build_outputs.py)
ENTITY_SUMMARY      = os.path.join(OUT_DIR, "entity_summary.csv")
DOMAIN_METRICS      = os.path.join(OUT_DIR, "domain_metrics.csv")
MONTHLY_OVERALL     = os.path.join(OUT_DIR, "monthly_overall.csv")
ENTITY_MONTH_SPIKES = os.path.join(OUT_DIR, "entity_month_spikes.csv")
CONTROVERSY_SUMMARY = os.path.join(OUT_DIR, "controversy_summary.csv")

# Style
sns.set_theme(style="whitegrid", palette="muted", font_scale=1.1)
ACCENT   = "#4C72B0"
ACCENT2  = "#DD8452"
GREY     = "#aaaaaa"

# How many top entities to highlight on the spike timeline
TOP_N_ENTITIES = 5
# How many top domains to show in domain mix
TOP_N_DOMAINS  = 20


# 1. Engagement Distribution

def plot_engagement_distribution():
    df = pd.read_csv(ENTITY_SUMMARY)

    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    fig.suptitle("Engagement Distribution Across Entities", fontsize=14, fontweight="bold")

    # Left: log-scale histogram of avg_engagement per entity
    ax = axes[0]
    vals = df["avg_engagement"].dropna()
    vals_log = np.log10(vals.clip(lower=0.1))
    ax.hist(vals_log, bins=50, color=ACCENT, edgecolor="white", linewidth=0.4)
    ax.set_xlabel("Avg Engagement per Entity (log10 scale)")
    ax.set_ylabel("Number of Entities")
    ax.set_title("Distribution of Avg Engagement (log scale)")
    ax.xaxis.set_major_formatter(
        mticker.FuncFormatter(lambda x, _: f"{10**x:,.0f}")
    )

    # Right: percentile bar chart (p50, p90, max) for avg_engagement
    ax2 = axes[1]
    percentiles = [50, 75, 90, 95, 99]
    pct_vals    = [np.percentile(vals, p) for p in percentiles]
    bars = ax2.bar([f"p{p}" for p in percentiles], pct_vals, color=ACCENT, edgecolor="white")
    ax2.set_xlabel("Percentile")
    ax2.set_ylabel("Avg Engagement")
    ax2.set_title("Avg Engagement Percentiles Across Entities")
    ax2.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))
    for bar, val in zip(bars, pct_vals):
        ax2.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() * 1.02,
            f"{val:,.0f}",
            ha="center", va="bottom", fontsize=9
        )

    plt.tight_layout()
    out = os.path.join(PLOTS_DIR, "1_engagement_distribution.png")
    plt.savefig(out, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"Saved: {out}")


# 2. Domain Mix

def plot_domain_mix():
    df = pd.read_csv(DOMAIN_METRICS)

    fig, axes = plt.subplots(1, 2, figsize=(16, 6))
    fig.suptitle(f"Domain Mix — Top {TOP_N_DOMAINS} Domains", fontsize=14, fontweight="bold")

    # Left: top domains by post count
    top_posts = df.nlargest(TOP_N_DOMAINS, "n_posts")
    ax = axes[0]
    ax.barh(top_posts["domain"][::-1], top_posts["n_posts"][::-1], color=ACCENT)
    ax.set_xlabel("Number of Posts")
    ax.set_title(f"Top {TOP_N_DOMAINS} Domains by Post Count")
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))
    ax.tick_params(axis="y", labelsize=8)

    # Right: top domains by total engagement
    top_eng = df.nlargest(TOP_N_DOMAINS, "total_engagement")
    ax2 = axes[1]
    ax2.barh(top_eng["domain"][::-1], top_eng["total_engagement"][::-1], color=ACCENT2)
    ax2.set_xlabel("Total Engagement")
    ax2.set_title(f"Top {TOP_N_DOMAINS} Domains by Total Engagement")
    ax2.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))
    ax2.tick_params(axis="y", labelsize=8)

    plt.tight_layout()
    out = os.path.join(PLOTS_DIR, "2_domain_mix.png")
    plt.savefig(out, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"Saved: {out}")


# 3. Spike Timelines

def plot_spike_timelines():
    monthly  = pd.read_csv(MONTHLY_OVERALL, parse_dates=["month"])
    spikes   = pd.read_csv(ENTITY_MONTH_SPIKES, parse_dates=["month"])

    fig, axes = plt.subplots(2, 1, figsize=(14, 10))
    fig.suptitle("Engagement Spike Timelines", fontsize=14, fontweight="bold")

    # Top: overall monthly total engagement
    ax = axes[0]
    ax.plot(monthly["month"], monthly["total_engagement"], color=ACCENT, linewidth=1.8)
    ax.fill_between(monthly["month"], monthly["total_engagement"], alpha=0.15, color=ACCENT)
    ax.set_title("Overall Monthly Total Engagement")
    ax.set_ylabel("Total Engagement")
    ax.set_xlabel("")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))

    # Annotate the top spike month
    peak_idx = monthly["total_engagement"].idxmax()
    peak_row = monthly.loc[peak_idx]
    ax.annotate(
        f"Peak: {peak_row['month'].strftime('%b %Y')}\n{peak_row['total_engagement']:,.0f}",
        xy=(peak_row["month"], peak_row["total_engagement"]),
        xytext=(20, -40), textcoords="offset points",
        arrowprops=dict(arrowstyle="->", color="grey"),
        fontsize=9, color="dimgrey"
    )

    # Bottom: top N entities monthly engagement
    ax2 = axes[1]
    top_entities = (
        spikes.groupby("nces_id")["total_engagement"]
        .sum()
        .nlargest(TOP_N_ENTITIES)
        .index
        .tolist()
    )
    palette = sns.color_palette("tab10", TOP_N_ENTITIES)
    for i, eid in enumerate(top_entities):
        sub = spikes[spikes["nces_id"] == eid].sort_values("month")
        ax2.plot(
            sub["month"], sub["total_engagement"],
            label=f"NCES {eid}", color=palette[i], linewidth=1.6
        )

    ax2.set_title(f"Monthly Engagement — Top {TOP_N_ENTITIES} Entities by Total Engagement")
    ax2.set_ylabel("Total Engagement")
    ax2.set_xlabel("Month")
    ax2.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))
    ax2.legend(fontsize=8, loc="upper left")

    plt.tight_layout()
    out = os.path.join(PLOTS_DIR, "3_spike_timelines.png")
    plt.savefig(out, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"Saved: {out}")


# 4. Flagged vs. Non-Flagged

def plot_flagged_vs_nonflagged():
    df = pd.read_csv(CONTROVERSY_SUMMARY)

    # Map flag values to readable labels
    df["label"] = df["flagged_sensitive"].map({0: "Non-Flagged", 1: "Flagged (Sensitive)"})

    metrics = ["avg_engagement", "median_engagement", "p90_engagement"]
    titles  = ["Avg Engagement", "Median Engagement", "P90 Engagement"]
    colors  = [ACCENT, ACCENT2]

    fig, axes = plt.subplots(1, 3, figsize=(15, 5))
    fig.suptitle("Flagged vs. Non-Flagged Posts — Engagement Comparison",
                 fontsize=14, fontweight="bold")

    for ax, metric, title in zip(axes, metrics, titles):
        bars = ax.bar(df["label"], df[metric], color=colors, edgecolor="white", width=0.5)
        ax.set_title(title)
        ax.set_ylabel("Engagement")
        ax.set_xlabel("")
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:,.1f}"))
        for bar, val in zip(bars, df[metric]):
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                bar.get_height() * 1.02,
                f"{val:,.1f}",
                ha="center", va="bottom", fontsize=10, fontweight="bold"
            )

    # Add post count annotation below x-axis labels
    for ax in axes:
        ax.set_xticks(range(len(df)))
        ax.set_xticklabels(
            [f"{row['label']}\n(n={row['n_posts']:,})" for _, row in df.iterrows()],
            fontsize=9
        )

    plt.tight_layout()
    out = os.path.join(PLOTS_DIR, "4_flagged_vs_nonflagged.png")
    plt.savefig(out, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"Saved: {out}")


# Run all
if __name__ == "__main__":
    mode = "TEST" if args.test else "PRODUCTION"
    print(f"[{mode}] Reading CSVs from: {OUT_DIR}/")
    plot_engagement_distribution()
    plot_domain_mix()
    plot_spike_timelines()
    plot_flagged_vs_nonflagged()
    print(f"\n[{mode}] All plots saved to: {PLOTS_DIR}/")