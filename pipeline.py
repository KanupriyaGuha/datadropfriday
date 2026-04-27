"""
DataDropFriday — Weekly Music Trend Pipeline
=============================================
Sources:
  - Apple Music Charts  (RSS, no auth)
  - Spotify Global      (kworb.net, no auth)
  - Spotify US          (kworb.net, breakout signal)
  - Billboard Hot 100   (billboard.py, no auth)   ← Phase 2
  - Google Trends       (pytrends, enrichment)    ← Phase 2

Run every Thursday night:
  python pipeline.py

Outputs (in outputs/ folder):
  - weekly_report_YYYY-MM-DD.json
  - top_songs_YYYY-MM-DD.png
  - top_artists_YYYY-MM-DD.png
  - weekly_summary_YYYY-MM-DD.png
  - raw data in data/raw_YYYY-MM-DD.csv
"""

import matplotlib
matplotlib.use("Agg")  # headless backend — required for GitHub Actions (no display)

import requests
import pandas as pd
import json
import os
import shutil
import time
from bs4 import BeautifulSoup
from datetime import datetime

# ─────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────

OUTPUT_DIR = "outputs"
DATA_DIR   = "data"
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(DATA_DIR, exist_ok=True)

TODAY = datetime.now().strftime("%Y-%m-%d")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# Number of chart sources (used for source coverage display)
CHART_SOURCES = ["apple_music", "spotify_global", "spotify_us", "billboard"]


# ─────────────────────────────────────────
# HELPER — parse kworb table
# ─────────────────────────────────────────

def parse_kworb_table(html: str, source: str, limit: int = 50) -> pd.DataFrame:
    """
    Parses any kworb Spotify chart table.
    Column structure: Pos | P+ | Artist and Title | Days | Pk | (x?) | Streams | ...
    Artist and Title format: "Artist Name - Song Title"
    """
    soup  = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    if not table:
        return pd.DataFrame()

    rows = []
    for i, row in enumerate(table.find_all("tr")[1:limit + 1]):
        cols = row.find_all("td")
        if len(cols) < 3:
            continue
        try:
            artist_title = cols[2].get_text(separator=" ", strip=True)

            if " - " in artist_title:
                parts  = artist_title.split(" - ", 1)
                artist = parts[0].strip()
                title  = parts[1].strip()
            else:
                artist = artist_title.strip()
                title  = artist_title.strip()

            streams = 0
            if len(cols) > 6:
                s = cols[6].get_text(strip=True).replace(",", "")
                streams = int(s) if s.isdigit() else 0

            rows.append({
                "rank":       i + 1,
                "title":      title,
                "artist":     artist,
                "album":      "",
                "genre":      "",
                "streams":    streams,
                "source":     source,
                "fetched_at": TODAY,
            })
        except Exception:
            continue

    return pd.DataFrame(rows)


# ─────────────────────────────────────────
# SOURCE 1 — APPLE MUSIC
# ─────────────────────────────────────────

def fetch_apple_music_charts(limit=50, country="us") -> pd.DataFrame:
    url = (
        f"https://rss.applemarketingtools.com/api/v2/"
        f"{country}/music/most-played/{limit}/songs.json"
    )
    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        r.raise_for_status()
        results = r.json().get("feed", {}).get("results", [])
        rows = []
        for i, item in enumerate(results):
            rows.append({
                "rank":       i + 1,
                "title":      item.get("name", ""),
                "artist":     item.get("artistName", ""),
                "album":      item.get("albumName", ""),
                "genre":      item.get("genres", [{}])[0].get("name", "")
                              if item.get("genres") else "",
                "streams":    0,
                "source":     "apple_music",
                "fetched_at": TODAY,
            })
        df = pd.DataFrame(rows)
        print(f"✅ Apple Music: {len(df)} tracks fetched")
        return df
    except Exception as e:
        print(f"❌ Apple Music fetch failed: {e}")
        return pd.DataFrame()


# ─────────────────────────────────────────
# SOURCE 2 — SPOTIFY GLOBAL DAILY (kworb)
# ─────────────────────────────────────────

def fetch_spotify_global(limit=50) -> pd.DataFrame:
    url = "https://kworb.net/spotify/country/global_daily.html"
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        df = parse_kworb_table(r.text, source="spotify_global", limit=limit)
        if df.empty:
            print("❌ Spotify Global: no data parsed")
            return pd.DataFrame()
        print(f"✅ Spotify Global (kworb): {len(df)} tracks fetched")
        return df
    except Exception as e:
        print(f"❌ Spotify Global fetch failed: {e}")
        return pd.DataFrame()


# ─────────────────────────────────────────
# SOURCE 3 — SPOTIFY US DAILY (kworb)
# US chart vs Global = breakout signal
# ─────────────────────────────────────────

def fetch_spotify_us(limit=50) -> pd.DataFrame:
    url = "https://kworb.net/spotify/country/us_daily.html"
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        df = parse_kworb_table(r.text, source="spotify_us", limit=limit)
        if df.empty:
            print("❌ Spotify US: no data parsed")
            return pd.DataFrame()
        print(f"✅ Spotify US (kworb): {len(df)} tracks fetched")
        return df
    except Exception as e:
        print(f"❌ Spotify US fetch failed: {e}")
        return pd.DataFrame()


# ─────────────────────────────────────────
# SOURCE 4 — BILLBOARD HOT 100
# ─────────────────────────────────────────

def fetch_billboard_hot100(limit=50) -> pd.DataFrame:
    """
    Fetches the Billboard Hot 100 via the billboard.py library.
    Install: pip install billboard.py
    """
    try:
        import billboard
        chart = billboard.ChartData("hot-100")
        rows = []
        for i, entry in enumerate(chart[:limit]):
            rows.append({
                "rank":       i + 1,
                "title":      entry.title,
                "artist":     entry.artist,
                "album":      "",
                "genre":      "",
                "streams":    0,
                "source":     "billboard",
                "fetched_at": TODAY,
            })
        df = pd.DataFrame(rows)
        print(f"✅ Billboard Hot 100: {len(df)} tracks fetched")
        return df
    except ImportError:
        print("❌ Billboard: billboard.py not installed. Run: pip install billboard.py")
        return pd.DataFrame()
    except Exception as e:
        print(f"❌ Billboard fetch failed: {e}")
        return pd.DataFrame()


# ─────────────────────────────────────────
# SOURCE 5 — GOOGLE TRENDS (enrichment)
# Not a ranked chart — enriches top picks
# with real-world search interest signal
# ─────────────────────────────────────────

def fetch_google_trends(top_songs: list, timeframe="now 7-d") -> dict:
    """
    Takes a list of "Title — Artist" strings (top songs from chart ranking).
    Returns a dict: { "Title — Artist": interest_score (0–100) }

    Uses pytrends to query Google search interest over the past 7 days.
    Processes in batches of 5 (pytrends API limit).
    Category 35 = Music on Google Trends.

    Install: pip install pytrends
    """
    try:
        from pytrends.request import TrendReq
    except ImportError:
        print("❌ Google Trends: pytrends not installed. Run: pip install pytrends")
        return {}

    if not top_songs:
        return {}

    print(f"   Querying Google Trends for top {len(top_songs)} songs...")
    pytrends = TrendReq(hl="en-US", tz=300)
    scores   = {}

    # pytrends allows max 5 keywords per request
    for i in range(0, len(top_songs), 5):
        batch = top_songs[i : i + 5]
        try:
            pytrends.build_payload(
                batch,
                cat=35,            # Music category
                timeframe=timeframe,
                geo="US",
            )
            df = pytrends.interest_over_time()
            if not df.empty:
                for kw in batch:
                    if kw in df.columns:
                        scores[kw] = int(df[kw].mean())
                    else:
                        scores[kw] = 0
            else:
                for kw in batch:
                    scores[kw] = 0
            time.sleep(1.5)  # respect rate limits
        except Exception as e:
            print(f"   ⚠️  Google Trends batch {i//5 + 1} failed: {e}")
            for kw in batch:
                scores[kw] = 0

    fetched = sum(1 for v in scores.values() if v > 0)
    print(f"✅ Google Trends: interest scores for {fetched}/{len(top_songs)} songs")
    return scores


# ─────────────────────────────────────────
# NORMALIZER
# ─────────────────────────────────────────

def normalize_and_score(dfs: list) -> pd.DataFrame:
    all_rows = []
    for df in dfs:
        if df.empty:
            continue
        df = df.copy()
        max_rank    = df["rank"].max()
        df["score"] = ((max_rank - df["rank"] + 1) / max_rank) * 100
        all_rows.append(df)

    if not all_rows:
        print("⚠️  No data to score")
        return pd.DataFrame()

    combined = pd.concat(all_rows, ignore_index=True)
    combined["key"] = (
        combined["title"].str.lower().str.strip() + " — " +
        combined["artist"].str.lower().str.strip()
    )
    return combined


# ─────────────────────────────────────────
# SCORER
# ─────────────────────────────────────────

def compute_weekly_rankings(combined: pd.DataFrame, trends: dict = None) -> dict:
    if combined.empty:
        return {}

    total_chart_sources = combined["source"].nunique()

    # Top Songs — combined score across all chart sources
    song_scores = (
        combined.groupby(["key", "title", "artist"])["score"]
        .agg(["sum", "count"])
        .reset_index()
        .rename(columns={"sum": "total_score", "count": "source_count"})
        .sort_values("total_score", ascending=False)
    )

    # Add source coverage string, e.g. "3/4 sources"
    song_scores["coverage"] = (
        song_scores["source_count"].astype(str) + "/" +
        str(total_chart_sources) + " sources"
    )

    # Add Google Trends interest score if available
    if trends:
        def get_trend_score(row):
            # Try exact key match, then partial
            key = row["title"].lower().strip() + " — " + row["artist"].lower().strip()
            for trend_key, score in trends.items():
                if trend_key.lower() in key or key in trend_key.lower():
                    return score
            return None
        song_scores["search_interest"] = song_scores.apply(get_trend_score, axis=1)
    else:
        song_scores["search_interest"] = None

    # Top Artists
    artist_scores = (
        combined[combined["artist"] != "Unknown"]
        .groupby("artist")["score"]
        .agg(["sum", "count"])
        .reset_index()
        .rename(columns={"sum": "total_score", "count": "appearances"})
        .sort_values("total_score", ascending=False)
    )

    # Top Genres — Apple Music provides this
    genre_scores = pd.DataFrame()
    if "genre" in combined.columns:
        genre_df = combined[
            combined["genre"].notna() & (combined["genre"] != "")
        ]
        if not genre_df.empty:
            genre_scores = (
                genre_df.groupby("genre")["score"]
                .agg(["sum", "count"])
                .reset_index()
                .rename(columns={"sum": "total_score", "count": "track_count"})
                .sort_values("total_score", ascending=False)
            )

    # Breakout Picks — in US chart but NOT in Global chart
    us      = combined[combined["source"] == "spotify_us"].copy()
    global_ = combined[combined["source"] == "spotify_global"].copy()

    breakout = pd.DataFrame()
    if not us.empty and not global_.empty:
        us_keys       = set(us["key"].str.lower())
        global_keys   = set(global_["key"].str.lower())
        breakout_keys = us_keys - global_keys
        breakout = us[
            us["key"].str.lower().isin(breakout_keys)
        ].sort_values("score", ascending=False)

    return {
        "week_of":           TODAY,
        "chart_sources_used": total_chart_sources,
        "top_songs":         song_scores.head(10).to_dict("records"),
        "top_artists":       artist_scores.head(10).to_dict("records"),
        "top_genres":        genre_scores.head(5).to_dict("records")
                             if not genre_scores.empty else [],
        "breakout_picks":    breakout.head(5)[["title", "artist", "score"]]
                             .to_dict("records") if not breakout.empty else [],
        "song_of_week":      song_scores.iloc[0].to_dict()
                             if not song_scores.empty else {},
        "artist_of_week":    artist_scores.iloc[0].to_dict()
                             if not artist_scores.empty else {},
        "genre_of_week":     genre_scores.iloc[0].to_dict()
                             if not genre_scores.empty else {},
        "one_to_watch":      breakout.iloc[0][["title", "artist", "score"]]
                             .to_dict() if not breakout.empty else {},
    }


# ─────────────────────────────────────────
# VISUALIZER
# ─────────────────────────────────────────

def generate_visualizations(report: dict):
    import matplotlib.pyplot as plt

    BG     = "#F0EDEA"
    TEXT   = "#0D0D0D"
    SUBTLE = "#888888"

    plt.rcParams.update({
        "font.family":        "sans-serif",
        "font.size":          12,
        "axes.spines.top":    False,
        "axes.spines.right":  False,
        "axes.spines.left":   False,
        "axes.spines.bottom": False,
        "figure.facecolor":   BG,
        "axes.facecolor":     BG,
        "text.color":         TEXT,
        "axes.labelcolor":    TEXT,
        "xtick.color":        TEXT,
        "ytick.color":        TEXT,
    })

    n_sources = report.get("chart_sources_used", 3)

    # Chart 1 — Top 10 Songs
    if report.get("top_songs"):
        songs  = report["top_songs"][:10]
        labels = [
            f"{s['artist']} — {s['title']}"[:45] +
            f"  [{s.get('source_count', '?')}/{n_sources}]"
            for s in reversed(songs)
        ]
        scores = [round(s["total_score"], 1) for s in reversed(songs)]

        fig, ax = plt.subplots(figsize=(10, 7))
        bars = ax.barh(labels, scores, color=TEXT, height=0.6)
        for bar, score in zip(bars, scores):
            ax.text(bar.get_width() + 1, bar.get_y() + bar.get_height() / 2,
                    f"{score:.0f}", va="center", ha="left",
                    fontsize=10, color=SUBTLE)
        ax.set_xticks([])
        ax.set_title("TOP 10 SONGS THIS WEEK", fontsize=16,
                     fontweight="bold", pad=20, loc="left")
        ax.text(0, -0.08, f"datadropfriday · {TODAY}  ·  [{n_sources} sources]",
                transform=ax.transAxes, fontsize=9, color=SUBTLE)
        plt.tight_layout()
        path = f"{OUTPUT_DIR}/top_songs_{TODAY}.png"
        plt.savefig(path, dpi=180, bbox_inches="tight", facecolor=BG)
        plt.close()
        shutil.copy(path, f"{OUTPUT_DIR}/top_songs_latest.png")
        print(f"✅ Saved: {path}")

    # Chart 2 — Top Artists
    if report.get("top_artists"):
        artists = report["top_artists"][:8]
        names   = [a["artist"][:30] for a in reversed(artists)]
        scores  = [round(a["total_score"], 1) for a in reversed(artists)]

        fig, ax = plt.subplots(figsize=(10, 6))
        bars = ax.barh(names, scores, color=TEXT, height=0.55)
        for bar, score in zip(bars, scores):
            ax.text(bar.get_width() + 1, bar.get_y() + bar.get_height() / 2,
                    f"{score:.0f}", va="center", ha="left",
                    fontsize=10, color=SUBTLE)
        ax.set_xticks([])
        ax.set_title("TOP ARTISTS THIS WEEK", fontsize=16,
                     fontweight="bold", pad=20, loc="left")
        ax.text(0, -0.1, f"datadropfriday · {TODAY}",
                transform=ax.transAxes, fontsize=9, color=SUBTLE)
        plt.tight_layout()
        path = f"{OUTPUT_DIR}/top_artists_{TODAY}.png"
        plt.savefig(path, dpi=180, bbox_inches="tight", facecolor=BG)
        plt.close()
        shutil.copy(path, f"{OUTPUT_DIR}/top_artists_latest.png")
        print(f"✅ Saved: {path}")

    # Chart 3 — Weekly Summary Card
    fig, ax = plt.subplots(figsize=(8, 8))
    ax.axis("off")

    # Header
    ax.text(0.5, 0.95, "DATA DROP FRIDAY", ha="center", va="top",
            fontsize=22, fontweight="bold", color=TEXT,
            transform=ax.transAxes)
    ax.text(0.5, 0.88, f"Week of {TODAY}", ha="center", va="top",
            fontsize=12, color=SUBTLE, transform=ax.transAxes)
    ax.axhline(y=0.84, xmin=0.05, xmax=0.95, color=TEXT, linewidth=1)

    # Song of the Week — include coverage + search interest if available
    sow       = report.get("song_of_week", {})
    sow_title = sow.get("title", "—")
    sow_artist = sow.get("artist", "")
    sow_coverage = sow.get("coverage", "")
    sow_interest = sow.get("search_interest")

    sow_subtitle = sow_coverage
    if sow_interest is not None:
        sow_subtitle += f"  ·  {sow_interest}/100 search interest"

    sections = [
        ("SONG OF THE WEEK",
         f"{sow_title}\n{sow_artist}",
         sow_subtitle,
         0.76),
        ("ARTIST OF THE WEEK",
         report.get("artist_of_week", {}).get("artist", "—"),
         "",
         0.56),
        ("GENRE OF THE WEEK",
         report.get("genre_of_week", {}).get("genre", "—"),
         "",
         0.40),
        ("ONE TO WATCH",
         report.get("one_to_watch", {}).get("title", "—") + "\n" +
         report.get("one_to_watch", {}).get("artist", ""),
         "US trending · not yet global",
         0.24),
    ]

    for label, value, subtitle, y in sections:
        ax.text(0.08, y + 0.06, label, ha="left", va="top",
                fontsize=10, fontweight="bold", color=SUBTLE,
                transform=ax.transAxes)
        ax.text(0.08, y, value, ha="left", va="top",
                fontsize=15, fontweight="bold", color=TEXT,
                transform=ax.transAxes)
        if subtitle:
            ax.text(0.08, y - 0.075, subtitle, ha="left", va="top",
                    fontsize=9, color=SUBTLE, transform=ax.transAxes)
        ax.axhline(y=y - 0.10, xmin=0.05, xmax=0.95,
                   color=SUBTLE, linewidth=0.4, alpha=0.5)

    # Footer
    ax.text(0.5, 0.04,
            f"@datadropfriday  ·  Music trends. By numbers.  ·  {n_sources} sources",
            ha="center", va="bottom", fontsize=9, color=SUBTLE,
            transform=ax.transAxes)

    plt.tight_layout()
    path = f"{OUTPUT_DIR}/weekly_summary_{TODAY}.png"
    plt.savefig(path, dpi=180, bbox_inches="tight", facecolor=BG)
    plt.close()
    shutil.copy(path, f"{OUTPUT_DIR}/weekly_summary_latest.png")
    print(f"✅ Saved: {path}")


# ─────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────

def run_pipeline():
    print("\n DataDropFriday Pipeline Starting...")
    print(f"   Week of {TODAY}\n")

    # ── Fetch all chart sources ──────────
    apple_df     = fetch_apple_music_charts()
    spotify_df   = fetch_spotify_global()
    us_df        = fetch_spotify_us()
    billboard_df = fetch_billboard_hot100()

    chart_dfs = [apple_df, spotify_df, us_df, billboard_df]
    combined  = normalize_and_score(chart_dfs)

    if combined.empty:
        print("❌ No data fetched. Check internet connection.")
        return

    raw_path = f"{DATA_DIR}/raw_{TODAY}.csv"
    combined.to_csv(raw_path, index=False)
    print(f"\n   Raw data saved: {raw_path}")

    # ── Google Trends enrichment ─────────
    # Query top 10 songs for real-world search interest
    report_preview = compute_weekly_rankings(combined)
    top_song_keys  = [
        f"{s['title']} {s['artist']}"
        for s in report_preview.get("top_songs", [])[:10]
    ]
    trends = fetch_google_trends(top_song_keys) if top_song_keys else {}

    # ── Final rankings with enrichment ───
    report = compute_weekly_rankings(combined, trends=trends)

    report_path = f"{OUTPUT_DIR}/weekly_report_{TODAY}.json"
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2, default=str)
    print(f"   Report saved: {report_path}")

    # ── Print summary ────────────────────
    n = report.get("chart_sources_used", "?")
    print("\n" + "=" * 55)
    print("  THIS WEEK'S DATA DROP")
    print(f"  ({n} chart sources · + Google Trends enrichment)")
    print("=" * 55)

    if report.get("song_of_week"):
        s        = report["song_of_week"]
        coverage = s.get("coverage", "")
        interest = s.get("search_interest")
        trend_str = f"  · search interest {interest}/100" if interest is not None else ""
        print(f"  Song of the Week:   {s.get('title','—')} — {s.get('artist','—')}")
        print(f"                      {coverage}{trend_str}")

    if report.get("artist_of_week"):
        print(f"  Artist of the Week: {report['artist_of_week'].get('artist','—')}")

    if report.get("genre_of_week"):
        print(f"  Genre of the Week:  {report['genre_of_week'].get('genre','—')}")

    if report.get("one_to_watch"):
        o = report["one_to_watch"]
        print(f"  One to Watch:       {o.get('title','—')} — {o.get('artist','—')}")
        print(f"                      US trending · not yet global")

    print("=" * 55)

    print("\n   Generating visualizations...")
    generate_visualizations(report)

    print(f"\n   Pipeline complete. Check {OUTPUT_DIR}/ for your posts.\n")
    return report


if __name__ == "__main__":
    run_pipeline()
