# DataDropFriday

**Music trends. By numbers.**

A weekly music trend analysis pipeline that tracks top songs, artists, and genres across streaming platforms — publishing findings as clean data visualizations every Friday.

Inspired by [@databutmakeitfashion](https://instagram.com/databutmakeitfashion) (Madé Lapuerta) applied to music.

Follow: [@datadropfriday](https://instagram.com/datadropfriday) on all platforms.

---

## How It Works

Every Thursday night, the pipeline pulls from 4 independent chart sources, cross-references them, and produces 3 post-ready visualizations for Friday.

```
Apple Music Top 50  ──┐
Spotify Global Daily ─┤→ normalize → score → rankings → visualizations
Spotify US Daily    ──┤
Billboard Hot 100   ──┘
                       + Google Trends search interest enrichment
```

**Scoring logic:** Each source ranks songs 1–50. Score = `((max_rank - rank + 1) / max_rank) * 100`. Songs appearing across multiple sources score higher — meaning results reflect genuine cross-platform consensus, not a single chart's bias.

**Breakout detection:** Songs in Spotify US but not in Spotify Global = trending domestically before going global. This surfaces the "One to Watch" each week.

---

## Weekly Output

Every Friday post covers 5 data points:

| Metric | Description |
|---|---|
| Song of the Week | Highest combined score across all sources |
| Artist of the Week | Biggest cross-platform momentum |
| Genre of the Week | Dominating sound this week |
| One to Watch | US trending before global — about to blow up |
| Search Interest | Google Trends score for top picks (0–100) |

---

## Data Sources

| Source | Method | Auth |
|---|---|---|
| Apple Music Top 50 | RSS JSON feed | None |
| Spotify Global Daily | kworb.net scraper | None |
| Spotify US Daily | kworb.net scraper | None |
| Billboard Hot 100 | billboard.py | None |
| Google Trends | pytrends | None |

No API keys required for any source.

---

## Setup

```bash
git clone https://github.com/YOUR_USERNAME/datadropfriday.git
cd datadropfriday

python3 -m venv venv
source venv/bin/activate

pip install requests pandas matplotlib beautifulsoup4 python-dotenv billboard.py pytrends
```

## Run

```bash
source venv/bin/activate
python pipeline.py
```

Outputs saved to `outputs/`:
- `top_songs_YYYY-MM-DD.png` + `top_songs_latest.png`
- `top_artists_YYYY-MM-DD.png` + `top_artists_latest.png`
- `weekly_summary_YYYY-MM-DD.png` + `weekly_summary_latest.png`
- `weekly_report_YYYY-MM-DD.json`

---

## Project Structure

```
datadropfriday/
├── pipeline.py          ← main pipeline (run every Thursday)
├── data/                ← raw weekly CSVs (gitignored)
├── outputs/             ← generated PNGs + JSON (gitignored)
└── venv/                ← Python virtual environment (gitignored)
```

---

## Automation

The pipeline is scheduled to run every Thursday at 9pm via cron:

```
0 21 * * 4 cd ~/Desktop/Projects/datadropfriday && source venv/bin/activate && python pipeline.py >> logs/pipeline.log 2>&1
```

---

## Built By

[Kanupriya Guha](https://github.com/YOUR_USERNAME) — ML Engineer (NLP specialist)  
Building in public: [@datadropfriday](https://instagram.com/datadropfriday)

---

*Est. 2026*
