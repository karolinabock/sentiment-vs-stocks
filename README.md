# sentiment-vs-stocks

## Project Overview

This project analyzes whether the sentiment of market news published on [MKT News](https://mktnews.com/index.html) influences the stock prices of AAPL, TSLA, and AMZN over the last 6 months. These three stocks were selected for testing purposes, but the pipeline is easily extendable to analyze other stocks by modifying the input symbol list.

## Objective

The goal is to examine correlations between market sentiment (expressed in news) and stock price changes using NLP sentiment analysis, financial data, and a Power BI dashboard.

## Technologies

| Area                 | Tools / Technologies         |
| -------------------- | ---------------------------- |
| Data Collection      | `yfinance`, `Selenium`       |
| NLP / Sentiment      | `VADER`, `TextBlob` (Python) |
| Database             | `PostgreSQL`, `SQLAlchemy`   |
| Data Analysis  <WIP> | `pandas`, `matplotlib`       |
| Visualization  <WIP> | `Power BI Desktop`           |
| Data Modeling  <WIP> | `draw.io`                    |
| Documentation / Repo | `README.md`, `GitHub`        |

## Folder Structure

```
sentiment-vs-stocks/
├── data/
│   ├── raw/                 # Raw data from yfinance and MKTNews
├── notebooks/ <WIP>         # Jupyter notebooks (NLP, EDA)
├── src/
│   ├── etl/                 # Data extraction scripts
│   ├── process/ 
│     ├── map_news_to_sessions.py       # Mapping news to sessions
│     ├── sentiment.py                  # Sentiment analysis
│     └── update_missing_sentiment.py   # update missing sentiment if exist
│   └── db/
│     ├── schema.sql <WIP>
│     └── load_data.py                  # uploads stock data and news (with sentiment analysis) into a db, assigning symbol
├── dashboard/
│   └── sentiment_dashboard.pbix <WIP>
├── ERD/
│   └── data_model.drawio <WIP>
├── run_pipeline.py                     # run program
└── README.md
```

## How to Run

1. Create virtual environment and install requirements(Python Packages):

```bash
pip install -r requirements.txt
```

2. Set environment variables in `.env` (e.g., `DATABASE_URL=`)
3. Set stock symbols in data/raw/stock names.txt AS "AAPL;Apple"
4. Run the pipeline:

```bash
python run_pipeline.py
```

4. Open `sentiment_dashboard.pbix` in Power BI and connect to PostgreSQL

## Core Logic

- **Cutoff time**: news after 10:00 PM is assigned to the next trading session
- **Trading days only**: weekends and holidays are excluded, news are assigned to next session day
- **Sentiment**: classified as `positive`, `neutral`, or `negative` + compound score from VADER

## SQL Views <WIP>

This project generates several PostgreSQL views:

- `daily_sentiment` - daily average sentiment
- `stock_changes` - daily stock price changes (%)
- `sentiment_vs_stock` - merged sentiment + stock changes
- `joined_data` - view for Power BI

## Power BI Visualizations <WIP>

- Stock price vs sentiment over time
- Count of positive/negative news
- Correlation between sentiment changes and stock performance

## Outcome & Future Work

- Future extension: ML classifier to detect stock-specific impact of news

## Author

[Karolina Bock](https://github.com/karolinabock/sentiment-vs-stocks)  
[GitHub repo](https://github.com/karolinabock/sentiment-vs-stocks)

---

> 📆 Last updated: 2025-05-28
