# V3 Live Intelligence

Live market-data collector and V3 pre-pump radar for Nobitex USDT markets.

## Files
- `main.py` — scanner / intelligence engine
- `config.json` — runtime settings
- `requirements.txt` — Python dependencies
- `README.md` — project notes

## Run locally
```bash
pip install -r requirements.txt
python main.py
```

## Render
Use Python 3.

Build command:
```bash
pip install -r requirements.txt
```

Start command:
```bash
python main.py
```

The scanner uses public market data only; no trading API key is required.

Note: local SQLite storage on a cloud instance may be ephemeral unless persistent storage is configured.
