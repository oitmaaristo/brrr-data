# BRRR Data

> BRRR Capital market data pipeline

## Eesmärk

IB (Interactive Brokers) historical data kogumine ja haldamine.

## Asukoht VPS-il

```
/home/brrr/brrr-data/
├── market_data.db      # SQLite DB (gitignore'd!)
├── ib_backfill.py      # Backfill script
├── rollover_dates.json # Futures rollover kuupäevad
└── logs/               # Skriptide logid
```

## Kasutamine

```bash
# VPS-ile
ssh brrr@100.93.186.17
cd /home/brrr/brrr-data

# Backfill käivitamine
python ib_backfill.py

# DB kontroll
sqlite3 market_data.db "SELECT COUNT(*) FROM ohlcv_MNQ_1m;"
```

## Andmebaas

**NB:** `market_data.db` on `.gitignore`'s kuna see on liiga suur.

Backup tuleb teha eraldi (TODO).

### Tabelid

| Tabel | Sisu |
|-------|------|
| `ohlcv_MNQ_1m` | MNQ 1-min bars |
| `ohlcv_MES_1m` | MES 1-min bars |
| `ohlcv_NQ_1m` | NQ 1-min bars |
| ... | ... |

## TWS Ühendus

Backfill skript ühendub TWS-iga:
- **Host:** Risto Windows (192.168.x.x) või VPS Tailscale
- **Port:** 7496 (live) / 7497 (paper)
- **Client ID:** 10

## Seotud

- [brrr-printer2](https://github.com/oitmaaristo/brrr-printer2) - Trading engine
- TWS API docs: https://interactivebrokers.github.io/

---
*BRRR Capital 🖨️💰*
