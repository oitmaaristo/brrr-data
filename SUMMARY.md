# BRRR-DATA - SUMMARY

> Auto-genereeritud: 2026-02-14

## Eesmärk

Market data pipeline - IB historical andmete kogumine ja haldamine.
Database fail on gitignore'd (liiga suur), ainult skriptid ja konfig repos.

## Põhikomponendid

| Fail | Otstarve |
|------|----------|
| `ib_backfill.py` | IB historical data backfill |
| `rollover_dates.json` | Futures rollover kuupäevad |
| `check_db.py` | Database diagnostika |

## Asukoht

- **VPS:** `/home/brrr/brrr-data/`
- **Database:** `market_data.db` (gitignore'd)

## Viimased muudatused

- 2026-02-14: Repo loodud, struktuur paika
- 2026-02-13: Backfill script specific contracts versioon

---
*BRRR Capital mälusüsteem*
