# BRRR-DATA TODO

## 🔍 Vajab inventuuri (koos CC-ga)

### `/home/brrr/brrr-data/collector/` kaust

Vana andmekogumise süsteem Pi-lt. **Pole teada kas PRINTER 2 kasutab seda!**

Failid:
- `api_server.py` - API server
- `backfill_service.py` - Backfill teenus
- `database.py` - DB operatsioonid
- `main.py` - Entry point
- `websocket_collector.py` - WS collector
- `config.py` - Konfig
- `requirements.txt` - Sõltuvused

**Küsimused CC-le:**
1. Kas PRINTER 2 engine kasutab `collector/` koodi?
2. Kas `ib_backfill.py` asendab `backfill_service.py`?
3. Kas `websocket_collector.py` on kasutusel?

### Tõenäoliselt kustutada (pärast kinnitust)

- `ib_backfill_pi.py` - Vana Pi versioon (uus `ib_backfill.py` olemas)
- `api_server.py.save` - Backup prügi
- `collector/__pycache__/` - Cache

---

*Lisatud: 2026-02-14*
