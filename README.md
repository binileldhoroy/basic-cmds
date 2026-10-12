# NetBrain tables → CSV → Kafka

1. Reads `Hoostname` (or `Hostname`), `Tablename` and `VRF` from the sheet.
2. For each row, calls `GET /V1/CMDB/Devices/DeviceTableData`, requesting every page until all rows are collected.
3. Writes `output/<hostname>_<table>[_<vrf>].csv`: the response columns, then `hostname` and `tableName`.
4. For each row, publishes this event with `await producer.publish(key="", value=event_data)`:
   ```json
   {"eventType": "<table name>", "entityType": "NCT_Table", "timestamp": "...",
    "attributes": {"hostname": "...", "tableName": "...", "vrf": "..."},
    "data": {"Dest.Addr": "0.0.0.0", "Mask": "0", ...}}
   ```

## Setup
```bash
pip install -r requirements.txt
cp .env.example .env   # set NETBRAIN_TOKEN, INPUT_XLSX
```

In `main()` in `netbrain_tables.py`, replace `from producer import producer` with your own producer, or call `await run(producer)` from your own code. Then run:
```bash
python netbrain_tables.py
```
