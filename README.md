
## Setup
```bash
pip install -r requirements.txt
cp .env.example .env   # set NETBRAIN_TOKEN, INPUT_XLSX
```

In `main()` in `netbrain_tables.py`, replace `from producer import producer` with your own producer, or call `await run(producer)` from your own code. Then run:
```bash
python netbrain_tables.py
```
