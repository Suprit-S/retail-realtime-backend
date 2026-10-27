# Dataset

This project uses the **UCI Online Retail II** dataset (1,067,371 timestamped
transaction rows, CC BY 4.0):

https://archive.ics.uci.edu/dataset/502/online+retail+ii

The file on UCI is an `.xlsx` with **two sheets** — "Year 2009-2010" and
"Year 2010-2011". Combine both sheets into a single CSV named
`online_retail_ii.csv` and place it in this folder, with these exact columns
(the original column names, comma-separated, UTF-8):

```
Invoice,StockCode,Description,Quantity,InvoiceDate,Price,Customer ID,Country
```

Quick way to combine both sheets with pandas:

```python
import pandas as pd

sheets = pd.read_excel("online_retail_ii.xlsx", sheet_name=None)
combined = pd.concat(sheets.values(), ignore_index=True)
combined.to_csv("online_retail_ii.csv", index=False)
```

This CSV is intentionally **not** committed to Git (see `.gitignore`) — it is
large, and each machine that runs `docker compose up` should supply its own
copy here. The producer reads it from `/data/online_retail_ii.csv` inside its
container (mounted from this folder — see `docker-compose.yml`).

For a quick smoke test without the full 1M-row file, any small CSV with the
same columns and a valid `InvoiceDate` works — the producer does not care how
many rows there are.