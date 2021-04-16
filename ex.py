import pandas as pd
import psycopg2
import datetime
import re

# Import CSV
data = pd.read_csv(r'C:\Users\xrahimov\Desktop\cash__in.csv')   
df = pd.DataFrame(data)

# Connect to Postgres
conn = psycopg2.connect("dbname=ifad user=postgres password=postgres")
cursor = conn.cursor()


# Get only Date Function
def get_date_parts(inp):
    dates = re.split(r"(\d+\D+)", inp)
    filter_dates = list(filter(lambda d: d != '', dates))
    return [int(re.sub('[^0-9]', '', x)) for x in filter_dates]


wa_number = 0
# Insert DataFrame into Table
for row in df.itertuples():
    print(row)
    wa_number += 1
    amount = float(row.amount.replace(',', ''))
    dateList = get_date_parts(row.date)
    
    # Insert Expense
    cursor.execute("""
        INSERT INTO cash_in (
            amount,
            "from",
            "appNumber",
            source,
            currency,
            current_balance,
            "date",
            "start",
            "end",
            "type"
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
       (
           amount,
           "ifad",
           wa_number,
           "iloan" if row.src == "LOAN" else "igrant",
           "usd",
           amount,
           datetime.date(dateList[2], dateList[1], dateList[0]),
           datetime.date(dateList[2], dateList[1], dateList[0]),
           datetime.date(dateList[2], dateList[1], dateList[0]),
           "repl",
       )       
    )
    
conn.commit()
