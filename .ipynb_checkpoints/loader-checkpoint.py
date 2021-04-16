import pandas as pd
import psycopg2
import datetime
import re

# Import CSV
data = pd.read_csv(r'C:\Users\xrahimov\Desktop\03__2019.csv')   
df = pd.DataFrame(data)

print(df)
# Connect to Postgres
conn = psycopg2.connect("dbname=ifad user=postgres password=postgres")
cursor = conn.cursor()

# Insert DataFrame into Table
for row in df.itertuples():
    dates = re.split(r"(\d+\D+)", row.paymentDate)
    filterDates = list(filter(lambda d: d != '', dates))
    dateList = [int(re.sub('[^0-9]', '', x)) for x in filterDates]
    
    cursor.execute("""
        INSERT INTO expense (
            description, 
            "paymentDate",
            currency,
            amount,
            "categoryId",
            "componentId", 
            "subComponentId",
            "recipientId"
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """,
                   (row.Description,
                   datetime.date(dateList[2], dateList[1], dateList[0]),
                   'usd',
                   float(row.usd.replace(',', '')),
                   row.Category,
                   1,
                   2,
                   2)       
    )
conn.commit()