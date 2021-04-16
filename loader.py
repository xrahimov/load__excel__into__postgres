import pandas as pd
import psycopg2
import datetime
import re

# Import CSV
data = pd.read_csv(r'C:\Users\xrahimov\Desktop\test.csv')   
df = pd.DataFrame(data)

# Connect to Postgres
conn = psycopg2.connect("dbname=ifad user=postgres password=postgres")
cursor = conn.cursor()


# Get only Date Function
def get_date_parts(inp):
    dates = re.split(r"(\d+\D+)", inp)
    filter_dates = list(filter(lambda d: d != '', dates))
    return [int(re.sub('[^0-9]', '', x)) for x in filter_dates]


# Insert DataFrame into Table
for row in df.itertuples():
    
    paymentDateList = get_date_parts(row.paymentDate)
    contractNumber = ''
    contractDateList = []
    spent = []
    cashs = []
    recipientId = 0
    componentId = 0
    categoryId = 0
    subComponentId = 0
    subCategoryId = 0
    rcategory = 0
    money = float(row.usd.replace(',', ''))
    
    if row.category == "I":
        rcategory = 1
    elif row.category == "II":
        rcategory = 2
    elif row.category == "III":
        rcategory = 3
    elif row.category == "IV":
        rcategory = 4
    elif row.category == "V":
        rcategory = 5
    elif row.category == "VI":
        rcategory = 6
    else:
        rcategory = 123
    
    # Get all recipients from DB
    recipients_query = "select * from recipient"
    cursor.execute(recipients_query)
    recipients = cursor.fetchall()
    
    # Get all components from DB
    components_query = "select * from component"
    cursor.execute(components_query)
    components = cursor.fetchall()
    
    # Get all categories from DB
    categories_query = "select * from category"
    cursor.execute(categories_query)
    categories = cursor.fetchall()
    
    # Get all subComponents from DB
    subComponents_query = "select * from sub_component"
    cursor.execute(subComponents_query)
    subComponents = cursor.fetchall()
    
    # Get all subCategories from DB
    subCategories_query = "select * from sub_category"
    cursor.execute(subCategories_query)
    subCategories = cursor.fetchall()
    
    # Transaction    
    while money > 0:
        cursor.execute("""
            select * 
            from cash_in
            where currency = 'usd' and source = %s and current_balance > 0
            order by date asc
            fetch first row only
            """,("iloan" if row.source == "Заем МФСР" else "igrant",))
        
        cashin = cursor.fetchone()
        cash_in_id = cashin[0]
        c_balance = cashin[7]
        
        if money > c_balance:
            money = money - c_balance
            spent.append(c_balance)
            c_balance = 0
        else:
            c_balance = c_balance - money
            spent.append(c_balance)
            money = 0
            
        cursor.execute("update cash_in set current_balance = %s where id = %s", (c_balance, cash_in_id,))
        conn.commit()
        cashs.append(cash_in_id)
        
    # Recipient
    for r in recipients:
        if r[1] == row.recipient:
            recipientId = r[0]
            break
        else:
            continue
    
    if recipientId == 0:
        cursor.execute("""INSERT INTO recipient ("recipientName") VALUES (%s) RETURNING id""", 
                       (row.recipient,))
        recipientId = cursor.fetchone()[0]
        conn.commit()
        
    # Component
    for com in components:
        if com[2] == row.component:
            componentId = com[0]
            break
        else:
            continue
    
    if componentId == 0:
        cursor.execute("""
        INSERT INTO component 
        (
            "componentName", 
            "componentNumber", 
            "budget"
        ) VALUES (%s, %s, %s) RETURNING id""", 
                       ("Component Name", row.component, 0))
        componentId = cursor.fetchone()[0]
        conn.commit()
        
    # Category
    for cat in categories:
        if cat[2] == rcategory:
            categoryId = cat[0]
            break
        else:
            continue
    
    if categoryId == 0:
        cursor.execute("""
        INSERT INTO category 
        (
            "categoryName", 
            "categoryNumber", 
            "budget"
        ) VALUES (%s, %s, %s) RETURNING id""", 
                       ("Category Name", rcategory, 0))
        categoryId = cursor.fetchone()[0]
        conn.commit()
    
    # SubComponent
    for scom in subComponents:
        if scom[2] == row.subComponent:
            subComponentId = scom[0]
            break
        else:
            continue
    
    if subComponentId == 0:
        cursor.execute("""
        INSERT INTO sub_component 
        (
            "subComponentName", 
            "subComponentNumber", 
            "componentId", 
            "budget"
        ) VALUES (%s, %s, %s, %s) RETURNING id""", 
                       ("SubComponent Name", row.subComponent, componentId, 0))
        subComponentId = cursor.fetchone()[0]
        conn.commit()
        
    # SubCategory
    for scat in subCategories:
        if scat[2] == rcategory + 0.1:
            subCategoryId = scat[0]
            break
        else:
            continue
    
    if subCategoryId == 0:
        cursor.execute("""
        INSERT INTO sub_category 
        (
            "subCategoryName", 
            "subCategoryNumber", 
            "categoryId", 
            "budget"
        ) VALUES (%s, %s, %s, %s) RETURNING id""", 
                       ("SubCategory Name", rcategory + 0.1, categoryId, 0))
        subCategoryId = cursor.fetchone()[0]
        conn.commit()
        
    # Contract
    if type(row.contractDateAndNumber) is float:
        contractNumber = ''
    else:
        otIndex = row.contractDateAndNumber.find('от')
        contractNumber = row.contractDateAndNumber[:otIndex-1]
        contractDateList = get_date_parts(row.contractDateAndNumber[otIndex+3:])
    
    # Insert Expense
    cursor.execute("""
        INSERT INTO expense (
            description, 
            "paymentDate",
            "paymentOrder",
            "contractNumber",
            "contractDate",
            "expenseSource",
            currency,
            amount,
            "categoryId",
            "componentId", 
            "subComponentId",
            "subCategoryId",
            "recipientId",
            "expenseCountry"
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        returning id
        """,
       (
           row.description,
           datetime.date(paymentDateList[2], paymentDateList[1], paymentDateList[0]),
           row.paymentOrder,
           contractNumber,
           datetime.date(contractDateList[2], contractDateList[1], contractDateList[0]) if contractDateList != [] else None,
           "iloan" if row.source == "Заем МФСР" else "igrant",
           'usd',
           float(row.usd.replace(',', '')),
           categoryId,
           componentId,
           subComponentId,
           subCategoryId,
           recipientId,
           "Uzbekistan"
       )       
    )
    
    exp = cursor.fetchone()[0]
    
    for i in range(len(cashs)):
        cursor.execute("""
            insert into expense_cash_in_transaction (
                amount,
                "cashInId",
                "expenseId"
            ) values (%s, %s, %s)
            """,
            (
                spent[i],
                cashs[i],
                exp
            )
        )
    
conn.commit()
