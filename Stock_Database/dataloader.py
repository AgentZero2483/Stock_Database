#%%
# The '%%' allow us to run each block of code 1by1 like a jupyter notebook
import matplotlib
import os
import pandas as pd
import psycopg2 #For SQLAlchemy data connections
from sqlalchemy import create_engine
from tqdm import tqdm_notebook
from passwords import db_password

#%%
# Create database connection
#db_password = os.environ.get('thisis2021')
engine = create_engine('postgresql://postgres:{}@localhost/stock_data'.format(db_password))

#Set variable for CSV file locations(Price information, Ticker information)
price_path = 'data/bars'
ticker_path = 'data/tickers'

#%%
#Create SQL table directory
def create_price_table(symbol):

    #import price csv into pandas data frame
    price_df = pd.read_csv('{}/{}.csv'.format(price_path, symbol))

    #formatting for the dataframe
    price_df = price_df[['symbol', 'date', 'volume', 'open', 'close', 'high', 'low',
                        'dividend', 'ratio_adj', 'volume_adj', 'open_adj', 'close_adj',
                        'high_adj', 'low_adj', 'dollar_volume']]
    price_df['date'] = pd.to_datetime(price_df['date'])
    price_df = price_df.fillna(0)
    price_df['updated'] = pd.to_datetime('now')

    #write data into database
    price_df.to_sql('daily_prices', engine, if_exists='replace', index=False)

    #Create primary key on 'daily_prices' table
    query = """ALTER TABLE daily_prices
                ADD PRIMARY KEY (symbol, date);"""
    engine.execute(query)

    return 'Daily prices table created successfully'

create_price_table('AAPL')

# %%
