from datetime import datetime, timedelta
from airflow.decorators import dag, task

default_args = {
    'owner': 'mickepicke',
    'depends_on_past': False,
    'start_date': datetime(2021, 1, 1),
    'email': ['micke.lind@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag('potato_pig_v3',catchup=False, schedule_interval='@hourly', default_args=default_args)
def potato_pig_dag():

    @task
    def parse_rss_to_file():
        import json
        import feedparser
        import pandas as pd
        import uuid
        from pathlib import Path
        feed = feedparser.parse('https://polisen.se/aktuellt/rss/stockholms-lan/handelser-rss---stockholms-lan/')
        df = pd.DataFrame()
        df["Title"] = [entry.title for entry in feed.entries]
        df["Link"] = [entry.link for entry in feed.entries]
        df["Published"] = [entry.published for entry in feed.entries]
        df["Summary"] = [entry.summary for entry in feed.entries]

        filename = str(uuid.uuid4().hex+".csv")
        p = Path(__file__).with_name(filename)
        df.to_csv(p, index=False)

        return json.dumps(filename)
    
    @task
    def file_to_database(filename):
        import sqlalchemy
        import psycopg2
        from pathlib import Path
        import pandas as pd
        import re
        import json
        import numpy as np

        engine = sqlalchemy.create_engine('postgresql+psycopg2://potatopig:potatopig@potatopig:5432/potatopig') 
        
        p = Path(__file__).with_name(json.loads(filename))
        df = pd.read_csv(p)
        df[['Tid', 'Typ','Område','Område2']] = df['Title'].str.split(',', expand=True)
        df['Område2'] = df['Område2'].where(df['Område2'].notnull(),df['Område'])
        df.drop(['Tid','Område', 'Title'], axis=1, inplace=True)

        file = open(Path(__file__).with_name('omraden_list.txt'),'r', encoding='utf-8')
        locations_list = [line.strip() for line in file]
    
        df['Område3'] = df['Summary'].str.findall('(' + '|'.join(locations_list) + ')', flags=re.IGNORECASE)
        df['Område3'] = df['Område3'].map(lambda x: str(x).lstrip('[\'').rstrip('\']'))
        df['Område3'].replace(r'^\s*$', np.nan, regex=True, inplace=True)
        df['Område3'].fillna(df['Område2'], inplace=True)
        df.drop(['Område2'], axis=1, inplace=True)
        df['Published'] = df['Published'].str.slice(start=5,stop=16)
        df['Published'] = pd.to_datetime(df['Published'])
        df.rename(columns={"Link": "Link", "Published": "Datum", "Summary": "Beskrivning", "Typ": "Typ", "Område3": "Plats"},inplace=True)

        try:
            df.to_sql('handelser_temp',con=engine,if_exists='replace', index=False)
            engine.execute("""
            CREATE TABLE IF NOT EXISTS handelser(Link text UNIQUE NOT NULL, Datum date, Beskrivning text, Typ varchar(50), Plats varchar(50) );
            """)
            engine.execute("""
            INSERT INTO handelser(Link, Datum, Beskrivning, Typ, Plats)
            SELECT * FROM handelser_temp
            ON CONFLICT(Link) DO NOTHING;
            """)
            engine.execute("""
            DROP TABLE handelser_temp;
            """)
            engine.execute("""
            COMMIT;
            """)
        except ValueError:
            pass
        return(filename)
    @task
    def delete_file(filename):
        import os
        from pathlib import Path
        import json
        try:
            os.remove(Path(__file__).with_name(json.loads(filename)))
        except FileNotFoundError:
            pass

    #Ordning
    delete_file(file_to_database(parse_rss_to_file()))


potato = potato_pig_dag()
