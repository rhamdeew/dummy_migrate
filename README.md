### Dummy migrate
Can migrate only data from MySQL to PostgreSQL.

#### Install

    pip install -r requirements.txt

replace

    dbname='db_name', user='postgres', password='superpass', host='postgres'

and

    host='db', user='root',  password='superpass', db='db_name',

with your requisites

#### Usage
1) Create new PostgreSQL database, apply migrations
2) Run script

Python 2

    python migrate_python2.py

Python 3

    python migrate.py
