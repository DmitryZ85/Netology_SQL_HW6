import sqlalchemy
from sqlalchemy.orm import sessionmaker
import json
import psycopg2
from Models import Publisher, Book, Shop, Stock, Sale, create_tables


#Item 1

DSN = 'postgresql://postgres:Z25instr@localhost:5432/netology_hw6'
engine = sqlalchemy.create_engine(DSN)
con = engine.connect()

Session = sessionmaker(bind=engine)
session = Session()

create_tables(engine)


#Item 3

with open('test_data.json', 'r') as f:
    data = json.load(f)

for record in data:
    model = {
        'publisher': Publisher,
        'shop': Shop,
        'book': Book,
        'stock': Stock,
        'sale': Sale,
    }[record.get('model')]
    session.add(model(id=record.get('pk'), **record.get('fields')))

session.commit()
session.close()


#Item 2

def looking_for_publisher():
    input_publisher = input('Введите ID издателя: ')
    join_query = session.query(Shop).join(Stock).join(Book).join(Publisher)
    resulted_query = join_query.filter(Publisher.id == input_publisher)
    for result in resulted_query.all():
        print(f'Издатель под номером {input_publisher} найден в магазине {result.name}')


    looking_for_publisher()