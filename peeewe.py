from peewee import *
import requests
from random import * 

db_name = 'Coins'

dbhandle = PostgresqlDatabase(db_name,user = 'postgres', password = 'root1', host = 'localhost')


class Coins(Model):
    id = PrimaryKeyField(null=False)
    symbol = CharField(unique=True)
    priceChange = FloatField()
    priceChangePercent = FloatField()
    weightedAvgPrice = FloatField()
    prevClosePrice = FloatField()
    lastPrice = FloatField()
    lastQty = FloatField()
    bidPrice = FloatField()
    askPrice = FloatField()
    openPrice = FloatField()
    highPrice = FloatField()
    lowPrice = FloatField()
    volume = FloatField()
    quoteVolume = FloatField()
    openTime = TimestampField()
    closeTime = TimestampField()
    firstId = IntegerField()
    lastId = IntegerField()
    count = IntegerField()

    class Meta:
        db_table = 'coins'
        database = dbhandle

try:
    dbhandle.connect()
    Coins.create_table()
except InternalError as px:
    print(str(px))

# Получаем список криптовалют от Binance.
data = requests.get('https://api.binance.com/api/v1/ticker/24hr').json()

for coin in data:
    # Проверяем если валюта существует.
    exists = Coins.select().where(Coins.symbol == coin['symbol'])

    if bool(exists):
        print('Обновляем цены для:', coin['symbol'])
        # Запись существует.
        # Обновляем цены криптовалют.
        Coins.update(
            lastPrice=coin['lastPrice'],
            lastQty=coin['lastQty'],
            bidPrice=coin['bidPrice'],
            askPrice=coin['askPrice'],
        ).where(Coins.symbol == coin['symbol']).execute()
    else:
        print('Добавляем новую запись:', coin['symbol'])
        # Создаем новую запись.
        Coins.create(
            symbol=coin['symbol'],
            priceChange=coin['priceChange'],
            priceChangePercent=coin['priceChangePercent'],
            weightedAvgPrice=coin['weightedAvgPrice'],
            prevClosePrice=coin['prevClosePrice'],
            lastPrice=coin['lastPrice'],
            lastQty=coin['lastQty'],
            bidPrice=coin['bidPrice'],
            askPrice=coin['askPrice'],
            openPrice=coin['openPrice'],
            highPrice=coin['highPrice'],
            lowPrice=coin['lowPrice'],
            volume=coin['volume'],
            quoteVolume=coin['quoteVolume'],
            openTime=int(coin['openTime'] / 1000),
            closeTime=int(coin['closeTime'] / 1000),
            firstId=coin['firstId'],
            lastId=coin['lastId'],
            count=coin['count'],
        )

# Сортируем монеты по объему за 24 часа.
sorted_coins = Coins.select().order_by(Coins.volume.desc())

print('Сортировка по объему за 24 часа')
for coin in sorted_coins:
    print(coin.symbol, coin.volume)

print('-' * 30)

# Кол-во монет которые торгуются с BNB в паре (Binance Coin).
bnb_count = Coins.select().where(Coins.symbol.endswith('BNB')).count()
print('Кол-во монет в паре с BNB:', bnb_count)

print('-' * 30)

# Выбираем случайные 5 монет.
random = Coins.select().order_by(Coins.bidPrice>1000).limit(5)

print('Случайные 5 монет:')
for coin in random:
    print(coin.symbol)
