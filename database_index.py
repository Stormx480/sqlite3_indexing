import sqlite3
import re
import pymorphy2
from collections import Counter
from config import *


class Singleton(type):

    _instances = {}

    def __new__(class_, *args, **kwargs):

        if class_ not in class_._instances:

            class_._instances[class_] = super(Singleton, class_).__new__(class_, *args, **kwargs)

        return class_._instances[class_]


class Database(metaclass=Singleton):

    def __init__(self):
        self.conn = sqlite3.connect(database_index_name+".db",
                                    detect_types=sqlite3.PARSE_DECLTYPES,
                                    check_same_thread=False)

        self.conn_main = sqlite3.connect(database_main_name+".db",
                                    detect_types=sqlite3.PARSE_DECLTYPES,
                                    check_same_thread=False)
        self.check_tables()
        self.morph = pymorphy2.MorphAnalyzer()

    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super(Database, cls).__new__(cls)
        return cls.instance

    def connection_decorate(db=None):

        def decorator(a_function_to_decorate):

            def wrapper(self, *args, **kwargs):

                if db == 'main':
                    cursor = self.conn_main.cursor()
                else:
                    cursor = self.conn.cursor()

                kwargs["cursor"] = cursor

                data = a_function_to_decorate(self, *args, **kwargs)

                self.conn.commit()
                cursor.close()

                return data

            return wrapper

        return decorator

    @connection_decorate()
    def check_tables(self, cursor=None):

        sql = "SELECT name FROM sqlite_master WHERE type = 'table'"
        cursor.execute(sql)
        response = cursor.fetchall()

        tables_list = ['words', 'indexes']

        for x in response:
            if x[0] in tables_list:
                tables_list.remove(x[0])

        if len(tables_list) == 0:
            print('Проверка таблиц завершена.')
            return True
        else:
            for table_name in tables_list:
                eval('self.create_table_'+table_name)()
                return True

    @connection_decorate()
    def create_table_words(self, cursor=None):

        sql = "CREATE TABLE IF NOT EXISTS words(id integer PRIMARY KEY, word text UNIQUE)"
        cursor.execute(sql)

        return True

    @connection_decorate()
    def create_table_indexes(self, cursor=None):

        sql = "CREATE TABLE IF NOT EXISTS indexes(id integer PRIMARY KEY, word_id integer, row_id integer)"
        cursor.execute(sql)

        return True

    @connection_decorate(db='main')
    def indexing(self, cursor=None):

        # Создаем запрос к базе данных и отправляем его
        sql = "SELECT id, title, description FROM main"
        cursor.execute(sql)
        response = cursor.fetchall()

        if len(response) == 0:
            return False
        else:
            # Если ответ пришел не пустой, и строки в базе существуют то начинаем индексацию.
            for row in response:

                # Склеиваем текст
                text = row[1]+' '+row[2]
                # Разбиваем его на слова от 2 букв и более.
                words = re.findall(r'\w{2,}', text)

                # Создаем пустой список в который будем записывать готовые слова.
                words_inf = []

                for word in words:
                    # Путем перебора приводим слова к изначальной форме и записываем их в список.
                    word_inf = self.morph.parse(word.lower())[0].normal_form
                    words_inf.append((word_inf,))

                # Отправляем готовые данные на запись в базу данных.
                self.create_index(words=words_inf, row_id=row[0])

            return True

    @connection_decorate()
    def insert_words(self, words, cursor=None):

        sql = "INSERT OR IGNORE INTO words (word) VALUES (?)"
        cursor.executemany(sql, words)

        return True

    @connection_decorate()
    def create_index(self, words, row_id, cursor=None):

        self.insert_words(words)

        sql = "INSERT INTO indexes (row_id, word_id) VALUES ({}, (SELECT id FROM words WHERE word=?))".format(row_id)
        cursor.executemany(sql, words)

        return True

    @connection_decorate()
    def search_row_by_words(self, words, cursor=None):

        sql = "SELECT row_id FROM indexes LEFT JOIN words ON words.word in ({}) WHERE word_id=words.id".format(
            ', '.join('?' for _ in words))
        cursor.execute(sql, words)
        response = cursor.fetchall()

        c = Counter(response).most_common()

        return c


if __name__ == '__main__':
    db = Database()
    db.indexing()
    print(db.search_row_by_words(words=['фотография', '2020', 'айдея']))

