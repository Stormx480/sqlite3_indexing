import sqlite3
from config import database_main_name


class Singleton(type):

    _instances = {}

    def __new__(class_, *args, **kwargs):

        if class_ not in class_._instances:

            class_._instances[class_] = super(Singleton, class_).__new__(class_, *args, **kwargs)

        return class_._instances[class_]


class Database(metaclass=Singleton):

    def __init__(self):
        """
        При инициализации класса мы создаем объект подключения к базе данных,
         и проверяем наличие всех необходимых таблиц.
        """
        self.conn = sqlite3.connect(database_main_name+".db",
                                    detect_types=sqlite3.PARSE_DECLTYPES,
                                    check_same_thread=False)
        self.check_tables()

    def __new__(cls):
        """
        Функция которая гарантирует существование только одного объекта определённого класса,
            а также позволяет достучаться до этого объекта из любого места программы.
        Использует metaclass Singleton.
        В качестве первого аргумента в метод super надо передать название класса.
        """
        if not hasattr(cls, 'instance'):
            cls.instance = super(Database, cls).__new__(cls)
        return cls.instance

    def connection_decorate(a_function_to_decorate):
        """
        Функция-декоратор которая позволяет инициализировать объект курсора при запросе к базе данных.
        После запроса изменения вносятся в базу данных и курсор закрывается.
        """
        def wrapper(self, *args, **kwargs):

            cursor = self.conn.cursor()

            kwargs["cursor"] = cursor

            data = a_function_to_decorate(self, *args, **kwargs)

            self.conn.commit()
            cursor.close()

            return data

        return wrapper

    @connection_decorate
    def check_tables(self, cursor=None):
        """
        Функция для проверки наличия всех необходимых таблиц в базе данных.
        В случае отстутвия таблиц создает их, вызывая соотвествующую по названию функцию.
        Список таблиц должен быть заранее определен в переменной tables_list.
        :param cursor:
        :return:
        """
        sql = "SELECT name FROM sqlite_master WHERE type = 'table'"
        cursor.execute(sql)
        response = cursor.fetchall()

        tables_list = ['main']

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

    @connection_decorate
    def create_table_main(self, cursor=None):

        sql = "CREATE TABLE IF NOT EXISTS main(id integer PRIMARY KEY, title text UNIQUE, description text, path text)"
        cursor.execute(sql)

        return True

    @connection_decorate
    def write_to_table_main(self, title: str, description: str, path: str, cursor=None):

        sql = "INSERT INTO main (title, description, path) VALUES (?, ?, ?)"
        cursor.execute(sql, (title, description, path))

        return True


if __name__ == '__main__':
    db = Database()

    table_data = (
        {
            'title': 'Фотография слона',
            'description': 'Фотография слона в Африке летом 2019 года.',
            'path': '/home/mikhail/photo13.jpg'
        },
        {
            'title': 'Архив с тестовыми',
            'description': 'Архив с тестовыми заданиями за апрель.',
            'path': '/home/mikhail/tests.rar'
        },
        {
            'title': 'дятел',
            'description': 'дятел вуди для комикса',
            'path': '/home/mikhail/wudi.png'
        },
        {
            'title': 'текст faint',
            'description': 'Linkin Park - Faint',
            'path': '/home/mikhail/faint.txt'
        },
        {
            'title': 'key_2020',
            'description': 'ключи для айдеи за 20 год',
            'path': '/home/mikhail/key_2020_06.txt'
        }
    )

    for x in table_data:
        db.write_to_table_main(
            title=x['title'],
            description=x['description'],
            path=x['path']
        )

