import psycopg2


class DBManager:
    """
    Класс для работы с базой данных
    """
    def __init__(self, user: str, password: str, host: str, port: str, dbname: str = 'postgres',
                 table_name: str = 'vacancy_info'):
        """
        :param user, password, host, port, dbname: параметры для подключения к БД
        :param table_name: имя создаваемой таблицы при создании экземпляра класса
        """
        self.conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
        self.cur = self.conn.cursor()
        self.table_name = table_name
        self.create_table()

    def create_table(self) -> None:
        """
        Создает таблицу в БД
        """
        with self.conn:
            self.cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
            ID_вакансии SERIAL PRIMARY KEY,
            должность VARCHAR(255) NOT NULL,
            работодатель VARCHAR(255),
            зарплата INT,
            валюта VARCHAR(5),
            курс REAL,
            ссылка TEXT,
            дата date
            )
            """)

    def insert_data(self, data: list[dict]) -> None:
        """
        Заполняет созданную таблицу данными
        :param data: список словарей с вакансиями
        """
        with self.conn:
            self.cur.execute(f"""TRUNCATE TABLE {self.table_name} RESTART IDENTITY""")
            for emp in data:
                self.cur.execute(f"""
                INSERT INTO {self.table_name} (должность, работодатель, зарплата, валюта, курс, ссылка, дата)
                VALUES(%s, %s, %s, %s, %s, %s, %s)""",
                                 (emp["title"], emp["employer"], emp["salary"], emp['currency'],
                                  emp['rate'], emp["url"], emp["date_add"]))

    def get_companies_and_vacancies_count(self) -> list[dict]:
        """
        Функция возвращает количество вакансий для каждого из работодателй
        """
        with self.conn:
            self.cur.execute(f"""
            SELECT работодатель, COUNT(*)
            FROM {self.table_name}
            GROUP BY работодатель""")
        data = self.cur.fetchall()
        data_dict = [{d[0]: d[1]} for d in data]
        return data_dict

    def get_all_vacancies(self) -> list[dict]:
        """
        Функция возвращает список всех вакансий от всех работодателй
        """
        with self.conn:
            self.cur.execute(f"""
            SELECT *
            FROM {self.table_name} """)
        data = self.cur.fetchall()
        data_dict = [{"должность": d[1], "работодатель": d[2], "зарплата": d[3], "валюта": d[4],
                      "ссылка на вакансию": d[6], "дата добавления": d[7].strftime('%m.%d.%Y')} for d in data]
        return data_dict

    def get_avg_salary(self) -> float:
        """
        Функция рассчитывает среднюю зарплату по таблице с вакансиями и возвращает ее
        :return: средняя зарплата
        """
        with self.conn:
            self.cur.execute(f"""
               SELECT CEILING(SUM(зарплата/курс)/COUNT(зарплата)) as ср_зарплата
               FROM {self.table_name}
               WHERE зарплата IS NOT NULL""")
        data = self.cur.fetchall()
        return data[0][0]

    def get_vacancies_with_higher_salary(self) -> list[dict]:
        """
        :return: список словарей с вакансиями с зарплатой выше средней
        """
        with self.conn:
            self.cur.execute(f"""
               SELECT *
               FROM {self.table_name}
               WHERE зарплата > (SELECT SUM(зарплата/курс)/COUNT(зарплата)
               FROM {self.table_name}
               WHERE зарплата IS NOT NULL)""")
        data = self.cur.fetchall()
        data_dict = [{"должность": d[1], "работодатель": d[2], "зарплата": d[3], "валюта": d[4],
                      "ссылка на вакансию": d[6], "дата добавления": d[7].strftime('%m.%d.%Y')}
                     for d in data]
        return data_dict

    def get_vacancies_with_keyword(self, keyword):
        """
        :param keyword: ключевое слово, по которому происходит поиск.
        :return: список словарей с вакансиями, в названии которых присутствует ключевое слово
        """
        with self.conn:
            self.cur.execute(f"""
               SELECT *
               FROM {self.table_name}
               WHERE должность LIKE '%{keyword}%'""")
        data = self.cur.fetchall()
        data_dict = [{"должность": d[1], "работодатель": d[2], "зарплата": d[3], "валюта": d[4],
                      "ссылка на вакансию": d[6], "дата добавления": d[7].strftime('%m.%d.%Y')}
                     for d in data]
        return data_dict
