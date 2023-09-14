from pprint import pprint
from dbmanager import DBManager
from function import get_request, get_vacancies
from config import config

params = config()
employers = ['МТС', 'YOTA', 'Beeline', 'AXELSOFT', 'UnitCode', 'Яндекс Крауд', 'Гравити Групп',
             'Первое маркетинговое агентство', 'АЙТИ.СПЕЙС', 'АйТиКвик']


def main():
    # Получаем от API список вакансий по работодателям
    data_vacancy = get_request(employers)
    vacancies = get_vacancies(data_vacancy)

    # Создаем экземпляр класса DBManager, подключаемся к БД, создаем таблицу
    dbmanager = DBManager('vacancy_db', **params)

    # Заполняем таблицу данными
    dbmanager.insert_data(vacancies)

    all_vacancies = dbmanager.get_all_vacancies()
    print('Все вакансии из БД:')
    pprint(all_vacancies, sort_dicts=False)

    print(f'Средняя зарплата - {dbmanager.get_avg_salary()} рублей')

    count_vacancies = dbmanager.get_companies_and_vacancies_count()
    print('Компании и количество их вакансий:')
    pprint(count_vacancies, width=2, sort_dicts=False)

    vacancies_with_keyword = dbmanager.get_vacancies_with_keyword("разработчик")
    print(f'Найдено {len(vacancies_with_keyword)} вакансий с ключевым словом:')
    pprint(vacancies_with_keyword, sort_dicts=False)

    vac_with_higher_salary = dbmanager.get_vacancies_with_higher_salary()
    print(f'Найдено {len(vac_with_higher_salary)} вакансий с зарплатой выше средней:')
    pprint(vac_with_higher_salary, sort_dicts=False)


if __name__ == '__main__':
    main()
