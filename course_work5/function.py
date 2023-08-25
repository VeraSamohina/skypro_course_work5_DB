import requests

url = 'https://api.hh.ru/vacancies/'


def get_request(employers: list) -> list[dict]:
    """
    Функция получает на вход список с работодателями, отправляет request запрос к API по каждому
     и возвращает данные по запросу
    :param employers: список работодателй для поиска
    :return: список словарей с данными от API по вакансиям выбранных работодателей
    """
    data_employers = []
    for employer in employers:
        response = requests.get(url, params={'text': employer, 'search_field': 'company_name', 'per_page': 100,
                                             'archived': False})
        data = response.json()
        print(f'Добавлены вакансии {employer}')
        data_employers.append(data['items'])
    return data_employers


def get_rates() -> dict:
    """
    Функция делает запрос к API для получения справочных словарей hh
     и формирует словарь с данными по отношению валюты к рублю
    :return: словарь вида {код валюты: курс валюты(по отношению к рублю)}
    """
    rates = {}
    dictionaries_hh = requests.get('https://api.hh.ru/dictionaries/').json()
    list_currency = dictionaries_hh['currency']
    for dct in list_currency:
        rates[dct['code']] = dct['rate']
    return rates


def get_vacancies(data: list[dict]) -> list[dict]:
    """
    :param data: список словарей с данными по вакансиям от API
    :return: список словарей с вакансиями в формате, удобном для добавления в БД
    """
    rates = get_rates()
    vacancies = []
    for emp in data:
        for vac in emp:
            vacancy = {
                'employer': vac['employer']['name'],
                'title': vac['name'],
                'salary': vac['salary']['from'] if vac['salary'] else None,
                'currency': vac['salary']['currency'] if vac['salary'] else None,
                'rate': rates[vac['salary']['currency']] if vac['salary'] else None,
                'url': vac['alternate_url'],
                'date_add': vac['published_at']
            }
            vacancies.append(vacancy)
    return vacancies
