import os
from flask import Flask, render_template, request, g, session, flash, abort, url_for
import psycopg2
from psycopg2 import extras
from FDataBase import FDataBase
import pandas as pd
import uuid
import warnings


app = Flask(__name__)

warnings.resetwarnings()
warnings.simplefilter('ignore', pd.errors.SettingWithCopyWarning)

dict_card = [
    {'title_card': 'Возрастная проба',
     'discr_card': 'Отчеты возрастной пробы',
     'url': 'prob',
     'img_path': 'images/aged_prob.png'
     },
    {'title_card': 'BioFox',
     'discr_card': 'Расчетные задачи из BioFox',
     'url': 'tasks_biofox',
     'img_path': 'images/counting_tasks.png'
     },
    {'title_card': 'BioFox',
     'discr_card': 'Справки по запросу',
     'url': 'info_biofox',
     'img_path': 'images/query.png'
     },
    {'title_card': 'Анализ питания',
     'discr_card': 'Отчеты об анализе питания',
     'url': 'analysis_nutrition',
     'img_path': 'images/analysis_nutr.png'
     },
    {'title_card': 'Другие задачи',
     'discr_card': 'Прочая работа с файлами',
     'url': 'another_tasks',
     'img_path': 'images/another_tasks.png'
     },
]

dict_report = {
    'prob': {
        'title_page': 'Возрастная проба',
        'discr_page': 'Отчеты возрастной пробы',
        'alias': 'prob',
        'html_forms': {
            'aged-form1': 'aged-prob1.html',
            'aged-form2': 'aged-prob2.html',
            'aged-form3': 'aged-prob3.html',
            'aged-form4': 'aged-prob4.html',
            'aged-form11': 'aged-prob11.html',
            'aged-form12': 'aged-prob12.html',
            'aged-form13': 'aged-prob13.html',
            'aged-form15': 'aged-prob15.html',
        },
        'forms': [
            {'title': 'Форма 1', 'title_text': 'Средний вес и длина по возрасту, размеру и полу', 'url': 'aged-form1',
             },
            {'title': 'Форма 2', 'title_text': 'Возрастной состав в процентах по месяцам', 'url': 'aged-form2',
             },
            {'title': 'Форма 3', 'title_text': 'Средняя жирность по месяцам и районам', 'url': 'aged-form3',
             },
            {'title': 'Форма 4', 'title_text': 'Средняя жирность и упитанность по возрасту, размеру и полу',
             'url': 'aged-form4'},
            {'title': 'Форма 11', 'title_text': 'Численность, средний вес и жирность по возрасту, размеру и полу',
             'url': 'aged-form11'},
            {'title': 'Форма 12', 'title_text': 'Численность по возрастам и стадиям зрелости', 'url': 'aged-form12',
             },
            {'title': 'Форма 13', 'title_text': 'Интенсивность питания и частота встречаемости пищевых организмов',
             'url': 'aged-form13'},
            {'title': 'Форма 15', 'title_text': 'Численность по размерам и стадиям зрелости', 'url': 'aged-form15',
             },
        ]
    },

    'tasks_biofox': {
        'title_page': 'BioFox',
        'discr_page': 'Расчетные задачи из комплекса BioFox',
        'alias': 'tasks_biofox',
        'html_forms': {
            # 'SvodBi-form': 'SvodBi-form.html',
            'svod-bi': 'SvodBi-form.html', #curr_form: name-form.html in the templates
            # 'aged-form2': 'aged-prob2.html',
            # 'aged-form3': 'aged-prob3.html',
            # 'aged-form4': 'aged-prob4.html',
            # 'aged-form11': 'aged-prob11.html',
            # 'aged-form12': 'aged-prob12.html',
            # 'aged-form13': 'aged-prob13.html',
            # 'aged-form15': 'aged-prob15.html',
        },
        'forms': [
            {'title': 'SvodBi', 'title_text': 'Комплексная обработка ихтиологической информации', 'url': 'svod-bi'},
            {'title': 'SvodPit', 'title_text': 'Спектр и частоты встречаемости пищевых объектов', 'url': 'svod-pit'},
            {'title': 'BioAn', 'title_text': 'Расчет средних биологических характеристик объектов', 'url': 'bio-an'},
            {'title': 'SvodRa', 'title_text': 'Формирование и обработка размерных рядов', 'url': 'svod-ra'}
        ]

    },

    'info_biofox': {
        'title_page': 'BioFox',
        'discr_page': 'Справки по запросу',
        'alias': 'info_biofox',
        'html_forms': {
            'bio-tral': 'biotral-form.html', #curr_form: name-form.html in the templates
            'bio-tral-extension': 'biotral-extension-form.html', 
            'bio-pap': 'biopap-form.html', 
            'bio-razm': 'biorazm-form.html', 
        },
        'forms': [
            {'title': 'BioTral', 'title_text': 'Карточки тралового лова', 'url': 'bio-tral'},
            {'title': 'Расширенная форма BioTral', 'title_text': 'Карточка тралового лова',
             'url': 'bio-tral-extension'},
            {'title': 'BioPap', 'title_text': 'Карточки биологического анализа', 'url': 'bio-pap'},
            {'title': 'BioRazm', 'title_text': 'Карточки массового помера', 'url': 'bio-razm'}
        ]
    },

    'analysis_nutrition': {
        'title_page': 'Анализ питания',
        'discr_page': 'Отчеты об анализе питания',
        'alias': 'analysis_nutrition',
        'forms': [
            {'title': 'Форма 1', 'title_text': 'Статистика собранных желудков', 'url': 'nutrition-form1'},
            {'title': 'Форма 2', 'title_text': 'Распределение сборов желудков', 'url': 'nutrition-form2'},
            {'title': 'Форма 3', 'title_text': 'Состав пищи в % по массе', 'url': 'nutrition-form3'},
            {'title': 'Форма 4', 'title_text': 'Карта значений жертвы в питании', 'url': 'nutrition-form4'},
            {'title': 'Форма 5', 'title_text': 'Расчёт потребления - вес пищевых организмов', 'url': 'nutrition-form5'},
            {'title': 'Форма 6', 'title_text': 'Размерный состав жертвы в пробе', 'url': 'nutrition-form6'},
            {'title': 'Форма 7', 'title_text': 'Состав пищи', 'url': 'nutrition-form7'},
            {'title': 'Форма 8', 'title_text': 'Рацион питания', 'url': 'nutrition-form8'}
        ]
    },

    'another_tasks': {
        'title_page': 'Анализ питания',
        'discr_page': 'Отчеты об анализе питания',
        'alias': 'another_tasks',
        'html_forms': {
            'another-form1': 'another_tasks1.html',
            'another-form9': 'razm-vozr-key.html',
            # 'aged-form3': 'aged-prob3.html',
            # 'aged-form4': 'aged-prob4.html',
            # 'aged-form11': 'aged-prob11.html',
            # 'aged-form12': 'aged-prob12.html',
            # 'aged-form13': 'aged-prob13.html',
            # 'aged-form15': 'aged-prob15.html',
        },
        'forms': [
            {'title': 'Форма 1', 'title_text': 'Опись материалов', 'url': 'another-form1'},
            {'title': 'Форма 2', 'title_text': 'Выгрузить файл для BIOFOX', 'url': 'another-form2'},
            {'title': 'Форма 3', 'title_text': 'Выгрузить файл для возрастной пробы', 'url': 'another-form3'},
            {'title': 'Форма 4', 'title_text': 'Файл по приловам', 'url': 'another-form4'},
            {'title': 'Форма 5', 'title_text': 'Файл для норвежцев', 'url': 'another-form5'},
            {'title': 'Форма 6', 'title_text': 'TXT-файл для трофологов', 'url': 'another-form6'},
            {'title': 'Форма 7', 'title_text': 'Файл для норвежцев(пелагические)', 'url': 'another-form7'},
            {'title': 'Форма 8', 'title_text': 'Выгрузить файл(трал. карт.) для ARCVIEW, SURFER',
             'url': 'another-form8'},
            {'title': 'Форма 9', 'title_text': 'Размерно-возрастной ключ', 'url': 'another-form9'},
            {'title': 'Форма 10', 'title_text': 'Выгрузить файл для ARCVIEW, SURFER', 'url': 'another-form10'},
            {'title': 'Форма 11', 'title_text': 'Выгрузить файл для рыб-бентофагов', 'url': 'another-form11'},
            {'title': 'Форма 12', 'title_text': 'Справка по объектам лова', 'url': 'another-form12'},
            {'title': 'Форма 13', 'title_text': 'Объем биоматериала в районах исследования ФГБНУ "ПИНРО"(по морям)',
             'url': 'another-form13'},
            {'title': 'Форма 14', 'title_text': 'Объем биоматериала НИС и НПС', 'url': 'another-form14'}
        ]
    }

}

views_dict = {
    'tral_view': '',
    'biopap_view': '',
    'ulov_view': '',
    'biopit_view': '',
    'razm_view': '',
}


def connect_db():
    conn = psycopg2.connect(host='host',
                            database='db',
                            user='user',
                            password='password')
    conn.cursor_factory = psycopg2.extras.DictCursor
    return conn


def get_db():
    """Соединение с БД, если оно еще не установлено"""
    if not hasattr(g, 'link_db'):
        g.link_db = connect_db()
    return g.link_db


dbase = None


@app.before_request
def before_request():
    """Установление соединения с БД перед выполнением запроса"""
    global dbase
    db = get_db()
    dbase = FDataBase(db)


@app.route("/")
def index():
    """Главная страница"""
    return render_template('index_main.html', title='Главная страница')

@app.route("/info")
def info():
    """Справка о наличии информации в базе"""
    df_info = dbase.info_data()
    print(df_info)
    if not df_info.empty:
        df_info_html = df_info.to_html(classes='table table-striped table-bordered zero-configuration')
    else:
        df_info_html = 'Нет информации'
    return render_template('info.html', df=df_info_html)


@app.route("/forms")
def forms():
    return render_template('forms.html', title='Формы', dict_card=dict_card)


@app.route("/forms/<alias>")
def sampling_form(alias):
    """Большая форма, где будет формироваться выборка, из которой
    будут формироваться остальные формочки"""
    alias = dict_report[alias].get('alias')
    forms_list = dict_report[alias].get('forms')
    # form_html = dict_report[alias].get('html_forms')[form]
    cards = 'cards'
    # print(alias)
    # print(forms_list)

    loc_dist = dbase.getLocalDictricts()
    eco_dict = dbase.getEcoDistricts()
    ikes_dict = dbase.getIkesDistricts()
    kod_fish_dict = dbase.getKodFish()
    dop_code_dict = dbase.getDopCode()
    kodpit_dict = dbase.getKodPit()
    vidlov_dict = dbase.getVidlov()
    orlov_dict = dbase.getOrlov()
    ship_reis_dict = dbase.getShipReis()
    pol_list = [None, 'D', 'F', 'I', 'J', 'M']

    if request.method == "POST":
        params_dict_base = {
            # траловая карточка
            'ship': tuple([i for i in request.form['ship'].split(',')]),
            'reis': tuple([int(i) for i in request.form['reis'].split(',')]),
        }

    return render_template('sample_form.html', loc_dist_list=loc_dist,
                           eco_dict_list=eco_dict, ikes_dict_list=ikes_dict, kod_fish_dict_list=kod_fish_dict,
                           dop_code_dict_list=dop_code_dict, kodpit_dict_list=kodpit_dict, vidlov_dict_list=vidlov_dict,
                           orlov_dict_list=orlov_dict, pol_list=pol_list,ship_reis_dict=ship_reis_dict,
                           alias=alias)


@app.route("/forms/<path:alias>/sample_queries", methods=["POST", "GET"])
def sample_queries(alias):
    forms_list = dict_report[alias].get('forms')

    if request.method == "POST":
        params_dict_base = {
            # траловая карточка
            'tral_card': {
                # 'ship': tuple([i for i in request.form['ship'].split(',')]),
                # 'reis': tuple([int(i) for i in request.form['reis'].split(',')]),
                'ship_reis': tuple([i for i in request.form.getlist('ship_reis')]),
                'ship_reis_except': tuple([i for i in request.form.getlist('ship_reis_except')]),

                'trl': request.form.get('trl', None),
                'start_trl': request.form.get('start_trl', None),
                'end_trl': request.form.get('end_trl', None),

                'date': request.form.get('date', None),
                'start_date': request.form.get('start_date', None),
                'end_date': request.form.get('end_date', None),

                'shir': request.form.get('shir', None),
                'start_shir': request.form.get('start_shir', None),
                'end_shir': request.form.get('end_shir', None),

                'dolg': request.form.get('dolg', None),
                'start_dolg': request.form.get('start_dolg', None),
                'end_dolg': request.form.get('end_dolg', None),

                'glub': request.form.get('glub', None),
                'start_glub': request.form.get('start_glub', None),
                'end_glub': request.form.get('end_glub', None),

                'gor': request.form.get('gor', None),
                'start_gor_tral': request.form.get('start_gor_tral', None),
                'end_gor_tral': request.form.get('end_gor_tral', None),

                'start_kutok': request.form.get('start_kutok', None),
                'end_kutok': request.form.get('end_kutok', None),

                'start_rub': request.form.get('start_rub', None),
                'end_rub': request.form.get('end_rub', None),

                'start_sel_resh': request.form.get('start_sel_resh', None),
                'end_sel_resh': request.form.get('end_sel_resh', None),

                'filming': None if not request.form.get('filming') else request.form.get('filming'),
                'monitoring': None if not request.form.get('monitoring') else request.form.get('monitoring'),
                'registry': None if not request.form.get('registry') else request.form.get('registry'),
                'all_trals': None if not request.form.get('all_trals') else request.form.get('all_trals'),

                'ulov': request.form.get('ulov', None),
                'start_all_ulov': request.form.get('start_all_ulov', None),
                'end_all_ulov': request.form.get('end_all_ulov', None),

                'vidlov': tuple([i for i in request.form.getlist('vidlov')]),
                'orlov': tuple([i for i in request.form.getlist('orlov')]),

                'prod': request.form.get('prod', None),
                'start_prod_lov': request.form.get('start_prod_lov', None),
                'end_prod_lov': request.form.get('end_prod_lov', None),

                'prom_kv': request.form.get('prom_kv', None),
                'rnorv': request.form.get('rnorv', None),

                # 'loc': request.form.getlist('loc', None),
                'loc': tuple([i for i in request.form.getlist('loc')]),
                'eco': tuple([i for i in request.form.getlist('eco')]),
                'ikes': tuple([i for i in request.form.getlist('ikes')]),
            },

            'ulov_card': {
                'u_key': tuple([i for i in request.form.getlist('dopcode')]),
                'no_u_key': None if not request.form.get('no_dopcode') else request.form.get('no_dopcode'),

                'u_wes_operator': None if request.form['u_wes_operator'] == '' else request.form['u_wes_operator'],
                'u_wes': None if request.form['u_wes'] == '' else request.form['u_wes'],
                'u_prc_operator': None if request.form['u_prc_operator'] == '' else request.form['u_prc_operator'],
                'u_prc': request.form.get('u_prc', None),
            },

            'biopap_card': {
                'kodpit': tuple([i for i in request.form.getlist('kodpit')]),
                # 'vidlov': tuple([i for i in request.form.getlist('vidlov')]),
                # 'orlov': tuple([i for i in request.form.getlist('orlov')]),

                'zvozrp': None if not request.form.get('zvozrp') else request.form.get('zvozrp'),
                'zpolan': None if not request.form.get('zpolan') else request.form.get('zpolan'),
                'zskap': None if not request.form.get('zskap') else request.form.get('zskap'),
                'znlab': None if not request.form.get('znlab') else request.form.get('znlab'),

                # 'general': None if not request.form.get('general') else request.form.get('general'),
                # 'norway': None if not request.form.get('norway') else request.form.get('norway'),
                # 'yarus': None if not request.form.get('yarus') else request.form.get('yarus'),

                'proba': request.form.get('proba', None),
                'start_proba': request.form.get('start_proba', None),
                'end_proba': request.form.get('end_proba', None),

                'start_len': request.form.get('start_len', None),
                'end_len': request.form.get('end_len', None),

                'wes_ob_operator': request.form.get('wes_ob_operator', None),
                'wes_ob': request.form.get('wes_ob', None),

                'pol': tuple([i for i in request.form.getlist('pol')]),

                'vozr1_operator': request.form.get('vozr1_operator', None),
                'vozr1': None if request.form['vozr1'] == '' else request.form['vozr1'],
                'vozr2': None if request.form['vozr2'] == '' else request.form['vozr2'],

                'zrel_operator': None if request.form['zrel_operator'] == '' else request.form['zrel_operator'],
                'zrel': None if request.form['zrel'] == '' else request.form['zrel'],

                'start_nring': request.form.get('start_nring', None),
                'end_nring': request.form.get('end_nring', None),
            },

            'fish_list': {
                'kod_fish': tuple([i for i in request.form.getlist('kod_fish')]),
            },

            'razm_card': {
                'ship_reis': tuple([i for i in request.form.getlist('ship_reis')]),
                'ship_reis_except': tuple([i for i in request.form.getlist('ship_reis_except')]),

                'trl': request.form.get('trl', None),
                'start_trl': request.form.get('start_trl', None),
                'end_trl': request.form.get('end_trl', None),

                'pol': tuple([i for i in request.form.getlist('pol')]),

                'u_key': tuple([i for i in request.form.getlist('dopcode')]),
                'no_u_key': None if not request.form.get('no_dopcode') else request.form.get('no_dopcode'),
            }

        }

        a = params_dict_base['tral_card'].get('ship_reis')
        b = []

        tral_view_qur = dbase.generate_sample_tral(params_dict_base)
        biopap_view_qur = dbase.generate_sample_biopap(params_dict_base)
        ulov_view_qur = dbase.generate_sample_ulov(params_dict_base)
        biopit_view_qur = dbase.generate_sample_biopit(params_dict_base)
        razm_view_qur = dbase.generate_sample_razm(params_dict_base)

        views_dict['tral_view'] = tral_view_qur
        views_dict['biopap_view'] = biopap_view_qur
        views_dict['biopit_view'] = biopit_view_qur
        views_dict['ulov_view'] = ulov_view_qur
        views_dict['razm_view'] = razm_view_qur

        # print(views_dict['tral_view'])

        # fill_params_tral = dbase.correct_params(params_dict_base['tral_card'])
        fill_params_tral = params_dict_base['tral_card']
        # print(fill_params_tral)
        fill_params_ulov = dbase.correct_params(params_dict_base['ulov_card'])
        fill_params_biopap = dbase.correct_params(params_dict_base['biopap_card'])
        fill_params_fish = dbase.correct_params(params_dict_base['fish_list'])

        return render_template('cards.html', forms_list=forms_list, tral_params=fill_params_tral,
                               ulov_params=fill_params_ulov, biopap_params=fill_params_biopap,
                               fish_params=fill_params_fish,
                               alias=alias)


@app.route("/forms/<path:alias>/sample_queries/<path:curr_form>")
def open_form(alias, curr_form):
    # print(curr_form)
    html_form = dict_report[alias].get('html_forms')[curr_form]
    # print(curr_form)

    return render_template(html_form, alias=alias, curr_form=curr_form)

# @app.route("/forms/<path:alias>/sample_queries")


@app.route("/forms/<path:alias>/sample_queries/<path:curr_form>/report", methods=["POST", "GET"])
def open_report(alias, curr_form):
    dict_params = {
        'aged-form1': {
            'len_count': None if not request.form.get('len_count') else request.form.get('len_count'),
            'start_row': request.form.get('start_row', None),
            'counting_step': request.form.get('counting_step', None),
            'page': 'aged-prob_pages/aged-prob1_page.html'
        },
        'aged-form2': {
            'page': 'aged-prob_pages/aged-prob2_page.html',
        },
        'aged-form3': {
            'page': 'aged-prob_pages/aged-prob3_page.html'
        },
        'aged-form4': {
            'len_count': None if not request.form.get('len_count') else request.form.get('len_count'),
            'start_row': request.form.get('start_row', None),
            'counting_step': request.form.get('counting_step', None),
            'page': 'aged-prob_pages/aged-prob4_page.html'
        },
        'aged-form11': {
            'len_count': None if not request.form.get('len_count') else request.form.get('len_count'),
            'start_row': request.form.get('start_row', None),
            'counting_step': request.form.get('counting_step', None),
            'page': 'aged-prob_pages/aged-prob11_page.html'
        },
        'aged-form12': {
            'start_npzrel': None if not request.form.get('start_npzrel') else request.form.get('start_npzrel'),
            'end_npzrel': None if not request.form.get('end_npzrel') else request.form.get('end_npzrel'),
            'start_zrel': None if not request.form.get('start_zrel') else request.form.get('start_zrel'),
            'end_zrel': None if not request.form.get('end_zrel') else request.form.get('end_zrel'),

            'start_period1': None if not request.form.get('start_period1') else request.form.get('start_period1'),
            'end_period1': None if not request.form.get('end_period1') else request.form.get('end_period1'),
            'start_period2': None if not request.form.get('start_period2') else request.form.get('start_period2'),
            'end_period2': None if not request.form.get('end_period2') else request.form.get('end_period2'),

            'page': 'aged-prob_pages/aged-prob12_page.html'
        },
        'aged-form13': {
            'page': 'aged-prob_pages/aged-prob13_page.html'
        },
        'aged-form15': {
            'start_npzrel': None if not request.form.get('start_npzrel') else request.form.get('start_npzrel'),
            'end_npzrel': None if not request.form.get('end_npzrel') else request.form.get('end_npzrel'),
            'start_zrel': None if not request.form.get('start_zrel') else request.form.get('start_zrel'),
            'end_zrel': None if not request.form.get('end_zrel') else request.form.get('end_zrel'),

            # 'start_period1': None if not request.form.get('start_period1') else request.form.get('start_period1'),
            # 'end_period1': None if not request.form.get('end_period1') else request.form.get('end_period1'),
            # 'start_period2': None if not request.form.get('start_period2') else request.form.get('start_period2'),
            # 'end_period2': None if not request.form.get('end_period2') else request.form.get('end_period2'),

            # 'len_count': None if not request.form.get('len_count') else request.form.get('len_count'),
            'start_row': request.form.get('start_row', None),
            'counting_step': request.form.get('counting_step', None),

            'page': 'aged-prob_pages/aged-prob15_page.html'
        },
# справки из биофокс
        'bio-tral' : {
            'page': 'info_biofox_pages/biotral_page.html'
        },
        'bio-tral-extension' : {
            'page': 'info_biofox_pages/biotral-extension_page.html'
        },
        'bio-pap': {
            'len_count': None if not request.form.get('len_count') else request.form.get('len_count'),
            'fat_count': None if not request.form.get('fat_count') else request.form.get('fat_count'),
            'page': 'info_biofox_pages/bio-pap_page.html'
        },
        'bio-razm': {
            'by_fish': None if not request.form.get('by_fish') else request.form.get('by_fish'),
            'type_len': request.form.get('type_len', None),
            'by_M': None if not request.form.get('by_M') else request.form.get('by_M'),
            'by_F': None if not request.form.get('by_F') else request.form.get('by_F'),
            'by_J': None if not request.form.get('by_J') else request.form.get('by_J'),
            'by_I': None if not request.form.get('by_I') else request.form.get('by_I'),
            'by_all_pol': None if not request.form.get('by_all_pol') else request.form.get('by_all_pol'),

            'page': 'info_biofox_pages/bio-razm_page.html'
        },
# другие задачи
        'svod-bi': {
            'len_count': None if not request.form.get('len_count') else request.form.get('len_count'),
            'shift_row': request.form.get('shift_row', None),
            'fat_count': None if not request.form.get('fat_count') else request.form.get('fat_count'),
            'counting_step': request.form.get('counting_step', None),
            'by_glub': None if not request.form.get('by_glub') else request.form.get('by_glub'),
            'by_fish': None if not request.form.get('by_fish') else request.form.get('by_fish'),
            'by_year': None if not request.form.get('by_year') else request.form.get('by_year'),
            'by_month': None if not request.form.get('by_month') else request.form.get('by_month'),
            'page': 'tasks-biofox_pages/svod-bi_page.html'
        },
        'another-form1': {
            'by_what': None if not request.form.get('by_what') else request.form.get('by_what'),
            'only_itog': None if not request.form.get('only_itog') else request.form.get('only_itog'),

            'page': 'another-task_pages/another-task1_page.html'
        },
        'another-form9': {
            'start_row': request.form.get('start_row', None),
            'counting_step': request.form.get('counting_step', None),
            'end_row': request.form.get('end_row', None),

            'page': 'another-task_pages/razm-vozr-key_page.html'
        },

    }

    params_form = dict_params[curr_form]  # len_count, start_row, counting_step, page
    # print(params_form)
    result = dbase.general_view(params_form, curr_form, views_dict)
    df = pd.DataFrame(result)
    # print(curr_form)

# aged-prob
    if curr_form == 'aged-form1':
        res_t = dbase.datas_for_aged_prob1([params_form.get('len_count'), params_form.get('start_row'), params_form.get('counting_step')])
    elif curr_form == 'aged-form2':
        res_t = dbase.datas_for_aged_prob2()
    elif curr_form == 'aged-form3':
        res_t = dbase.datas_for_aged_prob3()
    elif curr_form == 'aged-form4':
        res_t = dbase.datas_for_aged_prob4(
            [params_form.get('len_count'), params_form.get('start_row'), params_form.get('counting_step')])
    elif curr_form == 'aged-form11':
        res_t = dbase.datas_for_aged_prob11(
            [params_form.get('len_count'), params_form.get('start_row'), params_form.get('counting_step')])
    elif curr_form == 'aged-form12':
        res_t = dbase.datas_for_aged_prob12(
            [params_form.get('start_npzrel'), params_form.get('end_npzrel'), params_form.get('start_zrel'), params_form.get('end_zrel'),
             params_form.get('start_period1'), params_form.get('end_period1'), params_form.get('start_period2'), params_form.get('end_period2')])
    elif curr_form == 'aged-form13':
        res_t = dbase.datas_for_aged_prob13()
    elif curr_form == 'aged-form15':
        res_t = dbase.datas_for_aged_prob15(
            [params_form.get('start_npzrel'), params_form.get('end_npzrel'), params_form.get('start_zrel'),
             params_form.get('end_zrel'), params_form.get('start_row'), params_form.get('counting_step'), ])

# справки по запросу
    elif curr_form == 'bio-tral':
        res_t = dbase.datas_for_biotral(curr_form)
    elif curr_form == 'bio-tral-extension':
        res_t = dbase.datas_for_biotral(curr_form)
    elif curr_form == 'bio-pap':
        res_t = dbase.datas_for_biopap([params_form.setdefault('len_count'), params_form.setdefault('fat_count')])
    elif curr_form == 'bio-razm':
        res_t = dbase.datas_for_biorazm([params_form.setdefault('by_fish'), params_form.setdefault('type_len'), params_form.setdefault('by_M'),
                                        params_form.setdefault('by_F'),params_form.setdefault('by_J'),params_form.setdefault('by_I'),
                                        params_form.setdefault('by_all_pol')])
        # res_t = dbase.datas_for_biotral_extension()
        
# расчетные задачи
    elif curr_form == 'svod-bi':
        main_res = dbase.datas_for_svodbi([params_form.setdefault('len_count'), params_form.setdefault('shift_row'), params_form.setdefault('fat_count'), 
                                           params_form.setdefault('counting_step'), params_form.setdefault('by_glub'), 
                                           params_form.setdefault('by_year'), params_form.setdefault('by_month'), params_form.setdefault('by_fish')])
        d = main_res[0]

# another-tasks
    elif curr_form == 'another-form1':
        res_t = dbase.datas_for_another_task1([params_form.setdefault('by_what'), params_form.setdefault('only_itog')])
    elif curr_form == 'another-form9':
        res_t = dbase.datas_for_another_task9([params_form.setdefault('start_row'), params_form.setdefault('counting_step'),
                                               params_form.setdefault('end_row')])


    if not df.empty:
        df_html = df.to_html(classes='table table-striped table-bordered zero-configuration')
        if curr_form == 'svod-bi':
            for key, val in d.items():
                for i in range(len(val)):
                    val[i] = val[i].to_html(classes='table table-striped table-bordered zero-configuration')
            return render_template(dict_params[curr_form].get('page'), df_html=df_html, parent_list=d)
        else:
            for i in range(len(res_t)):
                print(res_t[i])
                res_t[i] = res_t[i].to_html(classes='table table-striped table-bordered zero-configuration')

            return render_template(dict_params[curr_form].get('page'), df_html=df_html, datas=res_t)
    else:
        df_html = 'Нет информации'

        return render_template(dict_params[curr_form].get('page'), df_html=df_html)


@app.teardown_appcontext
def close_db(error):
    """Закрываем соединение с БД, если оно было установлено"""
    if hasattr(g, 'link_db'):
        g.link_db.close()


@app.errorhandler(404)
def pageNotFound(error):
    return 'Страница не найдена', 404


if __name__ == "__main__":
    app.run(debug=True)
