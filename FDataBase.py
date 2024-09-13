import math
import sqlite3
import time

import pandas as pd
import numpy as np


class FDataBase:
    def __init__(self, db):
        self.__db = db
        self.__cur = db.cursor()

    def exec_view(self, sql):
        self.__cur.execute(sql)
        res = self.__cur.fetchall()
        if res:
            return res
        else:
            return "No info"

    def make_ship_reis_str(self, str, ship_reis_dict, table):
        str += ' AND ('
        for idx, (ship, reis) in enumerate(ship_reis_dict.items()):
            if len(reis) == 1:
                str += f" ({table}.ship = '{ship}' and {table}.reis = {reis[0]}) "
            elif len(reis) > 1:
                str += f" ({table}.ship = '{ship}' and {table}.reis IN {tuple(reis)}) "
            if idx < len(ship_reis_dict) - 1:
                str += " OR "
        str += ')'
        return str
    
    def make_ship_reis_str_except(self, str, ship_reis_dict, table):
        str += ' AND NOT('
        for idx, (ship, reis) in enumerate(ship_reis_dict.items()):
            if len(reis) == 1:
                str += f" ({table}.ship = '{ship}' and {table}.reis = {reis[0]}) "
            elif len(reis) > 1:
                str += f" ({table}.ship = '{ship}' and {table}.reis IN {tuple(reis)}) "
            if idx < len(ship_reis_dict) - 1:
                str += " OR "
        str += ')'
        return str

    def make_dict_request(self, sql):
        self.__cur.execute(sql)
        res = self.__cur.fetchall()
        columns = [column[0] for column in self.__cur.description]
        results = []
        if res:
            for row in res:
                results.append(dict(zip(columns, row)))

            return results

    # собираем строки-параметры для траловой карточки
    def make_tral_str(self, params_tral_card, view):
        if params_tral_card.get('ship_reis'):
            if len(params_tral_card.get('ship_reis')) > 1:
                lst = [*params_tral_card.get('ship_reis')]
                sq_lst = self.make_ship_reis(lst)
                view = self.make_ship_reis_str(view, sq_lst, 't')
            else:
                param = [*params_tral_card.get('ship_reis')][0]
                ship = param[0:4]
                reis = param[5:]

                view += f" AND t.ship='{ship}' AND t.reis={reis}"
                print(param)

        if params_tral_card.get('ship_reis_except'):
            if len(params_tral_card.get('ship_reis_except')) > 1:
                lst = [*params_tral_card.get('ship_reis_except')]
                sq_lst = self.make_ship_reis(lst)
                view = self.make_ship_reis_str_except(view, sq_lst, 't')
            else:
                param = [*params_tral_card.get('ship_reis_except')][0]
                ship = param[0:4]
                reis = param[5:]

                view += f" AND t.ship != '{ship}' AND t.reis != {reis}"
                print(param)

        if params_tral_card.get('trl'):
            view += f" AND t.trl IN ({params_tral_card['trl']})"
        if params_tral_card.get('start_trl') and params_tral_card.get('end_trl'):
            view += f" AND t.trl BETWEEN {params_tral_card['start_trl']} AND {params_tral_card['end_trl']}"

        if params_tral_card.get('date'):
            view += f" AND t.data=TO_DATE('{params_tral_card['date']}','YYYY-MM-DD')"
        if params_tral_card.get('start_date') and params_tral_card.get('end_date'):
            view += f" AND t.data BETWEEN TO_DATE('{params_tral_card['start_date']}','YYYY-MM-DD') AND TO_DATE('{params_tral_card['end_date']}', 'YYYY-MM-DD')"

        if params_tral_card.get('shir'):
            view += f" AND t.shir IN ({params_tral_card['shir']})"
        if params_tral_card.get('start_shir') and params_tral_card.get('end_shir'):
            view += f" AND t.shir BETWEEN {params_tral_card['start_shir']} AND {params_tral_card['end_shir']}"

        if params_tral_card.get('dolg'):
            view += f" AND t.dolg IN ({params_tral_card['dolg']})"
        if params_tral_card.get('start_dolg') and params_tral_card.get('end_dolg'):
            view += f" AND t.dolg BETWEEN {params_tral_card['start_dolg']} AND {params_tral_card['end_dolg']}"

        if params_tral_card.get('glub'):
            view += f" AND t.glub IN ({params_tral_card['glub']})"
        if params_tral_card.get('start_glub') and params_tral_card.get('end_glub'):
            view += f" AND t.glub BETWEEN {params_tral_card['start_glub']} AND {params_tral_card['end_glub']}"

        if params_tral_card.get('gor'):
            view += f" AND t.gor IN ({params_tral_card['gor']})"
        if params_tral_card.get('start_gor_tral') and params_tral_card.get('end_gor_tral'):
            view += f" AND t.gor BETWEEN {params_tral_card['start_gor_tral']} AND {params_tral_card['end_gor_tral']}"

        if params_tral_card.get('start_kutok') and params_tral_card.get('end_kutok'):
            view += f" AND t.kutok BETWEEN {params_tral_card['start_kutok']} AND {params_tral_card['end_kutok']}"

        if params_tral_card.get('start_rub') and params_tral_card.get('end_rub'):
            view += f" AND t.rub BETWEEN {params_tral_card['start_rub']} AND {params_tral_card['end_rub']}"

        if params_tral_card.get('start_sel_resh') and params_tral_card.get('end_sel_resh'):
            view += f" AND t.sel_r BETWEEN {params_tral_card['start_sel_resh']} AND {params_tral_card['end_sel_resh']}"

        if params_tral_card.get('filming') and not params_tral_card.get('monitoring'):
            view += f" AND t.tral_info=1"
        if params_tral_card.get('monitoring') and not params_tral_card.get('filming'):
            view += f" AND t.tral_info=0"
        if params_tral_card.get('monitoring') and params_tral_card.get('filming'):
            view += f" AND (t.tral_info=1 or t.tral_info=0)"

        if params_tral_card.get('registry') and not params_tral_card.get('all_trals'):
            view += f" AND t.uchet=1"
        if params_tral_card.get('all_trals') and not params_tral_card.get('registry'):
            view += f" AND t.uchet=0"
        if params_tral_card.get('all_trals') and params_tral_card.get('registry'):
            view += f" AND (t.uchet=0 or t.uchet=1)"

        if params_tral_card.get('ulov'):
            view += f" AND t.ulov={params_tral_card['ulov']}"
        if params_tral_card.get('start_all_ulov') and not params_tral_card.get('end_all_ulov'):
            view += f" AND t.ulov BETWEEN {params_tral_card['start_all_ulov']} AND {params_tral_card['end_all_ulov']}"

        if params_tral_card.get('vidlov'): # ! оно в трале
            list_vidlov = [*params_tral_card.get('vidlov')]
            if len(list_vidlov) > 1:
                view += f" AND t.vidlov IN {(*list_vidlov,)}"
            else:
                view += f" AND t.vidlov IN ({(list_vidlov[0])})"

        if params_tral_card.get('orlov'): # ! оно в трале
            list_orlov = [*params_tral_card.get('orlov')]
            if len(list_orlov) > 1:
                view += f" AND t.orlov IN {(*list_orlov,)}"
            else:
                view += f" AND t.orlov IN ({(list_orlov[0])})"

        if params_tral_card.get('prod'):
            view += f" AND t.prod={params_tral_card['prod']}"
        if params_tral_card.get('start_prod_lov') and not params_tral_card.get('end_prod_lov'):
            view += f" AND t.prod BETWEEN {params_tral_card['start_prod_lov']} AND {params_tral_card['end_prod_lov']}"

        if params_tral_card.get('prom_kv'):
            view += f" AND t.prom_kv IN ({params_tral_card['prom_kv']})"
        if params_tral_card.get('rnorv'):
            view += f" AND t.rnorv IN ({params_tral_card['rnorv']})"

        if params_tral_card.get('loc'):
            view += ' AND'
            N_locs = len(params_tral_card.get('loc'))
            locs_tuple = params_tral_card.get('loc')

            for i in range(N_locs):
                curr_val = int(locs_tuple[i])
                if curr_val % 1000 == 0:
                    end_val = (curr_val + 1000)-1
                    view += f" t.rloc BETWEEN {curr_val} and {end_val}"
                    # strs.append(f" t.rloc BETWEEN {curr_val} and {end_val}")               
                elif curr_val % 100 == 0:
                    end_val = (curr_val + 100)-1
                    view += f" t.rloc BETWEEN {curr_val} and {end_val}"
                    # strs.append(f" t.rloc BETWEEN {curr_val} and {end_val}")
                elif curr_val % 10 == 0:
                    end_val = (curr_val + 10)-1
                    view += f" t.rloc BETWEEN {curr_val} and {end_val}"
                    # strs.append(f" t.rloc BETWEEN {curr_val} and {end_val}")
                else:
                    view += f" t.rloc={curr_val}"
                    # strs.append(f" t.rloc={curr_val}")

                if i < len(locs_tuple) - 1:
                    view += " OR "
                # view += ' OR'
            
        if params_tral_card.get('eco'):
            list_eco = [*params_tral_card['eco']]
            if len(list_eco) > 1:
                print(list_eco)
                view += f" AND t.rzon IN {(*list_eco,)}"
            else:
                view += f" AND t.rzon IN ({(list_eco[0])})"

        if params_tral_card.get('ikes'):
            view += ' AND'
            N_ikes = len(params_tral_card.get('ikes'))
            ikes_tuple = params_tral_card.get('ikes')

            for i in range(N_ikes):
                curr_val = int(ikes_tuple[i])
                if curr_val % 100000 == 0:
                    end_val = (curr_val + 100000)-1
                    view += f" t.rikes BETWEEN {curr_val} and {end_val}"
                elif curr_val % 10000 == 0:
                    end_val = (curr_val + 10000)-1
                    view += f" t.rikes BETWEEN {curr_val} and {end_val}"
                elif curr_val % 1000 == 0:
                    end_val = (curr_val + 1000)-1
                    view += f" t.rikes BETWEEN {curr_val} and {end_val}"
                    # strs.append(f" t.rloc BETWEEN {curr_val} and {end_val}")               
                elif curr_val % 100 == 0:
                    end_val = (curr_val + 100)-1
                    view += f" t.rikes BETWEEN {curr_val} and {end_val}"
                    # strs.append(f" t.rloc BETWEEN {curr_val} and {end_val}")
                elif curr_val % 10 == 0:
                    end_val = (curr_val + 10)-1
                    view += f" t.rikes BETWEEN {curr_val} and {end_val}"
                    # strs.append(f" t.rloc BETWEEN {curr_val} and {end_val}")
                else:
                    view += f" t.rikes={curr_val}"
                    # strs.append(f" t.rloc={curr_val}")

                if i < len(ikes_tuple) - 1:
                    view += " OR "

            # view += f" AND t.rikes IN {params_tral_card['ikes']}"
        # //tral
        return view

    # собираем строки-параметры для биологического анализа
    def make_biopap_str(self, params_biopap_card, biopap_view):
        if params_biopap_card.get('vozr1') and params_biopap_card.get('vozr1_operator'):
            biopap_view += f" AND b.vozr1{params_biopap_card['vozr1_operator']}{params_biopap_card['vozr1']}"
        if params_biopap_card.get('vozr2'):
            biopap_view += f" AND b.vozr2={params_biopap_card['vozr2']}"

        if params_biopap_card.get('kodpit'): # ! а это вообще в биопите ???
            list_kodpit = [*params_biopap_card.get('kodpit')]
            if len(list_kodpit) > 1:
                print(list_kodpit)
                biopap_view += f" AND bi.kodpit IN {(*list_kodpit,)}"
            else:
                biopap_view += f" AND bi.kodpit IN ({(list_kodpit[0])})"

        # if params_biopap_card.get('vidlov'): # ! оно в трале
        #     list_vidlov = [*params_biopap_card.get('vidlov')]
        #     if len(list_vidlov) > 1:
        #         biopap_view += f" AND t.vidlov IN {(*list_vidlov,)}"
        #     else:
        #         biopap_view += f" AND t.vidlov IN ({(list_vidlov[0])})"

        # if params_biopap_card.get('orlov'): # ! оно в трале
        #     list_orlov = [*params_biopap_card.get('orlov')]
        #     if len(list_orlov) > 1:
        #         biopap_view += f" AND t.orlov IN {(*list_orlov,)}"
        #     else:
        #         biopap_view += f" AND t.orlov IN ({(list_orlov[0])})"
            # print(biopap_view)

        if params_biopap_card.get('zvozrp') and not params_biopap_card.get('zpolan'):
            biopap_view += f" AND b.bio_info=0"
        if params_biopap_card.get('zpolan') and not params_biopap_card.get('zvozrp'):
            biopap_view += f" AND b.bio_info=1"
        if params_biopap_card.get('zpolan') and params_biopap_card.get('zvozrp'):
            biopap_view += f" AND (b.bio_info=0 OR b.bio_info=1)"
        if params_biopap_card.get('zskap'):
            biopap_view += f" AND b.skap_info=1"
        if params_biopap_card.get('znlab'):
            biopap_view += f" AND b.menu_kap=1"

        if params_biopap_card.get('proba'):
            biopap_view += f" AND b.proba IN ({params_biopap_card['proba']})"
        if params_biopap_card.get('start_proba') and params_biopap_card.get('end_proba'):
            biopap_view += f" AND b.proba BETWEEN {params_biopap_card['start_proba']} and {params_biopap_card['end_proba']}"

        if params_biopap_card.get('start_len') and params_biopap_card.get('end_len'):
            biopap_view += f" AND b.l_big BETWEEN {params_biopap_card['start_len']} AND {params_biopap_card['end_len']}"

        if params_biopap_card.get('wes_ob_operator') and params_biopap_card.get('wes_ob'):
            biopap_view += f" AND b.wes_ob{params_biopap_card['wes_ob_operator']}{params_biopap_card['wes_ob']}"

        if params_biopap_card.get('pol'):
            biopap_view += f" AND b.pol IN {params_biopap_card['pol']}"

        if params_biopap_card.get('zrel_operator') and params_biopap_card.get('zrel'):
            biopap_view += f" AND b.zrel{params_biopap_card['zrel_operator']}{params_biopap_card['zrel']}"

        if params_biopap_card.get('start_nring') and params_biopap_card.get('end_nring'):
            biopap_view += f" AND b.n_ring BETWEEN {params_biopap_card['start_nring']}{params_biopap_card['end_nring']}"

        return biopap_view

    # собираем строки-параметры для карточки улова
    def make_ulov_str(self, params_ulov_card, biopap_view):
        if params_ulov_card.get('u_key'):
            list_ukey = [*params_ulov_card.get('u_key')]
            if len(list_ukey) > 1:
                biopap_view += f" AND u.u_key IN {(*list_ukey,)}"
            else:
                biopap_view += f" AND u.u_key IN ('{(list_ukey[0])}')"

        if params_ulov_card.get('no_u_key'):
            biopap_view += f" AND u.u_key IN ('')"

        if params_ulov_card.get('u_wes_operator') and params_ulov_card.get('u_wes'):
            biopap_view += f" AND u.u_wes{params_ulov_card['u_wes_operator']}{params_ulov_card['u_wes']}"
        if params_ulov_card.get('u_prc_operator') and params_ulov_card.get('u_prc'):
            biopap_view += f" AND u.u_prc{params_ulov_card['u_prc_operator']}{params_ulov_card['u_prc']}"

        return biopap_view

    # ============
    # корректировка полученных значений в формат SQL-строк
    def correct_params(self, params_dict):
        for key, val in params_dict.items():
            # print(key)
            if key == 'ship_reis':
                # print(key)
                continue
            if params_dict[key]:
                # continue
                if isinstance(params_dict[key], int) or isinstance(params_dict[key], str):
                    params_dict[key] = val
                    # print(val)
                # elif len(params_dict[key]) == 1:
                #     if type(val[0]) == str and val[0] != '':
                #         params_dict[key] = f"('{val[0]}')"
                #     elif val[0] == '':
                #         params_dict[key] = None
                #     else:
                #         params_dict[key] = f"({val[0]})"

            else:
                params_dict[key] = None

        return params_dict

    # формирование строки с позывным и номером рейса
    def make_ship_reis(self, ship_reis_lst):
        ship_reis_dict = dict() #d
        for i in ship_reis_lst:
            ship = i[0:4]
            reis = i[5:]
            if ship in ship_reis_dict:
                ship_reis_dict[ship].append(reis)
            else:
                ship_reis_dict[ship] = [reis]

        return ship_reis_dict

    # ============
    # формируется представление TRAL_VIEW
    # def generate_sample_tral(self, params):
    def generate_sample_tral(self, params):
        # CREATE OR REPLACE TEMP VIEW
        tral_view = f"""
        CREATE OR REPLACE TEMP VIEW tral_view AS
        SELECT distinct t.*
        FROM tral t
        """

        # print(params['tral_card'])

        params_tral_card = self.correct_params(params['tral_card'])
        # params_tral_card = params['tral_card']
        params_ulov_card = self.correct_params(params['ulov_card'])
        params_biopap_card = self.correct_params(params['biopap_card'])
        params_fishes = self.correct_params(params['fish_list'])

        # # Проверяем, есть ли хотя бы одно непустое значение в словаре
        one_value_not_none_Tral = any(value is not None for value in params_tral_card.values())
        one_value_not_none_Ulov = any(value is not None for value in params_ulov_card.values())
        one_value_not_none_Biopap = any(value is not None for value in params_biopap_card.values())
        one_value_not_none_Fishes = any(value is not None for value in params_fishes.values())

        # ship_reis_dict = self.make_ship_reis(params_tral_card['ship_reis'])

        # print(params_biopap_card)

        # джойны.
        # собираем фулл запрос для выборки TRAL
        # only ship,reis \\ только параметры tral
        if one_value_not_none_Tral and not (
                one_value_not_none_Ulov or one_value_not_none_Biopap or one_value_not_none_Fishes):
            tral_view += """\nWHERE 1=1 """
            tral_view = self.make_tral_str(params_tral_card, tral_view)
            # tral_view = self.make_ship_reis_str(tral_view, ship_reis_dict, 't')
            tral_view += """;"""

        # # ship,reis,vozr1 \\ параметры из tral + прочие параметры из biopap
        elif one_value_not_none_Tral and one_value_not_none_Biopap and not (
                one_value_not_none_Ulov or one_value_not_none_Fishes):
            tral_view += """\nJOIN ulov u ON t.trkl = u.trkl\nJOIN biopap b ON u.ulkl = b.ulkl\nJOIN biopit bi ON bi.bikl = b.bikl"""
            tral_view += """\nWHERE 1=1 """
            # ulov ship reis
            # tral_view = self.make_ship_reis_str(tral_view, ship_reis_dict, 'u')
            tral_view = self.make_tral_str(params_tral_card, tral_view)

            # biopap params
            tral_view = self.make_biopap_str(params_biopap_card, tral_view)
            # // biopap

            tral_view += """;"""

        # # ship,reis,kodvid \\ параметры tral + kodvid
        elif one_value_not_none_Tral and one_value_not_none_Fishes and not (
                one_value_not_none_Ulov or one_value_not_none_Biopap):
            tral_view += """\nJOIN ulov u ON t.trkl = u.trkl"""
            tral_view += """\nWHERE 1=1 """

            tral_view = self.make_tral_str(params_tral_card, tral_view)

            if params_fishes.get('kod_fish'):
                list_fishes = [*params_fishes.get('kod_fish')]
                if len(list_fishes) > 1:
                    # print(list_fishes)
                    tral_view += f" AND u.kodvid IN {(*list_fishes,)}"
                else:
                    tral_view += f" AND u.kodvid IN ({(list_fishes[0])})"

            tral_view += """;"""

                # tral_view += f" AND u.kodvid IN {params_fishes['kod_fish']};"

        # # ship,reis,kodvid,vozr1 \\ параметры tral + kodvid + параметры biopap
        elif one_value_not_none_Tral and one_value_not_none_Fishes and one_value_not_none_Biopap and not (
                one_value_not_none_Ulov):
            tral_view += """\nJOIN biopap b on b.trkl=t.trkl\nJOIN biopit bi ON bi.bikl = b.bikl"""
            tral_view += """\nWHERE 1=1 """

            tral_view = self.make_tral_str(params_tral_card, tral_view)

            # kodfish
            if params_fishes.get('kod_fish'):
                list_fishes = [*params_fishes.get('kod_fish')]
                if len(list_fishes) > 1:
                    # print(list_fishes)
                    tral_view += f" AND b.kodvid IN {(*list_fishes,)}"
                else:
                    tral_view += f" AND b.kodvid IN ({(list_fishes[0])})"
                # tral_view += f" AND b.kodvid IN {params_fishes['kod_fish']}"
            # // kodfish

            # biopap params
            tral_view = self.make_biopap_str(params_biopap_card, tral_view)
            # // biopap

            tral_view += """;"""

        #     # print(tral_view)

        # # ship,reis,u_key \\ параметры tral + параметры ulov
        elif one_value_not_none_Tral and one_value_not_none_Ulov and not (
                one_value_not_none_Biopap or one_value_not_none_Fishes):
            tral_view += """\nJOIN ulov u ON t.trkl = u.trkl"""
            tral_view += """\nWHERE 1=1 """

            # tral
            tral_view = self.make_tral_str(params_tral_card, tral_view)
            # //tral

            # ulov params
            tral_view = self.make_ulov_str(params_ulov_card, tral_view)
            # //ulov

            tral_view += """;"""

        # # ship,reis,kodvid,u_wes \\ параметры tral + kodvid + параметры ulov
        elif one_value_not_none_Tral and one_value_not_none_Fishes and one_value_not_none_Ulov and not (
                one_value_not_none_Biopap):
            tral_view += """\nJOIN ulov u ON t.trkl = u.trkl"""
            tral_view += """\nWHERE 1=1 """

            # tral
            tral_view = self.make_tral_str(params_tral_card, tral_view)
            # //tral

            # kodfish
            if params_fishes.get('kod_fish'):
                list_fishes = [*params_fishes.get('kod_fish')]
                if len(list_fishes) > 1:
                    # print(list_fishes)
                    tral_view += f" AND u.kodvid IN {(*list_fishes,)}"
                else:
                    tral_view += f" AND u.kodvid IN ({(list_fishes[0])})"
                # tral_view += f" AND u.kodvid IN {params_fishes['kod_fish']}"
            # // kodfish

            # ulov params
            tral_view = self.make_ulov_str(params_ulov_card, tral_view)
            # //ulov

            tral_view += """;"""

        # # ship,reis,kodvid,u_wes,vozr1 \\ параметры tral + kodvid + параметры ulov + параметры biopap
        elif one_value_not_none_Tral and one_value_not_none_Fishes and one_value_not_none_Ulov and one_value_not_none_Biopap:
            tral_view += """\nJOIN ulov u ON t.trkl = u.trkl\nJOIN biopap b ON u.ulkl = b.ulkl\nJOIN biopit bi ON bi.bikl = b.bikl"""
            tral_view += """\nWHERE 1=1 """

            # tral
            tral_view = self.make_tral_str(params_tral_card, tral_view)
            # //tral

            # ulov params
            tral_view = self.make_ulov_str(params_ulov_card, tral_view)
            # //ulov

            # kodfish ulov
            if params_fishes.get('kod_fish'):
                list_fishes = [*params_fishes.get('kod_fish')]
                if len(list_fishes) > 1:
                    # print(list_fishes)
                    tral_view += f" AND u.kodvid IN {(*list_fishes,)}"
                else:
                    tral_view += f" AND u.kodvid IN ({(list_fishes[0])})"
                # tral_view += f" AND u.kodvid IN {params_fishes['kod_fish']}"
            # // kodfish

            # biopap params
            tral_view = self.make_biopap_str(params_biopap_card, tral_view)
            # // biopap

            # kodfish biopap
            if params_fishes.get('kod_fish'):
                list_fishes = [*params_fishes.get('kod_fish')]
                if len(list_fishes) > 1:
                    # print(list_fishes)
                    tral_view += f" AND b.kodvid IN {(*list_fishes,)}"
                else:
                    tral_view += f" AND b.kodvid IN ({(list_fishes[0])})"
                # tral_view += f" AND b.kodvid IN {params_fishes['kod_fish']}"
            # // kodfish

            tral_view += """;"""

        # # print(params)

        # tral_view += """\nEND\n$$;"""

        print(tral_view)

        # print(tral_view)
        try:
            # self.__cur.execute(tral_view)
            # self.__db.commit()

            return tral_view
            # self.exec_view(biopap_view)
            # res = self.make_dict_request(tral_view)
            # if res:
            #     return res
        except Exception as e:
            print(f'Ошибка чтения из БД: {e}')
            return []

    # формируется представление BIOPAP_VIEW
    def generate_sample_biopap(self, params):
        # CREATE OR REPLACE TEMP VIEW
        biopap_view = f"""
        CREATE OR REPLACE TEMP VIEW biopap_view AS
        SELECT distinct b.*
        FROM biopap b
        """

        # params_tral_card = params['tral_card']
        params_tral_card = self.correct_params(params['tral_card'])

        params_ulov_card = self.correct_params(params['ulov_card'])
        params_biopap_card = self.correct_params(params['biopap_card'])
        params_fishes = self.correct_params(params['fish_list'])

        # # Проверяем, есть ли хотя бы одно непустое значение в словаре
        one_value_not_none_Tral = any(value is not None for value in params_tral_card.values())
        one_value_not_none_Ulov = any(value is not None for value in params_ulov_card.values())
        one_value_not_none_Biopap = any(value is not None for value in params_biopap_card.values())
        one_value_not_none_Fishes = any(value is not None for value in params_fishes.values())

        ship_reis_dict = self.make_ship_reis(params_tral_card['ship_reis'])


        # print(ship_reis_dict)

        # джойны.
        # собираем фулл запрос для выборки BIOPAP
        # only ship,reis \\ только параметры tral
        if one_value_not_none_Tral and not (
                one_value_not_none_Ulov or one_value_not_none_Biopap or one_value_not_none_Fishes):
            biopap_view += """\nJOIN tral t ON b.trkl = t.trkl"""
            biopap_view += """\nWHERE 1=1 """

            # biopap_view = self.make_ship_reis_str(biopap_view, ship_reis_dict, 'b')
            biopap_view = self.make_tral_str(params_tral_card, biopap_view)
            biopap_view += """;"""

        # # ship,reis,vozr1 \\ параметры tral + параметры biopap
        elif one_value_not_none_Tral and one_value_not_none_Biopap and not (
                one_value_not_none_Ulov or one_value_not_none_Fishes):
            biopap_view += """\nJOIN tral t ON b.trkl = t.trkl\nJOIN biopit bi ON bi.bikl = b.bikl"""
            biopap_view += """\nWHERE 1=1 """
            # biopap ship reis
            biopap_view = self.make_tral_str(params_tral_card, biopap_view)

            # biopap params
            biopap_view = self.make_biopap_str(params_biopap_card, biopap_view)
            # // biopap
            biopap_view += """;"""

        # # ship,reis,kodvid \\ параметры tral + kodvid
        elif one_value_not_none_Tral and one_value_not_none_Fishes and not (
                one_value_not_none_Ulov or one_value_not_none_Biopap):
            biopap_view += """\nJOIN tral t ON b.trkl = t.trkl"""
            biopap_view += """\nWHERE 1=1 """
            # tral ship reis
            biopap_view = self.make_tral_str(params_tral_card, biopap_view)
            # //tral

            if params_fishes.get('kod_fish'):
                list_fishes = [*params_fishes.get('kod_fish')]
                if len(list_fishes) > 1:
                    # print(list_fishes)
                    biopap_view += f" AND b.kodvid IN {(*list_fishes,)}"
                else:
                    biopap_view += f" AND b.kodvid IN ({(list_fishes[0])})"
                # biopap_view += f" AND b.kodvid IN {params_fishes['kod_fish']}"

            biopap_view += """;"""

        # # ship,reis,kodvid,vozr1 \\ параметры tral + kodvid + параметры biopap
        elif one_value_not_none_Tral and one_value_not_none_Fishes and one_value_not_none_Biopap and not (
                one_value_not_none_Ulov):
            biopap_view += """\nJOIN tral t ON b.trkl = t.trkl\nJOIN biopit bi ON bi.bikl = b.bikl"""
            biopap_view += """\nWHERE 1=1 """
            # tral ship reis
            biopap_view = self.make_tral_str(params_tral_card, biopap_view)
            # //tral

            # kodfish
            if params_fishes.get('kod_fish'):
                list_fishes = [*params_fishes.get('kod_fish')]
                if len(list_fishes) > 1:
                    # print(list_fishes)
                    biopap_view += f" AND b.kodvid IN {(*list_fishes,)}"
                else:
                    biopap_view += f" AND b.kodvid IN ({(list_fishes[0])})"
                # biopap_view += f" AND b.kodvid IN {params_fishes['kod_fish']}"
            # // kodfish

            # biopap params
            biopap_view = self.make_biopap_str(params_biopap_card, biopap_view)
            # // biopap

            biopap_view += """;"""

        # # ship,reis,u_key \\ параметры tral + параметры ulov
        elif one_value_not_none_Tral and one_value_not_none_Ulov and not (
                one_value_not_none_Biopap or one_value_not_none_Fishes):
            biopap_view += """\nJOIN ulov u ON b.ulkl = u.ulkl\nJOIN tral t ON u.trkl = t.trkl"""
            biopap_view += """\nWHERE 1=1 """
            # tral ship reis
            biopap_view = self.make_tral_str(params_tral_card, biopap_view)
            # //tral

            # ulov params
            biopap_view = self.make_ulov_str(params_ulov_card, biopap_view)
            # //ulov

            biopap_view += """;"""

        # # ship,reis,kodvid,u_wes \\ параметры tral + kodvid + параметры ulov
        elif one_value_not_none_Tral and one_value_not_none_Fishes and one_value_not_none_Ulov and not (
                one_value_not_none_Biopap):
            biopap_view += """\nJOIN ulov u ON b.ulkl = u.ulkl\nJOIN tral t ON u.trkl = t.trkl"""
            biopap_view += """\nWHERE 1=1 """

            # tral
            biopap_view = self.make_tral_str(params_tral_card, biopap_view)
            # //tral

            # kodfish
            if params_fishes.get('kod_fish'):
                list_fishes = [*params_fishes.get('kod_fish')]
                if len(list_fishes) > 1:
                    # print(list_fishes)
                    biopap_view += f" AND u.kodvid IN {(*list_fishes,)}"
                else:
                    biopap_view += f" AND u.kodvid IN ({(list_fishes[0])})"
                # biopap_view += f" AND u.kodvid IN {params_fishes['kod_fish']}"
            # // kodfish

            # ulov
            biopap_view = self.make_ulov_str(params_ulov_card, biopap_view)
            # //ulov

            biopap_view += """;"""

        # # ship,reis,kodvid,u_wes,vozr1 \\ параметры tral + kodvid + параметры ulov + параметры biopap
        elif one_value_not_none_Tral and one_value_not_none_Fishes and one_value_not_none_Ulov and one_value_not_none_Biopap:
            biopap_view += """
            JOIN ulov u ON b.ulkl = u.ulkl
            JOIN tral t ON u.trkl = t.trkl
            JOIN biopit bi ON bi.bikl = b.bikl
            """
            biopap_view += """\nWHERE 1=1 """

            # tral
            biopap_view = self.make_tral_str(params_tral_card, biopap_view)
            # //tral

            # kodfish for biopap
            if params_fishes.get('kod_fish'):
                list_fishes = [*params_fishes.get('kod_fish')]
                if len(list_fishes) > 1:
                    # print(list_fishes)
                    biopap_view += f" AND b.kodvid IN {(*list_fishes,)}"
                else:
                    biopap_view += f" AND b.kodvid IN ({(list_fishes[0])})"
                # biopap_view += f" AND b.kodvid IN {params_fishes['kod_fish']}"
            # // kodfish for biopap

            # biopap params
            biopap_view = self.make_biopap_str(params_biopap_card, biopap_view)
            # //biopap

            # ulov params
            biopap_view = self.make_ulov_str(params_ulov_card, biopap_view)
            # //ulov

            # kodfish for ulov
            if params_fishes.get('kod_fish'):
                biopap_view += f" AND u.kodvid IN {params_fishes['kod_fish']}"
            # // kodfish for ulov

            biopap_view += """;"""

        # print(biopap_view)

        try:
            # self.__cur.execute(biopap_view)
            # self.__db.commit()
            # self.exec_view(biopap_view)
            # res = self.make_dict_request(biopap_view)
            # if res:
            return biopap_view
        except Exception as e:
            print(f'Ошибка чтения из БД: {e}')
            return []

    # формируется представление ULOV_VIEW
    def generate_sample_ulov(self, params):
        ulov_view = """
        CREATE OR REPLACE TEMP VIEW ulov_view AS
        SELECT distinct u.*
        FROM ulov u
        """
        # params_tral_card = params['tral_card']
        params_tral_card = self.correct_params(params['tral_card'])

        params_ulov_card = self.correct_params(params['ulov_card'])
        params_biopap_card = self.correct_params(params['biopap_card'])
        params_fishes = self.correct_params(params['fish_list'])

        # # Проверяем, есть ли хотя бы одно непустое значение в словаре
        one_value_not_none_Tral = any(value is not None for value in params_tral_card.values())
        one_value_not_none_Ulov = any(value is not None for value in params_ulov_card.values())
        one_value_not_none_Biopap = any(value is not None for value in params_biopap_card.values())
        one_value_not_none_Fishes = any(value is not None for value in params_fishes.values())

        ship_reis_dict = self.make_ship_reis(params_tral_card['ship_reis'])

        # джойны.
        # собираем фулл запрос для выборки ULOV
        # only ship,reis \\ только параметры tral
        if one_value_not_none_Tral and not (
                one_value_not_none_Ulov or one_value_not_none_Biopap or one_value_not_none_Fishes):
            ulov_view += """\nJOIN tral t ON u.trkl = t.trkl"""
            ulov_view += """\nWHERE 1=1 """

            ulov_view = self.make_tral_str(params_tral_card, ulov_view)
            ulov_view += """;"""

        # # ship,reis,vozr1 \\ параметры tral + параметры biopap
        elif one_value_not_none_Tral and one_value_not_none_Biopap and not (
                one_value_not_none_Ulov or one_value_not_none_Fishes):
            ulov_view += """\nJOIN biopap b ON b.ulkl = u.ulkl\nJOIN tral t ON u.trkl = t.trkl\nJOIN biopit bi ON bi.bikl = b.bikl"""
            ulov_view += """\nWHERE 1=1 """

            # biopap ship reis
            ulov_view = self.make_tral_str(params_tral_card, ulov_view)

            # biopap params
            ulov_view = self.make_biopap_str(params_biopap_card, ulov_view)
            # // biopap

            ulov_view += """;"""

        # # ship,reis,kodvid \\ параметры tral + kodvid
        elif one_value_not_none_Tral and one_value_not_none_Fishes and not (
                one_value_not_none_Ulov or one_value_not_none_Biopap):
            ulov_view += """\nJOIN tral t ON t.trkl = u.trkl"""
            ulov_view += """\nWHERE 1=1 """

            # tral
            ulov_view = self.make_tral_str(params_tral_card, ulov_view)
            # //tral

            if params_fishes.get('kod_fish'):
                list_fishes = [*params_fishes.get('kod_fish')]
                if len(list_fishes) > 1:
                    # print(list_fishes)
                    ulov_view += f" AND u.kodvid IN {(*list_fishes,)}"
                else:
                    ulov_view += f" AND u.kodvid IN ({(list_fishes[0])})"
                # ulov_view += f" AND u.kodvid IN {params_fishes['kod_fish']};"
            ulov_view += """;"""

        # # ship,reis,kodvid,vozr1 \\ параметры tral + kodvid + параметры biopap
        elif one_value_not_none_Tral and one_value_not_none_Fishes and one_value_not_none_Biopap and not (
                one_value_not_none_Ulov):
            ulov_view += """\nJOIN biopap b on b.trkl=t.trkl\nJOIN tral t ON t.trkl = u.trkl\nJOIN biopit bi ON bi.bikl = b.bikl"""
            ulov_view += """\nWHERE 1=1 """

            # tral
            ulov_view = self.make_tral_str(params_tral_card, ulov_view)
            # //tral

            # kodfish
            if params_fishes.get('kod_fish'):
                list_fishes = [*params_fishes.get('kod_fish')]
                if len(list_fishes) > 1:
                    # print(list_fishes)
                    ulov_view += f" AND b.kodvid IN {(*list_fishes,)}"
                else:
                    ulov_view += f" AND b.kodvid IN ({(list_fishes[0])})"
                # ulov_view += f" AND b.kodvid IN {params_fishes['kod_fish']}"
            # // kodfish

            # biopap params
            ulov_view = self.make_biopap_str(params_biopap_card, ulov_view)
            # // biopap

            ulov_view += """;"""

        # # ship,reis,u_key \\ параметры tral + параметры ulov
        elif one_value_not_none_Tral and one_value_not_none_Ulov and not (
                one_value_not_none_Biopap or one_value_not_none_Fishes):
            ulov_view += """\nJOIN tral t ON t.trkl = u.trkl"""
            ulov_view += """\nWHERE 1=1"""

            # tral
            ulov_view = self.make_tral_str(params_tral_card, ulov_view)
            # //tral

            # ulov params
            ulov_view = self.make_ulov_str(params_ulov_card, ulov_view)
            # //ulov

            ulov_view += """;"""

        # # ship,reis,kodvid,u_wes \\ параметры tral + kodvid + параметры ulov
        elif one_value_not_none_Tral and one_value_not_none_Fishes and one_value_not_none_Ulov and not (
                one_value_not_none_Biopap):
            ulov_view += """\nJOIN tral t ON t.trkl = u.trkl"""
            ulov_view += """\nWHERE 1=1 """

            # tral
            ulov_view = self.make_tral_str(params_tral_card, ulov_view)
            # //tral

            # kodfish
            if params_fishes.get('kod_fish'):
                list_fishes = [*params_fishes.get('kod_fish')]
                if len(list_fishes) > 1:
                    # print(list_fishes)
                    ulov_view += f" AND u.kodvid IN {(*list_fishes,)}"
                else:
                    ulov_view += f" AND u.kodvid IN ({(list_fishes[0])})"
                # ulov_view += f" AND u.kodvid IN {params_fishes['kod_fish']}"
            # // kodfish

            # ulov params
            ulov_view = self.make_ulov_str(params_ulov_card, ulov_view)
            # //ulov

            ulov_view += """;"""

        # # ship,reis,kodvid,u_wes,vozr1 \\ параметры tral + kodvid + параметры ulov + параметры biopap
        elif one_value_not_none_Tral and one_value_not_none_Fishes and one_value_not_none_Ulov and one_value_not_none_Biopap:
            ulov_view += """\nJOIN tral t ON t.trkl = u.trkl\nJOIN biopap b ON u.ulkl = b.ulkl\nJOIN biopit bi ON bi.bikl = b.bikl"""
            ulov_view += """\nWHERE 1=1 """

            # tral
            ulov_view = self.make_tral_str(params_tral_card, ulov_view)
            # //tral

            # ulov params
            ulov_view = self.make_ulov_str(params_ulov_card, ulov_view)
            # //ulov

            # kodfish ulov
            if params_fishes.get('kod_fish'):
                list_fishes = [*params_fishes.get('kod_fish')]
                if len(list_fishes) > 1:
                    # print(list_fishes)
                    ulov_view += f" AND u.kodvid IN {(*list_fishes,)}"
                else:
                    ulov_view += f" AND u.kodvid IN ({(list_fishes[0])})"
                # ulov_view += f" AND u.kodvid IN {params_fishes['kod_fish']}"
            # // kodfish

            # biopap params
            ulov_view = self.make_biopap_str(params_biopap_card, ulov_view)
            # // biopap

            # kodfish biopap
            if params_fishes.get('kod_fish'):
                list_fishes = [*params_fishes.get('kod_fish')]
                if len(list_fishes) > 1:
                    # print(list_fishes)
                    ulov_view += f" AND b.kodvid IN {(*list_fishes,)}"
                else:
                    ulov_view += f" AND b.kodvid IN ({(list_fishes[0])})"
                # ulov_view += f" AND b.kodvid IN {params_fishes['kod_fish']}"
            # // kodfish

            ulov_view += """;"""

        # print(ulov_view)

        try:

            return ulov_view

        except Exception as e:
            print(f'Ошибка чтения из БД: {e}')
            return []
        
    # формируется представление RAZM_VIEW
    def generate_sample_razm(self, params):
        razm_view = """
        CREATE OR REPLACE TEMP VIEW razm_view AS
        SELECT distinct r.*
        FROM razm r
        """

        params_tral_card = self.correct_params(params['tral_card'])
        params_ulov_card = self.correct_params(params['ulov_card'])
        params_biopap_card = self.correct_params(params['biopap_card'])
        params_fishes = self.correct_params(params['fish_list'])

        # # Проверяем, есть ли хотя бы одно непустое значение в словаре
        one_value_not_none_Tral = any(value is not None for value in params_tral_card.values())
        one_value_not_none_Ulov = any(value is not None for value in params_ulov_card.values())
        one_value_not_none_Biopap = any(value is not None for value in params_biopap_card.values())
        one_value_not_none_Fishes = any(value is not None for value in params_fishes.values())

        # ship_reis_dict = self.make_ship_reis(params_tral_card['ship_reis'])

        # джойны.
        # собираем фулл запрос для выборки ULOV
        # only ship,reis \\ только параметры tral
        if one_value_not_none_Tral and not (
                one_value_not_none_Ulov or one_value_not_none_Biopap or one_value_not_none_Fishes):
            razm_view += """\nJOIN tral t ON u.trkl = t.trkl\nJOIN ulov u ON u.ulkl = r.ulkl"""
            razm_view += """\nWHERE 1=1 """

            # tral
            razm_view = self.make_tral_str(params_tral_card, razm_view)
            # // tral

            # ulov params
            # razm_view = self.make_ulov_str(params_ulov_card, razm_view)
            # //ulov
           
            razm_view += """;"""

        # # ship,reis,vozr1 \\ параметры из tral + прочие параметры из biopap
        elif one_value_not_none_Tral and one_value_not_none_Biopap and not (
                one_value_not_none_Ulov or one_value_not_none_Fishes):
            razm_view += """\nJOIN tral t ON u.trkl = t.trkl\nJOIN ulov u ON u.ulkl = r.ulkl\nJOIN biopap b ON u.ulkl = b.ulkl"""
            razm_view += """\nWHERE 1=1 """

            # tral
            razm_view = self.make_tral_str(params_tral_card, razm_view)
            # // tral

            # biopap params
            razm_view = self.make_biopap_str(params_biopap_card, razm_view)
            # // biopap

            # ulov params
            # razm_view = self.make_ulov_str(params_ulov_card, razm_view)
            # //ulov

            razm_view += """;"""

        # # ship,reis,kodvid \\ параметры tral + kodvid
        elif one_value_not_none_Tral and one_value_not_none_Fishes and not (
                one_value_not_none_Ulov or one_value_not_none_Biopap):
            razm_view += """\nJOIN ulov u ON r.ulkl = u.ulkl\nJOIN tral t ON u.trkl = t.trkl"""
            razm_view += """\nWHERE 1=1 """
            
            # tral
            razm_view = self.make_tral_str(params_tral_card, razm_view)
            # // tral

            if params_fishes.get('kod_fish'):
                list_fishes = [*params_fishes.get('kod_fish')]
                if len(list_fishes) > 1:
                    # print(list_fishes)
                    razm_view += f" AND u.kodvid IN {(*list_fishes,)}"
                else:
                    razm_view += f" AND u.kodvid IN ({(list_fishes[0])})"
                    
            # ulov params
            # razm_view = self.make_ulov_str(params_ulov_card, razm_view)
            # //ulov

            razm_view += """;"""

        # # ship,reis,kodvid,vozr1 \\ параметры tral + kodvid + параметры biopap
        elif one_value_not_none_Tral and one_value_not_none_Fishes and one_value_not_none_Biopap and not (
                one_value_not_none_Ulov):
            razm_view += """\nJOIN tral t ON u.trkl = t.trkl\nJOIN biopap b on b.trkl=t.trkl\nJOIN biopit bi ON bi.bikl = b.bikl\nJOIN ulov u ON t.trkl = u.trkl"""
            razm_view += """\nWHERE 1=1 """
            
            # tral
            razm_view = self.make_tral_str(params_tral_card, razm_view)
            # // tral

            # biopap params
            razm_view = self.make_biopap_str(params_biopap_card, razm_view)
            # // biopap            

            razm_view += """;"""

        # # ship,reis,u_key \\ параметры tral + параметры ulov
        elif one_value_not_none_Tral and one_value_not_none_Ulov and not (
                one_value_not_none_Biopap or one_value_not_none_Fishes):
            razm_view += """\nJOIN ulov u ON r.ulkl = u.ulkl\nJOIN tral t ON u.trkl = t.trkl"""
            razm_view += """\nWHERE 1=1 """

            # tral
            razm_view = self.make_tral_str(params_tral_card, razm_view)
            # //tral

            # ulov params
            razm_view = self.make_ulov_str(params_ulov_card, razm_view)
            # //ulov

            razm_view += """;"""

        # # ship,reis,kodvid,u_wes \\ параметры tral + kodvid + параметры ulov
        elif one_value_not_none_Tral and one_value_not_none_Fishes and one_value_not_none_Ulov and not (
                one_value_not_none_Biopap):
            razm_view += """\nJOIN ulov u ON r.ulkl = u.ulkl\nJOIN tral t ON u.trkl = t.trkl"""
            razm_view += """\nWHERE 1=1 """

            # tral
            razm_view = self.make_tral_str(params_tral_card, razm_view)
            # //tral

            # kodfish
            if params_fishes.get('kod_fish'):
                list_fishes = [*params_fishes.get('kod_fish')]
                if len(list_fishes) > 1:
                    # print(list_fishes)
                    razm_view += f" AND u.kodvid IN {(*list_fishes,)}"
                else:
                    razm_view += f" AND u.kodvid IN ({(list_fishes[0])})"

            # ulov params
            razm_view = self.make_ulov_str(params_ulov_card, razm_view)
            # //ulov

            razm_view += """;"""

        # # ship,reis,kodvid,u_wes,vozr1 \\ параметры tral + kodvid + параметры ulov + параметры biopap
        elif one_value_not_none_Tral and one_value_not_none_Fishes and one_value_not_none_Ulov and one_value_not_none_Biopap:
            razm_view += """\nJOIN ulov u ON r.ulkl = u.ulkl\nJOIN tral t ON u.trkl = t.trkl\nJOIN biopap b ON u.ulkl = b.ulkl\nJOIN biopit bi ON bi.bikl = b.bikl"""
            razm_view += """\nWHERE 1=1 """

            # tral
            razm_view = self.make_tral_str(params_tral_card, razm_view)
            # //tral

            # ulov params
            razm_view = self.make_ulov_str(params_ulov_card, razm_view)
            # //ulov

            # kodfish
            if params_fishes.get('kod_fish'):
                list_fishes = [*params_fishes.get('kod_fish')]
                if len(list_fishes) > 1:
                    # print(list_fishes)
                    razm_view += f" AND u.kodvid IN {(*list_fishes,)}"
                else:
                    razm_view += f" AND u.kodvid IN ({(list_fishes[0])})"

            # biopap params
            razm_view = self.make_biopap_str(params_biopap_card, razm_view)
            # // biopap  

            razm_view += """;"""

        print(razm_view)

        try:
            # self.__cur.execute(tral_view)
            # self.__db.commit()

            return razm_view
            # self.exec_view(biopap_view)
            # res = self.make_dict_request(tral_view)
            # if res:
            #     return res
        except Exception as e:
            print(f'Ошибка чтения из БД: {e}')
            return []
        
    # формируется представление BIOPIT_VIEW
    def generate_sample_biopit(self, params):
        # CREATE OR REPLACE TEMP VIEW
        biopap_view = f"""
        CREATE OR REPLACE TEMP VIEW biopit_view AS
        SELECT distinct bi.*
        FROM biopit bi
        """

        # params_tral_card = params['tral_card']
        params_tral_card = self.correct_params(params['tral_card'])

        params_ulov_card = self.correct_params(params['ulov_card'])
        params_biopap_card = self.correct_params(params['biopap_card'])
        params_fishes = self.correct_params(params['fish_list'])

        # # Проверяем, есть ли хотя бы одно непустое значение в словаре
        one_value_not_none_Tral = any(value is not None for value in params_tral_card.values())
        one_value_not_none_Ulov = any(value is not None for value in params_ulov_card.values())
        one_value_not_none_Biopap = any(value is not None for value in params_biopap_card.values())
        one_value_not_none_Fishes = any(value is not None for value in params_fishes.values())

        ship_reis_dict = self.make_ship_reis(params_tral_card['ship_reis'])

        # джойны.
        # собираем фулл запрос для выборки BIOPIT
        # only ship,reis \\ только параметры tral
        if one_value_not_none_Tral and not (
                one_value_not_none_Ulov or one_value_not_none_Biopap or one_value_not_none_Fishes):
            biopap_view += """\nJOIN tral t on bi.ship=t.ship and bi.reis=t.reis and bi.trl=t.trl"""
            biopap_view += """\nWHERE 1=1 """

            # biopap_view = self.make_ship_reis_str(biopap_view, ship_reis_dict, 'b')
            biopap_view = self.make_tral_str(params_tral_card, biopap_view)
            biopap_view += """;"""

        # # ship,reis,vozr1 \\ параметры tral + параметры biopap
        elif one_value_not_none_Tral and one_value_not_none_Biopap and not (
                one_value_not_none_Ulov or one_value_not_none_Fishes):
            biopap_view += """\nJOIN biopap b ON bi.bikl = b.bikl\nJOIN tral t on bi.ship=t.ship and bi.reis=t.reis and bi.trl=t.trl"""
            biopap_view += """\nWHERE 1=1 """
            # biopap ship reis
            biopap_view = self.make_tral_str(params_tral_card, biopap_view)

            # biopap params
            biopap_view = self.make_biopap_str(params_biopap_card, biopap_view)
            # // biopap
            biopap_view += """;"""

        # # ship,reis,kodvid \\ параметры tral + kodvid
        elif one_value_not_none_Tral and one_value_not_none_Fishes and not (
                one_value_not_none_Ulov or one_value_not_none_Biopap):
            biopap_view += """\nJOIN tral t on bi.ship=t.ship and bi.reis=t.reis and bi.trl=t.trl"""
            biopap_view += """\nWHERE 1=1 """
            # tral ship reis
            biopap_view = self.make_tral_str(params_tral_card, biopap_view)
            # //tral

            if params_fishes.get('kod_fish'):
                list_fishes = [*params_fishes.get('kod_fish')]
                if len(list_fishes) > 1:
                    # print(list_fishes)
                    biopap_view += f" AND bi.kodvid IN {(*list_fishes,)}"
                else:
                    biopap_view += f" AND bi.kodvid IN ({(list_fishes[0])})"
                # biopap_view += f" AND bi.kodvid IN {params_fishes['kod_fish']}"

            biopap_view += """;"""

        # # ship,reis,kodvid,vozr1 \\ параметры tral + kodvid + параметры biopap
        elif one_value_not_none_Tral and one_value_not_none_Fishes and one_value_not_none_Biopap and not (
                one_value_not_none_Ulov):
            biopap_view += """\nJOIN biopap b ON bi.bikl = b.bikl\nJOIN tral t ON b.trkl = t.trkl"""
            biopap_view += """\nWHERE 1=1 """
            # tral ship reis
            biopap_view = self.make_tral_str(params_tral_card, biopap_view)
            # //tral

            # kodfish
            if params_fishes.get('kod_fish'):
                list_fishes = [*params_fishes.get('kod_fish')]
                if len(list_fishes) > 1:
                    # print(list_fishes)
                    biopap_view += f" AND bi.kodvid IN {(*list_fishes,)}"
                else:
                    biopap_view += f" AND bi.kodvid IN ({(list_fishes[0])})"
                # biopap_view += f" AND bi.kodvid IN {params_fishes['kod_fish']}"
            # // kodfish

            # biopap params
            biopap_view = self.make_biopap_str(params_biopap_card, biopap_view)
            # // biopap

            biopap_view += """;"""

        # # ship,reis,u_key \\ параметры tral + параметры ulov
        elif one_value_not_none_Tral and one_value_not_none_Ulov and not (
                one_value_not_none_Biopap or one_value_not_none_Fishes):
            biopap_view += """\nJOIN tral t on bi.ship=t.ship and bi.reis=t.reis and bi.trl=t.trl\nJOIN ulov on bi.ship=u.ship and bi.reis=u.reis and bi.trl=u.trl"""
            biopap_view += """\nWHERE 1=1 """
            # tral ship reis
            biopap_view = self.make_tral_str(params_tral_card, biopap_view)
            # //tral

            # ulov params
            biopap_view = self.make_ulov_str(params_ulov_card, biopap_view)
            # //ulov

            biopap_view += """;"""

        # # ship,reis,kodvid,u_wes \\ параметры tral + kodvid + параметры ulov
        elif one_value_not_none_Tral and one_value_not_none_Fishes and one_value_not_none_Ulov and not (
                one_value_not_none_Biopap):
            biopap_view += """\nJOIN tral t on bi.ship=t.ship and bi.reis=t.reis and bi.trl=t.trl\nJOIN ulov on bi.ship=u.ship and bi.reis=u.reis and bi.trl=u.trl"""
            biopap_view += """\nWHERE 1=1 """

            # tral
            biopap_view = self.make_tral_str(params_tral_card, biopap_view)
            # //tral

            # kodfish
            if params_fishes.get('kod_fish'):
                list_fishes = [*params_fishes.get('kod_fish')]
                if len(list_fishes) > 1:
                    # print(list_fishes)
                    biopap_view += f" AND u.kodvid IN {(*list_fishes,)}"
                else:
                    biopap_view += f" AND u.kodvid IN ({(list_fishes[0])})"
                # biopap_view += f" AND u.kodvid IN {params_fishes['kod_fish']}"
            # // kodfish

            # ulov
            biopap_view = self.make_ulov_str(params_ulov_card, biopap_view)
            # //ulov

            biopap_view += """;"""

        # # ship,reis,kodvid,u_wes,vozr1 \\ параметры tral + kodvid + параметры ulov + параметры biopap
        elif one_value_not_none_Tral and one_value_not_none_Fishes and one_value_not_none_Ulov and one_value_not_none_Biopap:
            biopap_view += """
            JOIN biopap bi ON bi.bikl = b.bikl
            JOIN ulov u ON b.bikl = u.bikl
            JOIN tral t ON u.trkl = t.trkl
            """
            biopap_view += """\nWHERE 1=1 """

            # tral
            biopap_view = self.make_tral_str(params_tral_card, biopap_view)
            # //tral

            # kodfish for biopap
            if params_fishes.get('kod_fish'):
                list_fishes = [*params_fishes.get('kod_fish')]
                if len(list_fishes) > 1:
                    # print(list_fishes)
                    biopap_view += f" AND bi.kodvid IN {(*list_fishes,)}"
                else:
                    biopap_view += f" AND bi.kodvid IN ({(list_fishes[0])})"
                # biopap_view += f" AND bi.kodvid IN {params_fishes['kod_fish']}"
            # // kodfish for biopap

            # biopap params
            biopap_view = self.make_biopap_str(params_biopap_card, biopap_view)
            # //biopap

            # ulov params
            biopap_view = self.make_ulov_str(params_ulov_card, biopap_view)
            # //ulov

            # kodfish for ulov
            if params_fishes.get('kod_fish'):
                biopap_view += f" AND u.kodvid IN {params_fishes['kod_fish']}"
            # // kodfish for ulov

            biopap_view += """;"""

        # print(biopap_view)

        try:
            # self.__cur.execute(biopap_view)
            # self.__db.commit()
            # self.exec_view(biopap_view)
            # res = self.make_dict_request(biopap_view)
            # if res:
            return biopap_view
        except Exception as e:
            print(f'Ошибка чтения из БД: {e}')
            return []

    # ========== Формируем представления
    def general_view(self, params, form, views_dict):
        body_qur = {
            'aged-form1':
                f"""select a.ship,a.reis,a.trl,a.data,a.rloc,a.rzon,a.rikes,a.shir,a.dolg,b.proba, count(*) as kolr
                from tral_view a, biopap_view b
                where a.ship = b.ship and a.reis=b.reis and a.trl=b.trl and b.vozr1 is not null 
                and b.wes_ob is not null and b.wes_ob > 0
                """,

            'aged-form2':
                """select a.ship,a.reis,a.trl,a.data,a.rloc,a.rzon,a.rikes,a.shir,a.dolg,b.proba, count(*) as kolr
                from tral_view a, biopap_view b
                where a.ship = b.ship and a.reis=b.reis and a.trl=b.trl and b.vozr1 is not null
                """,

            'aged-form3':
                """select a.ship,a.reis,a.trl,a.data,a.rloc,a.rzon,a.rikes,a.shir,a.dolg,b.proba, count(*) as kolr
                from tral_view a, biopap_view b
                where a.ship = b.ship and a.reis=b.reis and a.trl=b.trl
                and (b.wes_ob != 0 and b.w_pech != 0 and b.vozr1 is not null)
                """,

            'aged-form4':
                """select a.ship,a.reis,a.trl,a.data,a.rloc,a.rzon,a.rikes,a.shir,a.dolg,b.proba, count(*) as kolr
                from tral_view a, biopap_view b
                where a.ship = b.ship and a.reis=b.reis and a.trl=b.trl
                and (b.vozr1 is not null and b.wes_ob > 0 and b.wes_ob is not null)
                """,

            'aged-form11':
                """select a.ship,a.reis,a.trl,a.data,a.rloc,a.rzon,a.rikes,a.shir,a.dolg,b.proba, count(*) as kolr
                from tral_view a, biopap_view b
                where a.ship = b.ship and a.reis=b.reis and a.trl=b.trl
                and (b.vozr1 is not null)
                """,

            'aged-form12':
                """select a.ship,a.reis,a.trl,a.data,a.rloc,a.rzon,a.rikes,a.shir,a.dolg,b.proba, count(*) as kolr
                from tral_view a, biopap_view b
                where a.ship = b.ship and a.reis=b.reis and a.trl=b.trl
                and (b.vozr1 is not null)
                """,

            'aged-form13':
                """select a.ship,a.reis,a.trl,a.data,a.rloc,a.rzon,a.rikes,a.shir,a.dolg,b.proba, count(*) as kolr
                from tral_view a, biopap_view b
                where a.ship = b.ship and a.reis=b.reis and a.trl=b.trl
                and b.puzo is not null
                """,

            'aged-form15':
                """select a.ship,a.reis,a.trl,a.data,a.rloc,a.rzon,a.rikes,a.shir,a.dolg,b.proba, count(*) as kolr
                from tral_view a, biopap_view b
                where a.ship = b.ship and a.reis=b.reis and a.trl=b.trl
                """,

            'another-form9':
                """select a.ship,a.reis,a.trl,a.data,a.rloc,a.rzon,a.rikes,a.shir,a.dolg,b.proba, count(*) as kolr
                from tral_view a, biopap_view b
                where a.ship = b.ship and a.reis=b.reis and a.trl=b.trl
                """,
            # это костыль
            'bio-tral':
                """select * from tral limit 1;
                """,
            'bio-tral-extension':
                """select * from tral limit 1;
                """,
            'svod-bi':
                """select * from tral limit 1;
                """,
            'bio-pap':
                """select * from tral limit 1;
                """,
            'bio-razm':
                """select * from tral limit 1;
                """,
            
        }
        # print(views_dict)

        sql = body_qur[form]

        # print(views_dict)
        tral_view = views_dict['tral_view']
        biopap_view = views_dict['biopap_view']
        ulov_view = views_dict['ulov_view']
        biopit_view = views_dict['biopit_view']
        razm_view = views_dict['razm_view']

        params = self.correct_params(params)

        if form == 'aged-form15':
            if params.get('start_row'):
                sql += f""" AND (b.l_big>={params['start_row']} AND b.l_big is not null)"""
        else:
            if params.get('len_count') and params.get('start_row'):
                sql += f""" AND b.{params['len_count']}>={params['start_row']} AND b.{params['len_count']} is not null"""

        if (params.get('start_period1') and params.get('end_period1')) or (
                params.get('start_period2') and params.get('end_period2')):
            sql += f""" AND ((a.data BETWEEN TO_DATE('{params['start_period1']}', 'YYYY-MM-DD') AND TO_DATE('{params['end_period1']}', 'YYYY-MM-DD')) 
            OR (a.data BETWEEN TO_DATE('{params['start_period2']}', 'YYYY-MM-DD') AND TO_DATE('{params['end_period2']}', 'YYYY-MM-DD')))"""

        if form in ['aged-form1', 'aged-form2', 'aged-form3', 'aged-form4', 'aged-form11', 'aged-form12', 'aged-form13', 'aged-form15', 'another-form9']:
            sql += """ 
            group by a.ship,a.reis,a.trl,a.data,a.rloc,a.rzon,a.rikes,a.shir,a.dolg,b.proba
            order by a.ship,a.reis,a.trl,a.data,a.rloc,a.rzon,a.rikes,a.shir,a.dolg;
            """
        else:
            sql += ''

        # print(sql)

        new_sql = f"""
        {tral_view}
        {biopap_view}
        {ulov_view}
        {biopit_view}
        {razm_view}
        
        {sql}
        """

        print(new_sql)

        try:
            res = self.make_dict_request(new_sql)
            if res:
                return res
        except Exception as e:
            print(f'Ошибка чтения из БД: {e}')
            return []

    # ============= // Формируем представления
    # ============= ФУНКЦИИ ФОРМ =============

    # РАСЧЕТНЫЕ ЗАДАЧИ BIOFOX
    # SvodBI
    # ============= Формируем все таблички (типа выборки) для этой формы
    def datas_for_svodbi(self, params):
        sql_t0 = f"""
        select ship, reis, trl, data from tral_view;
        """
        len_param = params[0]
        if len_param == 'l_big':
            type_dl = 'type_lbig'
        else:
            type_dl = 'type_llit'
        # print(len_param)

        sql_t1 = f"""
        with dtablr as (
        select distinct b.ulkl, b.kodvid, b.tip_dl, b.u_key, d.trkl
        FROM biopap_view a
        join razm_view b on b.ulkl=a.ulkl and b.kodvid=a.kodvid and a.{type_dl}=b.tip_dl and b.u_key=a.u_key
        join tral_view d on d.trkl=a.trkl
        )
        select distinct a.ulkl, b.trkl, a.kodvid, sk.kod_fish, sk.nam_fish, a.lnf, a.pol, a.kolf, a.tip_dl, a.u_key, d.data, d.glub
        FROM razm_view a
        join dtablr b on a.ulkl=b.ulkl and a.kodvid=b.kodvid and a.tip_dl=b.tip_dl
        and a.u_key=b.u_key
        join tral_view d on d.trkl=b.trkl
        join spr_kodfish sk on sk.kod_fish::int = a.kodvid;
        """

        sql_t2 = f"""
        with dtablr as (
        select distinct b.ulkl, b.kodvid, b.{type_dl}, b.u_key, b.trkl
        FROM biopap_view b
        join razm_view a on b.ulkl=a.ulkl and b.kodvid=a.kodvid and b.{type_dl}=a.tip_dl and b.u_key=a.u_key
        ),
        dtablr2 as (SELECT
            b.ulkl, b.bikl, c.trkl, b.kodvid, sk.kod_fish, sk.nam_fish, b.pol, b.wes_ob, b.{len_param}, b.w_pech, b.zrel,
            b.puzo, b.bio_info, b.skap_info, b.{type_dl}, b.u_key, b.salo, b.w_gonad,
            d.data, d.glub
        FROM biopap_view b
        join dtablr c on b.ulkl=c.ulkl and b.kodvid=c.kodvid and b.{type_dl}=c.{type_dl}
        and b.u_key=c.u_key
        join tral_view d on d.trkl=b.trkl
        join spr_kodfish sk on sk.kod_fish::int = b.kodvid
        where b.{len_param}>0 and b.{len_param} is not null)
        select * from dtablr2;
        """

        sql_t3 = f"""
        WITH dtablr AS (
            SELECT DISTINCT b.ulkl, b.bikl, b.kodvid, b.{type_dl}, b.u_key, b.trkl
            FROM biopap_view b
            JOIN razm_view a ON b.ulkl = a.ulkl AND b.kodvid = a.kodvid AND b.{type_dl} = a.tip_dl AND b.u_key = a.u_key
        ),
        fishes AS (
            SELECT DISTINCT bi.bikl, bi.kodvid, sk.kod_fish, sk.nam_fish, kp.nam_pit, bi.kodpit, b.*
            FROM biopit bi
            JOIN dtablr b ON b.bikl = bi.bikl AND b.kodvid = bi.kodvid AND b.u_key = bi.u_key
            JOIN kod_pit kp ON bi.kodpit = cast(kp.kod_pit AS int)
            join spr_kodfish sk on sk.kod_fish::int = bi.kodvid
        )
        select * from fishes;
        """

        sql_t4 = f"""
        with dtablr as (
            select distinct b.ulkl, b.kodvid, b.{type_dl}, b.u_key, b.trkl
            FROM biopap_view b
            join razm_view a on b.ulkl=a.ulkl and b.kodvid=a.kodvid and b.{type_dl}=a.tip_dl and b.u_key=a.u_key
        ),
        dtablr2 as (SELECT
            b.ulkl, b.bikl, c.trkl, b.kodvid, b.pol, b.wes_ob, b.{len_param}, b.w_pech, b.zrel,
            b.puzo, b.bio_info, b.skap_info, b.{type_dl}, b.u_key, b.salo, b.w_gonad,
            d.data, d.glub
        FROM biopap_view b
        join dtablr c on b.ulkl=c.ulkl and b.kodvid=c.kodvid and b.{type_dl}=c.{type_dl}
        and b.u_key=c.u_key
        join tral_view d on d.trkl=b.trkl
        where b.{len_param}>0 and b.{len_param} is not null)
        select DISTINCT b.bikl, b.kodvid, b.bio_info, b.u_key
        from dtablr2 b
        join biopit_view c on c.bikl=b.bikl and c.kodvid = b.kodvid and b.bio_info=c.bio_info and b.u_key=c.u_key
        where b.puzo is not null and c.kodpit is not null and c.kodpit > 0;
        """


        try:
            res_0 = self.make_dict_request(sql_t0)
            res_1 = self.make_dict_request(sql_t1)
            res_2 = self.make_dict_request(sql_t2)
            res_3 = self.make_dict_request(sql_t3)
            res_4 = self.make_dict_request(sql_t4)

            t0 = pd.DataFrame(res_0)
            t1 = pd.DataFrame(res_1)
            t2 = pd.DataFrame(res_2)
            t3 = pd.DataFrame(res_3)
            t4 = pd.DataFrame(res_4)
            # print(t0)
            # print(t1)
            

            if res_0 or res_1 or res_2 or res_3 or res_4:
                try:
                    result = self.counting_svodbi([t0, t1, t2, t3, t4], params)
                    return result
                except Exception as e:
                    print(f'Ошибка: {e}')
                    return []

        except Exception as e:
            print(f'Ошибка чтения из БД: {e}')
            return []
        
    # расчет таблиц SvodBi
    def counting_svodbi(self, data, params):
        # Шапка таблицы
        # print(params)
        df0 = data[0]
        len_param, shift, fat_count, step, *cnt_val = params
        is_all_none = any(value is not None for value in cnt_val)

        if not is_all_none:
            cnt_val = []
        else:
            cnt_val = [i for i in cnt_val if i is not None]


        self.rename_df_columns(df0, {'ship':'Позывной', 'reis':'Рейс','trl':'Трал','data':'Дата'})

        def make_table_header(df):
            df0_header = df.pivot_table(index=['Позывной', 'Рейс'], values=['Дата'], aggfunc=['min', 'max'])
            self.rename_df_columns(df0_header, {'min':'Начало рейса', 'max':'Конец рейса'})

            return df0_header
        
        
        # Данные для 1 таблицы
        df1 = data[1]

        # Данные для 2 таблицы и большинства других
        df2 = data[2]

        # Данные для 6 таблицы и большинства других
        df6 = data[3]

        # количество вскрытых желудков
        df_k_vsk0 = data[4]


        def default_params(param):
            if param is None:
                param = 10
            else:
                param = int(param)
            return param


        shift = default_params(shift)
        step = default_params(step)

        # Кусок 1\\ Кол-во рыб и %

        # Делим на бины
        # 1
        if df1.empty and df2.empty:
            df1_bin = pd.DataFrame()
            df2_bin = pd.DataFrame()


        else:
            df1_bin = self.make_bins2(df1, 'lnf', shift, step) 
            df2_bin = self.make_bins2(df2, len_param, shift, step)

            # 1
            self.rename_df_columns(df1_bin, {'kodvid':'Код рыбки','lnf':'Длина','pol':'Пол',
                                         'kolf':'Кол-во','tip_dl':'Тип длины', 'data':'Дата',
                                         'glub':'Глубина','bucket_range':'Диапазон', 'nam_fish':'Рыбка'})
            df1_bin['Дата'] = pd.to_datetime(df1_bin['Дата'])
            # 2
            self.rename_df_columns(df2_bin, {'kodvid':'Код рыбки', 'pol':'Пол', 'wes_ob':'Вес',
                                            len_param:'Длина', 'w_pech':'Вес печени', 'zrel':'Зрелость',
                                            'glub':'Глубина', 'bucket_range':'Диапазон', 'data': 'Дата',
                                            'nam_fish':'Рыбка'})
            
            df2_bin['Дата'] = pd.to_datetime(df2_bin['Дата'])
            df2_bin['Вес'] = df2_bin['Вес'].astype(float)

            # 6
            self.rename_df_columns(df6, {'kodpit':'Код', 'nam_pit':'Наименование', 'nam_fish':'Рыбка'})

            
            df_kvsk = df2_bin.copy()
            df_puzo0 = df_kvsk[(df_kvsk['puzo'] >= 0) & (df_kvsk['puzo'] <= 4)]
            df_k_vsk1 = df2_bin.copy()


        # Вычленяем месяца и года
        def make_formatted_date(df):
            """Вычленяет из даты месяц и год"""
            df['Месяц_Год'] = df['Дата'].dt.strftime('%B %Y')

            return df

        def make_formatted_month(df):
            """Вычленяет из даты только месяц"""
            df['Месяц'] = df['Дата'].dt.strftime('%B')

            return df

        def make_formatted_year(df):
            """Вычленяет из даты только год"""
            df['Год'] = df['Дата'].dt.strftime('%Y')

            return df


        # # 1
        df1_bin = make_formatted_date(df1_bin)
        df1_bin = make_formatted_month(df1_bin)
        df1_bin = make_formatted_year(df1_bin)

        # # 2
        df2_bin = make_formatted_date(df2_bin)
        df2_bin = make_formatted_month(df2_bin)
        df2_bin = make_formatted_year(df2_bin)

        df2_bin['Кол-во'] = 1


# ======== ФУНКЦИИ ФОРМИРОВАНИЯ ТАБЛИЦ ========
# ======== 1 ========
        # Делаем эту таблицу с количеством и процентом
        def make_part_cnt_perc_modified(df, indx, val, param):
            df_pivot = df.pivot_table(index=[indx], values=val, aggfunc='sum', margins=True)
            df_pivot = df_pivot[df_pivot[val] != 0]
            df_pivot['%'] = (df_pivot[val] / (max(df_pivot[val]))) * 100
            df_pivot['%'] = df_pivot['%'].apply(lambda x: round(x, 3))
            self.rename_df_columns(df_pivot, {'cnt':'Кол-во'})

            return self.add_new_level(df_pivot, [f'Число рыб из RAZM и Процент ({param})'])

        
        # # Кусок 2 
        def make_pivot_df2_modified(df2, func):
            # df2_piv = df2
            df2_piv = df2.pivot_table(index=['Диапазон'], columns=['Пол'], values='Вес', aggfunc=[func], margins=True)
            df2_piv = df2_piv[(df2_piv[func][:] > 0)]
            df2_piv.replace(0, np.nan, inplace=True)
            df2_piv.dropna(how='all', inplace=True)

            return df2_piv


        def make_mean_wes_modified(df, param):
            df_sum = make_pivot_df2_modified(df, 'sum')
            df_cnt = make_pivot_df2_modified(df, 'count')
            df2_mean_wes = round(df_sum['sum'] / df_cnt['count'], 2)

            return self.add_new_level(df2_mean_wes, [f'Средний вес(г) ({param})'])
        
        # Кусок 3
        df3_bio = df2_bin[df2_bin['bio_info'] == 0]
        df3_bio['Кол-во'] = 1

        def make_cnt_by_pol_modified(df, indx, val, param):
            """Вычисляет численность по полу"""
            pol_lst = df['Пол'].unique()
            df_cnt_pol = df.pivot_table(index=[indx], columns=['Пол'], values=val, aggfunc=['count'], margins=True)
            if df_cnt_pol.empty:
                    return df_cnt_pol
            else:
                df_cnt_pol = self.drop_multiindex_columns(df_cnt_pol)
                # rename_df_columns(df_cnt_pol, {'count_All':'N_All'})
                for i in pol_lst:
                    self.rename_df_columns(df_cnt_pol, {f'count_{i}':f'N_{i}'})
                    df_cnt_pol = cnt_percent(df_cnt_pol, i)

                df_cnt_pol.replace(0, np.nan, inplace=True)
                df_cnt_pol.dropna(how='all', inplace=True)

                return self.add_new_level(df_cnt_pol, [f'Численность по полу ({param})'])


        def cnt_percent(df, pol):
            """Вычисляет % от численности по полу"""
            if not df[f'N_{pol}'].empty:
                df[f'{pol}%'] = round(df[f'N_{pol}'][:-1] / max(df[f'N_{pol}']), 3) * 100
                df[f'{pol}%'][-1] = round(df[f'N_{pol}'][-1] / df[f'count_All'][-1], 3) * 100
                return df
            else:
                return df
            
        # Кусок 4
        df4_bio = df2_bin[(df2_bin['bio_info'] == 1) & ((df2_bin['Пол'] == 'M') | (df2_bin['Пол'] == 'F'))]

        def make_analys_pit_modified(df, param):
            """Вычисляет полевой анализ питания"""
            pol_lst = df['Пол'].unique()

            df4_cnt_pit = df.pivot_table(index=['Диапазон'], columns=['Пол'], values='bio_info', aggfunc=['count'], margins=True)
            if df4_cnt_pit.empty:
                return df4_cnt_pit
            else:
                df4_cnt_pit = self.drop_multiindex_columns(df4_cnt_pit)

                for i in pol_lst:
                    self.rename_df_columns(df4_cnt_pit, {f'count_{i}':f'{i}'})

                df4_cnt_pit.replace(0, np.nan, inplace=True)
                df4_cnt_pit.dropna(how='all', inplace=True)

                return self.add_new_level(df4_cnt_pit, [f'Полевой анализ питания ({param})'])
            
        # ТАБЛИЦА 2\\ ЖИРНОСТЬ В ПРОЦЕНТАХ
        # Кусок 1\ Кол-во рыб и %

        # Кусок 2\\ Жирность
        def fat_table(df, count_by, param):
            if df.empty:
                df = pd.DataFrame()
                self.add_new_level(df, [f'Жирность ({param})'])
                return df

            else:
                if count_by == 'by_scope':
                    df_filt = df[df['salo'] >= 0]
                    df2_res = df_filt.pivot_table(index=['Диапазон'], columns='Пол', values='salo', aggfunc=['count','mean'], margins=True)

                else:
                    filt = df['Вес печени'] > 0
                    t1_fat = df[filt]
                    t1_fat.loc[:, 'Жирность'] = (t1_fat['Вес печени'] / t1_fat['Вес']) * 100
                    t1_fat = t1_fat.round(1)

                    df2_res = t1_fat.pivot_table(index=['Диапазон'], columns='Пол', values='Жирность', aggfunc=['count', 'mean'], margins=True).round(3)

                self.rename_df_columns(df2_res, {'count': 'N_Жир', 'mean': 'Жирность'})

                df2_res.replace(0, np.nan, inplace=True)
                df2_res.dropna(how='all', inplace=True)

                return df2_res
            

        # Кусок 3\ Коэф.половой зрелости
        def kpz_table(df, param):
            if df.empty:
                df = pd.DataFrame()
                self.add_new_level(df, [f'Коэф. половой зрелости ({param})'])
                return df
            else:
                # filter
                filt = (df['w_gonad'] > 0) & (df['Вес'] > 0)
                df_filt = df[filt]
                # print(df_filt)
                df_filt['w_gonad'] = df_filt['w_gonad'].astype(float)
                df_filt['Вес'] = df_filt['Вес'].astype(float)

                # N kpz
                df_N = df_filt.pivot_table(index=['Диапазон'], columns='Пол', values='w_gonad', aggfunc='count', margins=True)
                self.rename_df_columns(df_N, {'F':'N_F', 'M':'N_M', 'J':'N_J', 'All':'N_All'})

                # kpz
                df_sum_g = df_filt.pivot_table(index=['Диапазон'], columns='Пол', values='w_gonad', aggfunc='sum', margins=True)
                df_sum_w = df_filt.pivot_table(index=['Диапазон'], columns='Пол', values='Вес', aggfunc='sum', margins=True)

                df_kpz = round(df_sum_g * 100 / df_sum_w, 2)
                
                self.rename_df_columns(df_kpz, {'F':'КПЗ_F', 'M':'КПЗ_M', 'J':'КПЗ_J', 'All':'КПЗ_All'})

                df_kpz_merg = pd.merge(df_N, df_kpz, on=['Диапазон'], how='left')
                if df_kpz_merg.empty:
                    return df_kpz_merg
                else:
                    self.add_new_level(df_kpz_merg, [f'Коэф. половой зрелости ({param})'])

                    df_kpz_merg.replace(0, np.nan, inplace=True)
                    df_kpz_merg.dropna(how='all', inplace=True)

                    return df_kpz_merg


        # ТАБЛИЦА 3\\ РАСПРЕДЕЛЕНИЕ ПО БАЛЛАМ ЖИРНОСТИ
        df3_salo = df2_bin[df2_bin['salo'] >= 0]
        def make_distrib_salo(df, count_by, param):
            if df.empty:
                return pd.DataFrame()
            else:
                if count_by == 'by_scope':
                    df3_cnt_salo = df.pivot_table(index=['salo'], columns='Пол', values='cnt', aggfunc='count', margins=True)
                    df3_cnt_salo.rename_axis(index={'salo': 'Балл'}, inplace=True)
                    df3_cnt_salo.rename(index={'All':'Итого'}, inplace=True)
                    self.rename_df_columns(df3_cnt_salo, {'All':'Всего'})
                else:
                    df3_cnt_salo = pd.DataFrame()

                return self.add_new_level(df3_cnt_salo, [f'Распределние по баллам жирности ({param})'])


        # ТАБЛИЦА 4\\ НАПОЛНЕНИЕ ЖЕЛУДКА В БАЛЛАХ
        def fill_puzo_score(df, param):
            df_filt = df[df['Пол'] != 'I']

            df_puzo_score = df_filt.pivot_table(index=['puzo'], columns='Пол', values='Диапазон',aggfunc='count', margins=True)
            df_puzo_avg = df_filt.pivot_table(index='Пол', values='puzo',aggfunc='mean', margins=True).round(3)
            df_puzo_avg = df_puzo_avg.T

            df_puzo_avg.rename(index={'puzo':'Ср.балл'}, inplace=True)

            df_puzo = pd.concat([df_puzo_score, df_puzo_avg], axis=0)
            df_puzo.rename(index={'All':'Итого'}, inplace=True)
            self.rename_df_columns(df_puzo, {'All':'Всего'})

            return self.add_new_level(df_puzo, [f'Наполнение желудка в баллах ({param})'])


        # ТАБЛИЦА 5
        def make_cnt_by_pol_modified(df, indx, val, param):
            # pol_lst = df['Пол'].unique()
            df_cnt_pol = df.pivot_table(index=[indx], columns=['Пол'], values=val, aggfunc=['count'], margins=True)
            # print(df_cnt_pol)
            if df_cnt_pol.empty:
                return df_cnt_pol
            else:
                df_cnt_pol = self.drop_multiindex_columns(df_cnt_pol)
                lst_pol = df_cnt_pol.columns
                # print(lst_pol[0:-1])
                # rename_df_columns(df_cnt_pol, {'count_All':'N_All'})

                for i in lst_pol:
                #     rename_df_columns(df_cnt_pol, {f'{i}':f'N_{i}'})
                    df_cnt_pol = cnt_percent_modified(df_cnt_pol, i)

                df_cnt_pol.replace(0, np.nan, inplace=True)
                df_cnt_pol.dropna(how='all', inplace=True)

                return self.add_new_level(df_cnt_pol, [f'Численность по полу ({param})'])


        def cnt_percent_modified(df, pol):
            df[f'{pol}%'] = round(df[f'{pol}'][:-1] / max(df[f'{pol}']), 3) * 100
            df[f'{pol}%'][-1] = round(df[f'{pol}'][-1] / df[f'count_All'][-1], 3) * 100
            return df


        # ТАБЛИЦА 6
        df1_bin_filt = df1_bin[['ulkl', 'trkl', 'kod_fish', 'Глубина', 'Месяц_Год', 'Месяц', 'Год']]
        df6_bio = pd.merge(df6, df1_bin_filt, on=['ulkl', 'trkl', 'kod_fish'], how='inner')
        df6_bio = df6_bio.drop_duplicates()
        df6_bio['cnt'] = 1


        def make_nutrition_struct_modified(df, param):
            df6_kodpit = df.pivot_table(index=['Код', 'Наименование'], values='cnt', aggfunc='count', margins=True)
            self.rename_df_columns(df6_kodpit, {'cnt':'Количество'})
            df6_kodpit['%'] = round(df6_kodpit['Количество'] / max(df6_kodpit['Количество']), 4) * 100

            return self.add_new_level(df6_kodpit, [f'Структура питания ({param})'])

        # print(df6_bio)

        # # Создание всех таблиц с параметрами\без
        def merge_tables(*tables):
            result = tables[0]
            for table in tables[1:]:
                result = pd.merge(result, table, on=['Диапазон'], how='left')
            return result


        def process_unique_params(df0, df1_bin, df2_bin, df3_bio, df3_salo, df4_bio, df6_bio, df_kvsk, df_puzo0, df_k_vsk1, df_k_vsk0, var_count_tab, fat_count):
            """Создаем все таблицы вне зависимости от дополнительных параметров"""
            list_results = {'Header':[], 'Таблица 1': [], 'ЖИРНОСТЬ В ПРОЦЕНТАХ': [],
                            'РАСПРЕДЕЛЕНИЕ ПО БАЛЛАМ ЖИРНОСТИ': [], 'НАПОЛНЕНИЕ ЖЕЛУДКА В БАЛЛАХ': [],
                            'РАСПРЕДЕЛЕНИЕ ПО СТАДИЯМ ЗРЕЛОСТИ': [], 'СТРУКТУРА ПИТАНИЯ': []}
            # print(df_k_vsk0)
            self.rename_df_columns(df_k_vsk0, {'kodvid':'Код рыбки'})
            df_k_vsk0_merg = pd.merge(df_k_vsk0, df2_bin[['bikl', 'Код рыбки', 'Рыбка', 'bio_info', 'u_key', 'Глубина', 'Месяц_Год', 'Месяц', 'Год']],
                                      on=['bikl', 'Код рыбки', 'bio_info', 'u_key'], how='inner')
            df_k_vsk0_merg.drop_duplicates(inplace=True)

            output_strings = {
                ('by_year', 'by_month', 'by_fish'): 'Год месяц, рыбка:',
                ('by_glub', 'by_fish'): 'Глубина, рыбка:',
                ('by_month', 'by_fish'): 'Месяц, рыбка:',
                ('by_year', 'by_fish'): 'Год, рыбка:',
                ('by_year', 'by_month'): 'Год, месяц:',
                ('by_glub',): 'Глубина:',
                ('by_fish',): 'Рыбка:',
                ('by_month',): 'Месяц:',
                ('by_year',): 'Год:',
            }

            df0_header = make_table_header(df0)
            list_results['Header'].append(df0_header)

            # print(var_count_tab)
            try:

                if (len(var_count_tab) == 1) or (len(var_count_tab) == 2 and var_count_tab == ['by_year', 'by_month']):

                    if len(var_count_tab) == 2:
                        if var_count_tab == ['by_year', 'by_month']:
                            filt_col = 'Месяц_Год'
                    else:
                        if var_count_tab[0] == 'by_month':
                            filt_col = 'Месяц'
                        elif var_count_tab[0] == 'by_year':
                            filt_col = 'Год'
                        elif var_count_tab[0] == 'by_fish':
                            filt_col = 'Рыбка'
                        elif var_count_tab[0] == 'by_glub':
                            filt_col = 'Глубина'

                    unique_param = pd.unique(df1_bin[filt_col].values.ravel())

                    df_kvsk = df2_bin.copy()
                    df_puzo0 = df_kvsk[(df_kvsk['puzo'] >= 0) & (df_kvsk['puzo'] <= 4)]

                    df_k_vsk1 = df2_bin.copy()
                    # print(df_k_vsk1)

                    # print(unique_param)
                    for param in sorted(unique_param):
                        curr_dfs = {'df1': df1_bin[df1_bin[filt_col] == param],
                                    'df2': df2_bin[df2_bin[filt_col] == param],
                                    'df3': df3_bio[df3_bio[filt_col] == param],
                                    'df3_salo': df3_salo[df3_salo[filt_col] == param],
                                    'df4': df4_bio[df4_bio[filt_col] == param],
                                    'df6': df6_bio[df6_bio[filt_col] == param],
                                    'df_puzo0': df_puzo0[df_puzo0[filt_col] == param],
                                    'k_kvsk0': df_k_vsk0_merg[df_k_vsk0_merg[filt_col] == param],
                                    'df_k_vsk1': df_k_vsk1[df_k_vsk1[filt_col] == param]
                                    }
                        # Table 1
                        curr_piece1 = make_part_cnt_perc_modified(curr_dfs['df1'], 'Диапазон', 'Кол-во', f'{output_strings[tuple(var_count_tab)]} {param}')
                        curr_piece2 = make_mean_wes_modified(curr_dfs['df2'], f'{output_strings[tuple(var_count_tab)]} {param}')
                        curr_piece3 = make_cnt_by_pol_modified(curr_dfs['df3'], 'Диапазон', 'bio_info', f'{output_strings[tuple(var_count_tab)]} {param}')
                        curr_piece4 = make_analys_pit_modified(curr_dfs['df4'], f'{output_strings[tuple(var_count_tab)]} {param}')
                        all_pieces_table1 = merge_tables(curr_piece1, curr_piece2, curr_piece3, curr_piece4)
                        all_pieces_table1.fillna('-', inplace=True)
                        list_results['Таблица 1'].append(all_pieces_table1)

                        # Table 2
                        curr_piece2_1 = make_part_cnt_perc_modified(curr_dfs['df1'], 'Диапазон', 'Кол-во', f'{output_strings[tuple(var_count_tab)]} {param}')
                        curr_piece2_2 = fat_table(curr_dfs['df2'], fat_count, f'{output_strings[tuple(var_count_tab)]} {param}')
                        if not curr_piece2_2.empty:
                            pieces_12 = merge_tables(curr_piece2_1, curr_piece2_2)
                        else:
                            pieces_12 = curr_piece2_1
                        curr_piece2_3 = kpz_table(curr_dfs['df2'], f'{output_strings[tuple(var_count_tab)]} {param}')
                        if not curr_piece2_3.empty:
                            all_pieces_table2 = merge_tables(pieces_12, curr_piece2_3)
                        else:
                            all_pieces_table2 = pieces_12
                        list_results['ЖИРНОСТЬ В ПРОЦЕНТАХ'].append(all_pieces_table2)

                        # Table 3
                        curr_piece3 = make_distrib_salo(curr_dfs['df3_salo'], fat_count, f'{output_strings[tuple(var_count_tab)]} {param}')
                        list_results['РАСПРЕДЕЛЕНИЕ ПО БАЛЛАМ ЖИРНОСТИ'].append(curr_piece3)

                        # Table 4
                        table4 = fill_puzo_score(curr_dfs['df2'], f'{output_strings[tuple(var_count_tab)]} {param}')
                        list_results['НАПОЛНЕНИЕ ЖЕЛУДКА В БАЛЛАХ'].append(table4)

                        # Table 5
                        curr_piece5_1 = make_part_cnt_perc_modified(curr_dfs['df2'], 'Зрелость', 'Кол-во', f'{output_strings[tuple(var_count_tab)]} {param}')
                        curr_piece5_2 = make_cnt_by_pol_modified(curr_dfs['df2'], 'Зрелость', 'Кол-во', f'{output_strings[tuple(var_count_tab)]} {param}')
                        all_pieces_table5 = pd.merge(curr_piece5_1, curr_piece5_2, on=['Зрелость'], how='left')
                        list_results['РАСПРЕДЕЛЕНИЕ ПО СТАДИЯМ ЗРЕЛОСТИ'].append(all_pieces_table5)

                        # Table 6
                        table6 = make_nutrition_struct_modified(curr_dfs['df6'], f'{output_strings[tuple(var_count_tab)]} {param}')
                        list_results['СТРУКТУРА ПИТАНИЯ'].append(table6)
                        # количества желудков
                        kvsk = curr_dfs['df_puzo0'].shape[0]
                        k_kvsk0 = curr_dfs['k_kvsk0'].shape[0]
                        # кол-во корректно вскрытых желудков
                        k_vsk1_filt = curr_dfs['df_k_vsk1'][curr_dfs['df_k_vsk1']['puzo'] == 0]
                        k_vsk1 = k_vsk1_filt.shape[0]
                        k_kvsk = k_vsk1 + k_kvsk0
                        # количество желудков с пищей
                        kvsk_p = curr_dfs['k_kvsk0'].shape[0]
                        df_k = pd.DataFrame(index=['Количество вскрытых желудков', 
                                                'Количество корректно вскрытых желудков', 
                                                'Количество желудков с пищей'], 
                                                data=[kvsk, k_kvsk, kvsk_p], columns=[''])
                        list_results['СТРУКТУРА ПИТАНИЯ'].append(df_k)


                elif (len(var_count_tab) >= 2 and 'by_fish' in var_count_tab):
                    df_kvsk = df2_bin.copy()
                    df_puzo0 = df_kvsk[(df_kvsk['puzo'] >= 0) & (df_kvsk['puzo'] <= 4)]

                    df_k_vsk1 = df2_bin.copy()
                    print(var_count_tab)
                    # print(var_count_tab[0:1])
                
                    if len(var_count_tab) == 3 and var_count_tab[0:2] == ['by_year', 'by_month']:
                        filt_col = 'Месяц_Год'
                    if len(var_count_tab) == 2:
                        if var_count_tab[0] == 'by_month':
                            filt_col = 'Месяц'
                        elif var_count_tab[0] == 'by_year':
                            filt_col = 'Год'
                        elif var_count_tab[0] == 'by_fish':
                            filt_col = 'Рыбка'
                        elif var_count_tab[0] == 'by_glub':
                            filt_col = 'Глубина'


                    uniq_fish = pd.unique(df1_bin['Рыбка'].values.ravel())
                    # print(filt_col)
                    for fish in uniq_fish:
                        df1_bin_ff = df1_bin[df1_bin['Рыбка'] == fish]
                        unique_param = pd.unique(df1_bin_ff[filt_col].values.ravel())
                        # print(unique_param)

                        df1_pretotal = df1_bin[df1_bin['Рыбка'] == fish]
                        df2_pretotal = df2_bin[df2_bin['Рыбка'] == fish]
                        df3_pretotal = df3_bio[df3_bio['Рыбка'] == fish]
                        df3_salo_pretotal = df3_salo[df3_salo['Рыбка'] == fish]
                        df4_pretotal = df4_bio[df4_bio['Рыбка'] == fish]
                        df6_pretotal = df6_bio[df6_bio['Рыбка'] == fish]

                        df_puzo0_pretotal = df_puzo0[df_puzo0['Рыбка'] == fish]
                        df_k_kvsk0_pretotal = df_k_vsk0_merg[df_k_vsk0_merg['Рыбка'] == fish]
                        df_k_vsk1_pretotal = df_k_vsk1[df_k_vsk1['Рыбка'] == fish]

                        for param in sorted(unique_param):
                            curr_dfs = {
                                'df1': df1_bin[df1_bin[filt_col] == param],
                                'df2': df2_bin[df2_bin[filt_col] == param],
                                'df3': df3_bio[df3_bio[filt_col] == param],
                                'df4': df4_bio[df4_bio[filt_col] == param],
                                'df5': df3_salo[df3_salo[filt_col] == param],
                                'df6': df6_bio[df6_bio[filt_col] == param],
                                'df_puzo0': df_puzo0[df_puzo0[filt_col] == param],
                                'k_kvsk0': df_k_vsk0_merg[df_k_vsk0_merg[filt_col] == param],
                                'df_k_vsk1': df_k_vsk1[df_k_vsk1[filt_col] == param]
                            }

                            # Table 1
                            curr_piece1 = make_part_cnt_perc_modified(curr_dfs['df1'], 'Диапазон', 'Кол-во', f'{output_strings[tuple(var_count_tab)]} {param}, ({fish.strip()})')
                            curr_piece2 = make_mean_wes_modified(curr_dfs['df2'], f'{output_strings[tuple(var_count_tab)]} {param}, ({fish.strip()})')
                            curr_piece3 = make_cnt_by_pol_modified(curr_dfs['df3'], 'Диапазон', 'bio_info', f'{output_strings[tuple(var_count_tab)]} {param}, ({fish.strip()})')
                            curr_piece4 = make_analys_pit_modified(curr_dfs['df4'], f'{output_strings[tuple(var_count_tab)]} {param}, ({fish.strip()})')

                            all_pieces_table1 = merge_tables(curr_piece1, curr_piece2, curr_piece3, curr_piece4)
                            all_pieces_table1.fillna('-', inplace=True)
                            list_results['Таблица 1'].append(all_pieces_table1)

                            # Table 2
                            curr_piece2_1 = make_part_cnt_perc_modified(curr_dfs['df1'], 'Диапазон', 'Кол-во', f'{output_strings[tuple(var_count_tab)]} {param}, ({fish.strip()})')
                            curr_piece2_2 = fat_table(curr_dfs['df2'], fat_count, f'{output_strings[tuple(var_count_tab)]} {param}, ({fish.strip()})')
                            if not curr_piece2_2.empty:
                                pieces_12 = merge_tables(curr_piece2_1, curr_piece2_2)
                            else:
                                pieces_12 = curr_piece2_1

                            curr_piece2_3 = kpz_table(curr_dfs['df2'], f'{output_strings[tuple(var_count_tab)]} {param}, ({fish.strip()})')
                            if not curr_piece2_3.empty:
                                all_pieces_table2 = merge_tables(pieces_12, curr_piece2_3)
                            else:
                                all_pieces_table2 = pieces_12

                            all_pieces_table2.fillna('-', inplace=True)
                            list_results['ЖИРНОСТЬ В ПРОЦЕНТАХ'].append(all_pieces_table2)

                            # Table 3
                            curr_piece3 = make_distrib_salo(curr_dfs['df5'], fat_count, f'{output_strings[tuple(var_count_tab)]} {param}, ({fish.strip()})')
                            curr_piece3.fillna('-', inplace=True)
                            list_results['РАСПРЕДЕЛЕНИЕ ПО БАЛЛАМ ЖИРНОСТИ'].append(curr_piece3)

                            # Table 4
                            table4 = fill_puzo_score(curr_dfs['df2'], f'{output_strings[tuple(var_count_tab)]} {param}, ({fish.strip()})')
                            table4.fillna('-', inplace=True)
                            list_results['НАПОЛНЕНИЕ ЖЕЛУДКА В БАЛЛАХ'].append(table4)

                            # Table 5
                            curr_piece5_1 = make_part_cnt_perc_modified(curr_dfs['df2'], 'Зрелость', 'Кол-во', f'{output_strings[tuple(var_count_tab)]} {param}, ({fish.strip()})')
                            curr_piece5_2 = make_cnt_by_pol_modified(curr_dfs['df2'], 'Зрелость', 'Кол-во', f'{output_strings[tuple(var_count_tab)]} {param}, ({fish.strip()})')
                            all_pieces_table5 = pd.merge(curr_piece5_1, curr_piece5_2, on=['Зрелость'], how='left')
                            all_pieces_table5.fillna('-', inplace=True)
                            list_results['РАСПРЕДЕЛЕНИЕ ПО СТАДИЯМ ЗРЕЛОСТИ'].append(all_pieces_table5)

                            # Table 6
                            table6 = make_nutrition_struct_modified(curr_dfs['df6'], f'{output_strings[tuple(var_count_tab)]} {param}, {fish.strip()}')
                            table6.fillna('-', inplace=True)
                            list_results['СТРУКТУРА ПИТАНИЯ'].append(table6)
                            # количества желудков
                            kvsk = curr_dfs['df_puzo0'].shape[0]
                            k_kvsk0 = curr_dfs['k_kvsk0'].shape[0]
                            # кол-во корректно вскрытых желудков
                            k_vsk1_filt = curr_dfs['df_k_vsk1'][curr_dfs['df_k_vsk1']['puzo'] == 0]
                            k_vsk1 = k_vsk1_filt.shape[0]
                            k_kvsk = k_vsk1 + k_kvsk0
                            # количество желудков с пищей
                            kvsk_p = curr_dfs['k_kvsk0'].shape[0]
                            df_k = pd.DataFrame(index=['Количество вскрытых желудков', 
                                                    'Количество корректно вскрытых желудков', 
                                                    'Количество желудков с пищей'], 
                                                    data=[kvsk, k_kvsk, kvsk_p], columns=[''])
                            list_results['СТРУКТУРА ПИТАНИЯ'].append(df_k)
                        
                        # pretotal 1
                        curr_piece1_pretotal = make_part_cnt_perc_modified(df1_bin_ff, 'Диапазон', 'Кол-во', f'{output_strings[tuple(var_count_tab)]} Итог({fish.strip()})')
                        curr_piece2_pretotal = make_mean_wes_modified(df2_pretotal, f'{output_strings[tuple(var_count_tab)]} Итог({fish.strip()})')
                        curr_piece3_pretotal = make_cnt_by_pol_modified(df2_pretotal, 'Диапазон', 'bio_info', f'{output_strings[tuple(var_count_tab)]} Итог({fish.strip()})')
                        curr_piece4_pretotal = make_analys_pit_modified(df4_pretotal, f'{output_strings[tuple(var_count_tab)]} Итог({fish.strip()})')

                        table1_fish_total = merge_tables(curr_piece1_pretotal, curr_piece2_pretotal, curr_piece3_pretotal, curr_piece4_pretotal)
                        table1_fish_total.fillna('-', inplace=True)
                        list_results['Таблица 1'].append(table1_fish_total)

                        # pretotal 2
                        curr_piece2_1_pretotal = make_part_cnt_perc_modified(df1_pretotal, 'Диапазон', 'Кол-во', f'{output_strings[tuple(var_count_tab)]} {param}, ({fish.strip()})')
                        curr_piece2_2_pretotal = fat_table(df2_pretotal, fat_count, f'{output_strings[tuple(var_count_tab)]} {param}, ({fish.strip()})')
                        if not curr_piece2_2.empty:
                            pieces_12_pretotal = merge_tables(curr_piece2_1_pretotal, curr_piece2_2_pretotal)
                        else:
                            pieces_12_pretotal = curr_piece2_1_pretotal
                        curr_piece2_3_pretotal = kpz_table(df2_pretotal, f'{output_strings[tuple(var_count_tab)]} {param}, ({fish.strip()})')
                        if not curr_piece2_3_pretotal.empty:
                            all_pieces_table2_pretotal = merge_tables(pieces_12_pretotal, curr_piece2_3_pretotal)
                        else:
                            all_pieces_table2_pretotal = pieces_12_pretotal
                        all_pieces_table2_pretotal.fillna('-', inplace=True)
                        list_results['ЖИРНОСТЬ В ПРОЦЕНТАХ'].append(all_pieces_table2_pretotal)

                        # pretotal 3
                        curr_piece3_pretotal = make_distrib_salo(df3_salo_pretotal, fat_count, f'{output_strings[tuple(var_count_tab)]} {param}, ({fish.strip()})')
                        curr_piece3_pretotal.fillna('-', inplace=True)
                        list_results['РАСПРЕДЕЛЕНИЕ ПО БАЛЛАМ ЖИРНОСТИ'].append(curr_piece3_pretotal)

                        # pretotal 4
                        table4_pretotal = fill_puzo_score(df2_pretotal, f'{output_strings[tuple(var_count_tab)]} {param}, ({fish.strip()})')
                        table4_pretotal.fillna('-', inplace=True)
                        list_results['НАПОЛНЕНИЕ ЖЕЛУДКА В БАЛЛАХ'].append(table4_pretotal)

                        # pretotal 5
                        curr_piece5_1_pretotal = make_part_cnt_perc_modified(df2_pretotal, 'Зрелость', 'Кол-во', f'{output_strings[tuple(var_count_tab)]} {param}, ({fish.strip()})')
                        curr_piece5_2_pretotal = make_cnt_by_pol_modified(df2_pretotal, 'Зрелость', 'Кол-во', f'{output_strings[tuple(var_count_tab)]} {param}, ({fish.strip()})')
                        all_pieces_table5_pretotal = pd.merge(curr_piece5_1_pretotal, curr_piece5_2_pretotal, on=['Зрелость'], how='left')
                        all_pieces_table5_pretotal.fillna('-', inplace=True)
                        list_results['РАСПРЕДЕЛЕНИЕ ПО СТАДИЯМ ЗРЕЛОСТИ'].append(all_pieces_table5_pretotal)

                        # pretotal 6
                        table6_pretotal = make_nutrition_struct_modified(df6_pretotal, f'{output_strings[tuple(var_count_tab)]} {param}, {fish.strip()}')
                        table6_pretotal.fillna('-', inplace=True)
                        list_results['СТРУКТУРА ПИТАНИЯ'].append(table6_pretotal)
                        # количество желудков
                        kvsk_pretotal = df_puzo0_pretotal.shape[0]
                        k_kvsk0_pretotal = df_k_kvsk0_pretotal.shape[0] 
                        k_vsk1_filt_pretotal = df_k_vsk1_pretotal[df_k_vsk1_pretotal['puzo'] == 0]
                        k_vsk1_pretotal = k_vsk1_filt_pretotal.shape[0]
                        k_kvsk_pretotal = k_vsk1_pretotal + k_kvsk0_pretotal
                        kvsk_p_pretotal = df_k_kvsk0_pretotal.shape[0]
                        df_k_pretotal = pd.DataFrame(index=['Количество вскрытых желудков', 
                                                        'Количество корректно вскрытых желудков', 
                                                        'Количество желудков с пищей'], 
                                                        data=[kvsk_pretotal, k_kvsk_pretotal, kvsk_p_pretotal], columns=[''])
                        list_results['СТРУКТУРА ПИТАНИЯ'].append(df_k_pretotal)
                # Итоги
                # 1
                summary1 = merge_tables(make_part_cnt_perc_modified(df1_bin, 'Диапазон', 'Кол-во', 'Итого'),
                                        make_mean_wes_modified(df2_bin, 'Итого'),
                                        make_cnt_by_pol_modified(df3_bio, 'Диапазон', 'bio_info', 'Итого'),
                                        make_analys_pit_modified(df4_bio, 'Итого'))
                summary1.fillna('-', inplace=True)
                list_results['Таблица 1'].append(summary1)

                # 2
                summary2 = merge_tables(
                    make_part_cnt_perc_modified(df1_bin, 'Диапазон', 'Кол-во', 'Итого'),
                    fat_table(df2_bin, fat_count, 'Итого'),
                    kpz_table(df2_bin, 'Итого')
                )
                summary2.fillna('-', inplace=True)
                list_results['ЖИРНОСТЬ В ПРОЦЕНТАХ'].append(summary2)

                # 3
                summary3 = make_distrib_salo(df3_salo, fat_count, 'Итого')
                summary3.fillna('-', inplace=True)
                list_results['РАСПРЕДЕЛЕНИЕ ПО БАЛЛАМ ЖИРНОСТИ'].append(summary3)

                # 4
                summary4 = fill_puzo_score(df2_bin, 'Итого')
                summary4.fillna('-', inplace=True)
                list_results['НАПОЛНЕНИЕ ЖЕЛУДКА В БАЛЛАХ'].append(summary4)

                # 5
                piece5_1 = make_part_cnt_perc_modified(df2_bin, 'Зрелость', 'Кол-во', 'Итого')
                piece5_2 = make_cnt_by_pol_modified(df2_bin, 'Зрелость', 'Кол-во', 'Итого')
                summary5 = pd.merge(piece5_1, piece5_2, on=['Зрелость'], how='left')
                summary5.fillna('-', inplace=True)
                list_results['РАСПРЕДЕЛЕНИЕ ПО СТАДИЯМ ЗРЕЛОСТИ'].append(summary5)

                # 6
                summary6 = make_nutrition_struct_modified(df6_bio, 'Итого')
                summary6.fillna('-', inplace=True)
                list_results['СТРУКТУРА ПИТАНИЯ'].append(summary6)
                kvsk_total = df_puzo0.shape[0]
                k_kvsk0_total = df_k_vsk0_merg.shape[0]
                k_vsk1_filt_total = df_k_vsk1[df_k_vsk1['puzo'] == 0]
                k_vsk1_total = k_vsk1_filt_total.shape[0]
                k_kvsk_total = k_vsk1_total + k_kvsk0_total
                kvsk_p_total = df_k_vsk0_merg.shape[0]
                df_k_total = pd.DataFrame(index=['Количество вскрытых желудков', 
                                                'Количество корректно вскрытых желудков', 
                                                'Количество желудков с пищей'], 
                                                data=[kvsk_total, k_kvsk_total, kvsk_p_total], columns=[''])
                list_results['СТРУКТУРА ПИТАНИЯ'].append(df_k_total)

            except:
                df0_header = pd.DataFrame()
                list_results['Header'].append(df0_header)
                list_results['Таблица 1'].append(df0_header)
                list_results['ЖИРНОСТЬ В ПРОЦЕНТАХ'].append(df0_header)
                list_results['РАСПРЕДЕЛЕНИЕ ПО БАЛЛАМ ЖИРНОСТИ'].append(df0_header)
                list_results['НАПОЛНЕНИЕ ЖЕЛУДКА В БАЛЛАХ'].append(df0_header)
                list_results['РАСПРЕДЕЛЕНИЕ ПО СТАДИЯМ ЗРЕЛОСТИ'].append(df0_header)
                list_results['СТРУКТУРА ПИТАНИЯ'].append(df0_header)


            return list_results


        results = process_unique_params(df0, df1_bin, df2_bin, df3_bio, df3_salo, df4_bio, df6_bio, df_kvsk, df_puzo0, df_k_vsk1, df_k_vsk0, cnt_val, fat_count)
        # print(results)

        return [results]

    # СПРАВКИ ПО ЗАПРОСУ
    # BioTral
    def datas_for_biotral(self, curr_form):
        # print(curr_form)
        """Добываем данные для BioTral и расширенного"""
        if curr_form == 'bio-tral':
            sql_t0 = "select ship, reis, trl, data from tral_view;"

            sql_t1 = """
            select distinct tv.ship, tv.reis, tv.trl, tv.data, tv.rloc, tv.rzon, tv.shir, tv.dolg, 
            tv.prod, tv.orlov, tv.gor, tv.ulov, uv.u_prc, uv.u_wes, round(uv.u_wes/tv.prod, 2) as wes_h, uv.u_num, 
            round(uv.u_num/tv.prod, 2) as num_h
            from tral_view tv
            join ulov_view uv on tv.trkl=uv.trkl
            order by ship, reis, data, trl;
            """
        else:
            sql_t0 = """
            select ship, reis, trl, data 
            from tral_view 
            where ship in ('UANA', 'UFJJ') and to_char(data, 'MM')::int  between 5 and 10;
            """

            sql_t1 = """
            select distinct tv.ship, tv.reis, tv.trl, tv.data, tv.rloc, tv.rzon, tv.shir, tv.dolg, 
            tv.speed, tv.nach, tv.prod, tv.gor, tv.ulov, uv.u_prc, uv.u_wes, round(uv.u_wes/tv.prod, 2) as wes_h, 
            uv.u_num, round(uv.u_num/tv.prod, 2) as num_h
            from tral_view tv
            join ulov_view uv on tv.trkl=uv.trkl
            where tv.ship in ('UANA', 'UFJJ') and to_char(tv.data, 'MM')::int  between 5 and 10
            order by ship, reis, data, trl;
            """            
        
        try:
            res_0 = self.make_dict_request(sql_t0)
            res_1 = self.make_dict_request(sql_t1)

            t0 = pd.DataFrame(res_0)
            t1 = pd.DataFrame(res_1)

            if res_0 or res_1:
                try:
                    result = self.counting_biotral([t0, t1], curr_form)
                    return result
                except Exception as e:
                    print(f'Ошибка: {e}')
                    return []
            else:
                return [pd.DataFrame(), pd.DataFrame()]
            
        except Exception as e:
            print(f'Ошибка чтения из БД: {e}')
            return []


    def counting_biotral(self, data, curr_form):
        # Начальная первая таблица
        df0 = data[0]
        df1 = data[1]

        if df0.empty and df1.empty:
            return [pd.DataFrame(), pd.DataFrame()]
        else:
            self.rename_df_columns(df0, {'ship':'Позывной', 'reis':'Рейс', 'trl':'Трал', 'data':'Дата'})

            df_header = df0.pivot_table(index=['Позывной', 'Рейс'], values='Дата', aggfunc=['min', 'max'])
            self.rename_df_columns(df_header, {'min':'Начало рейса', 'max':'Окончание рейса'})

            # Кол-во тралений
            cnt_tral = df0.shape[0]
            df_cnt_tral = pd.DataFrame(index=['Кол-во тралений'], data=[cnt_tral])
            self.rename_df_columns(df_cnt_tral, {0:''})

            # Основная таблциа
            self.rename_df_columns(df1, {'ship':'Позывной', 'reis':'Рейс', 'trl':'Трал', 'data':'Дата',
                                        'rloc':'Лок.район', 'rzon':'Экономич.зона', 'shir':'Широта',
                                        'dolg':'Долгота', 'prod':'Прод-сть', 'orlov':'Орудие лова',
                                        'gor':'Горизонт лова', 'ulov':'Общ.улов', 'u_prc':'%',
                                        'speed':'Скорость траления', 'nach':'Время нач.трал.',
                                        'u_wes':'кг.', 'wes_h':'кг/час', 'u_num':'шт.', 'num_h':'шт/час'})
            
            df1['Общ.улов'] = df1['Общ.улов'].astype(int)
            
            # Итоги
            ulov_total = sum(df1['Общ.улов'])
            wes_total = sum(df1['кг.'])
            num_total = sum(df1['шт.'])

            df_total = pd.DataFrame(index=['Итоги'], columns=['Общ.улов', 'кг.', 'шт.'], data=[])
            df_total['Общ.улов'] = ulov_total
            df_total['кг.'] = wes_total
            df_total['шт.'] = num_total

            # Среднее
            avg_total = round(ulov_total / cnt_tral, 2)
            avg_perc = round(sum(df1['%']) / cnt_tral, 2)
            avg_wes = round(wes_total / cnt_tral, 2)
            avg_wes_h = round(sum(df1['кг/час']) / cnt_tral, 2)
            avg_num = round(num_total / cnt_tral, 2)
            avg_num_h = round(sum(df1['шт/час']) / cnt_tral, 2)

            df_avg = pd.DataFrame(index=['В среднем за траление'], columns=['Общ.улов', '%', 'кг.', 'кг/час', 'шт.', 'шт/час'], data=[])
            df_avg['Общ.улов'] = avg_total
            df_avg['%'] = avg_perc
            df_avg['кг.'] = avg_wes
            df_avg['кг/час'] = avg_wes_h
            df_avg['шт.'] = avg_num
            df_avg['шт/час'] = avg_num_h

            def convert_time(x):
                hours = int(x)
                mins = round(x % 1, 3)
                time_str = f'{hours}:{int(mins*60)}'
                return time_str

            if curr_form == 'bio-tral-extension':
                df1['Время оконч.трал.'] = df1['Прод-сть'] + df1['Время нач.трал.']
                df1['Прод-сть'] = df1['Прод-сть'].apply(lambda x: convert_time(x))
                df1['Время нач.трал.'] = df1['Время нач.трал.'].apply(lambda x: convert_time(x))
                df1['Время оконч.трал.'] = df1['Время оконч.трал.'].apply(lambda x: convert_time(x))

            # print(df1)
                
            return [df_header, df1, df_cnt_tral, df_total, df_avg]


    # BioPap
    def datas_for_biopap(self, params):
            sql_t0 = "select ship, reis, trl, data from tral_view;"

            sql_t1 = """
            select * from biopap_view;
            """

            try:
                res_0 = self.make_dict_request(sql_t0)
                res_1 = self.make_dict_request(sql_t1)

                t0 = pd.DataFrame(res_0)
                t1 = pd.DataFrame(res_1)

                if res_0 or res_1:
                    try:
                        result = self.counting_biopap([t0, t1], params)
                        return result
                    except Exception as e:
                        print(f'Ошибка: {e}')
                        return []
                else:
                    return [pd.DataFrame(), pd.DataFrame()]
                
            except Exception as e:
                print(f'Ошибка чтения из БД: {e}')
                return []
            
        
    def counting_biopap(self, data, params):
        df0 = data[0]
        df1 = data[1]

        if df0.empty and df1.empty:
            return [pd.DataFrame(), pd.DataFrame()]
        else:
            self.rename_df_columns(df0, {'ship':'Позывной', 'reis':'Рейс', 'trl':'Трал', 'data':'Дата'})

            df_header = df0.pivot_table(index=['Позывной', 'Рейс'], values='Дата', aggfunc=['min', 'max'])
            self.rename_df_columns(df_header, {'min':'Начало рейса', 'max':'Окончание рейса'})

            # 1
            len_param, fat_param = params
            # Перевод в см
            df1['l_big_cm'] = df1['l_big'] / 10
            df1['l_lit_cm'] = df1['l_lit'] / 10

            # Ищем жирность
            df1['fat'] = (df1['w_pech'] / df1['wes_ob'])*100

            # Переименовывем на русский
            self.rename_df_columns(df1, {'l_big_cm':'Б.длина', 'l_lit_cm':'М.длина', 'fat':'Жирность',
                                         'ship':'Позывной', 'reis':'Рейс', 'trl':'Трал', 'wes_ob':'Вес',
                                         'salo':'Жирн(балл)', 'vozr1':'Возраст', 'puzo':'Балл(желуд.)',
                                         'pol':'Пол'})
            
            if len_param == 'l_big':
                len_val = 'Б.длина'
            else:
                len_val = 'М.длина'
            
            # Длина (см)
            df_pivot_len = df1.pivot_table(index=['Позывной', 'Рейс', 'Трал'], values=[len_val], aggfunc=['min', 'max', 'mean']).round(1)

            # Вес(гр)
            df_pivot_wes = df1.pivot_table(index=['Позывной', 'Рейс', 'Трал'], values=['Вес'], aggfunc=['min', 'max', 'mean']).round(1)

            # Жирность
            if fat_param == 'by_w_pech':
                df_pivot_fat = df1.pivot_table(index=['Позывной', 'Рейс', 'Трал'], values=['Жирность'], aggfunc=['min', 'max', 'mean']).round(1)
            else:
                df_pivot_fat = df1.pivot_table(index=['Позывной', 'Рейс', 'Трал'], values=['Жирн(балл)'], aggfunc=['min', 'max', 'mean']).round(1)

            # Возраст
            df_pivot_age = df1.pivot_table(index=['Позывной', 'Рейс', 'Трал'], values=['Возраст'], aggfunc=['min', 'max', 'mean']).round(1)

            # Сред. балл желуд. ???
            filt1 = df1['Балл(желуд.)'] >= 0
            filt2 = df1['Балл(желуд.)'] <= 4
            df_puzo = df1[filt1 & filt2]
            df_pivot_avg_puzo = df_puzo.pivot_table(index=['Позывной', 'Рейс', 'Трал'], values=['Балл(желуд.)'], aggfunc=['mean']).round(1)

            # Кол-во по полам
            df_pivot_cnt_pol = df1.pivot_table(index=['Позывной', 'Рейс', 'Трал'], values=['proba'], columns=['Пол'], aggfunc='count', margins=True).round(1)
            df_pivot_cnt_pol =df_pivot_cnt_pol[:-1]

            # соединение
            df_merge = pd.merge(df_pivot_len, df_pivot_wes, on=['Позывной', 'Рейс', 'Трал'], how='left')\
            .merge(df_pivot_fat, on=['Позывной', 'Рейс', 'Трал'], how='left')\
            .merge(df_pivot_age, on=['Позывной', 'Рейс', 'Трал'], how='left')\
            .merge(df_pivot_avg_puzo, on=['Позывной', 'Рейс', 'Трал'], how='left')\
            .merge(df_pivot_cnt_pol, on=['Позывной', 'Рейс', 'Трал'], how='left')

            df_merge.fillna('-', inplace=True)

            # Стадии зрелости
            # df_pivot_zrel = df1.pivot_table(index=['Позывной', 'Рейс', 'Трал'], values=['proba'], columns=['zrel', 'Пол'], 
            #                                 aggfunc='count').round(1)
            df1['zrel'].fillna('Неопред.', inplace=True)
            
            # df_zrel_null = df1[df1['zrel'].isna()]
            
            df_pivot_zrel = df1.pivot_table(index=['Позывной', 'Рейс', 'Трал'], values=['proba'], columns=['zrel', 'Пол'], 
                                            aggfunc='count').round(1)
            
            self.rename_df_columns(df_pivot_zrel, {'proba':'Стадии зрелости'})
            
            df_pivot_zrel.fillna('-', inplace=True)


            return [df_header, df_merge, df_pivot_zrel]


    # BioRazm
    def datas_for_biorazm(self, params):
        sql_t0 = "select ship, reis, trl, data from tral_view;"
        sql_t1 = """
        select rv.*, sk.nam_fish from razm_view rv
        join spr_kodfish sk on sk.kod_fish::int = rv.kodvid;
        """

        # print(params)

        try:
            res_0 = self.make_dict_request(sql_t0)
            res_1 = self.make_dict_request(sql_t1)
            t0 = pd.DataFrame(res_0)
            t1 = pd.DataFrame(res_1)
            if res_0 or res_1:
                try:
                    result = self.counting_biorazm([t0, t1], params)
                    return result
                except Exception as e:
                    print(f'Ошибка: {e}')
                    return []
            else:
                return [pd.DataFrame(), pd.DataFrame()]
                
        except Exception as e:
            print(f'Ошибка чтения из БД: {e}')
            return []
        

    def counting_biorazm(self, data, params):
        df0 = data[0]
        df1 = data[1]

        group_by, type_len, *pol_lst = params

        is_all_none = any(value is not None for value in pol_lst)

        if not group_by:
            group_by = []
        else:
            group_by = list(group_by)

        if not type_len:
            type_len = []
        else:
            type_len = list(type_len)

        print(group_by, type_len)

        if not is_all_none:
            pol_lst = []
        else:
            pol_lst = [i for i in pol_lst if i is not None]

        if pol_lst == ['by_all_pol']:
            pol_lst = ['M', 'F', 'J', 'I']

        if df0.empty and df1.empty:
            return [pd.DataFrame(), pd.DataFrame()]
        else:
            filt_dict = {
                'pol_lst': 'pol',
                'type_len': 'tip_dl'
            }
            self.rename_df_columns(df0, {'ship':'Позывной', 'reis':'Рейс', 'trl':'Трал', 'data':'Дата'})

            df_header = df0.pivot_table(index=['Позывной', 'Рейс'], values='Дата', aggfunc=['min', 'max'])
            self.rename_df_columns(df_header, {'min':'Начало рейса', 'max':'Окончание рейса'})

            def make_table(df, group_by='default', cols=[]):
                if group_by == 'default':
                    indx = ['Позывной','Рейс','Трал']
                elif group_by == 'by_fish':
                    indx = ['Позывной','Рейс','Трал', 'Рыбка']

                if df.empty:
                    return pd.DataFrame()
                else:
                    self.rename_df_columns(df, {'ship':'Позывной', 'reis':'Рейс', 'trl':'Трал',
                                        'lnf':'Длина', 'kolf':'Кол-во', 'nam_fish':'Рыбка'})
                    # нахождение min и max значений
                    min_max_df = df.pivot_table(index=indx, values='Длина', columns=cols, aggfunc=['min', 'max']).round(1)

                    # для расчета средней длины
                    df['Длин'] = df['Длина'] * df['Кол-во']
                    dlsum_df = df.pivot_table(index=indx, values='Длин', columns=cols, aggfunc=['sum']).round(1)
                    dlsum_df_part1 = dlsum_df['sum']
                    kolf_df = df.pivot_table(index=indx, values='Кол-во', columns=cols, aggfunc=['sum']).round(1)
                    kolf_df_part2 = kolf_df['sum']
                    if not cols:
                        kolf_df_part2.columns = ['Длин']
                    mean_len_df = dlsum_df_part1 / kolf_df_part2
                    mean_len_df = mean_len_df.round(0)
                    self.add_new_level(mean_len_df, ['Сред.(мм)'])

                    # итоговая таблица
                    df_merge = pd.merge(min_max_df, mean_len_df, on=indx, how='left').merge(kolf_df, on=indx, how='left')
                    self.rename_df_columns(df_merge, {'sum':'Кол-во(шт.)'})

                    return df_merge

            # 1 group by or pol or type
            if not pol_lst and not type_len and not group_by:
                result_df = make_table(df1)

            elif pol_lst and not type_len and not group_by:
                df1_pol = df1.query(f'pol in {pol_lst}')
                if df1_pol.empty:
                    df1_pol = df1.copy()
                    result_df = make_table(df1_pol)
                else:
                    result_df = make_table(df1_pol, cols=['pol'])

            # elif type_len and not pol_lst and not group_by:
            #     df1_typedl = df1[df1['tip_dl'] == int(type_len)]
            #     print(df1_typedl)
            #     if df1_typedl.empty:
            #         result_df = pd.DataFrame()
            #     else:
            #         result_df = make_table(df1_typedl)
            
            # elif pol_lst and type_len and group_by:
            #     df1_filt1 = df1[df1['tip_dl'] == int(type_len)]
            #     df1_filt2 = df1_filt1.query(f'pol in {pol_lst}')
            #     if df1_filt2.empty:
            #         result_df = pd.DataFrame()
            #     else:
            #         result_df = make_table(df1_filt2, group_by='by_fish', cols=['pol'])
            
            # elif not pol_lst and not type_len and group_by:
            #     result_df = make_table(df1, group_by='by_fish')
            
            # elif group_by and type_len and not pol_lst:
            #     df1_typedl = df1[df1['tip_dl'] == int(type_len)]
            #     if df1_typedl.empty:
            #         result_df = pd.DataFrame()
            #     else:
            #         result_df = make_table(df1_typedl, group_by='by_fish')

            # elif 


            # elif group_by and not pol_lst and not type_len:
            #     result_df = make_table(df1_pol, 'by_fish')

                # result_df = make_table(df1, 'by_fish')

            # elif pol_lst and not type_len and not group_by:
            #     df1 = df1.query(f'pol in {pol_lst}')
            #     result_df = make_table(df1, cols=['pol'])

            # elif type_len and pol_lst:
            #     df1 = df1[df1['tip_dl'] == int(type_len)]
            #     df1 = df1.query(f'pol in {pol_lst}')
            #     result_df = make_table(df1, 'by_fish', cols=['pol'])
            
            # else:
            #     result_df = make_table(df1)


            return [df_header, result_df]


    # ДРУГИЕ ЗАДАЧИ
    def datas_for_another_task1(self, params):
        pass

# ============= Формируем все таблички (типа выборки) для размерно-возр. ключа
    def datas_for_another_task9(self, params):
        sql_t1 = f"""
        select distinct ship, reis, min(data) as start_date, max(data) as end_date
        from tral_view
        group by ship, reis;
        """

        sql_t2 = f"""
        select l_big, b.wes_ob, b.vozr1, b.vozr2
        from biopap_view b
        where l_big > 100
        order by 1;
        """

        try:
            res_1 = self.make_dict_request(sql_t1)
            res_2 = self.make_dict_request(sql_t2)

            t1 = pd.DataFrame(res_1)
            t2 = pd.DataFrame(res_2)

            if res_1 or res_2:
                try:
                    result = self.counting_another_task9([t1, t2], params)
                    return result
                except Exception as e:
                    print(f'Ошибка: {e}')
                    return []

        except Exception as e:
            print(f'Ошибка чтения из БД: {e}')
            return []

# ============= Вычисления размерно-возр. ключа
    def counting_another_task9(self, data, params):
        # Собираем данные для 1 таблички (вид)
        df1 = data[0]
        self.rename_df_columns(df1, {'ship': 'Позывной', 'reis': 'Рейс', 'start_date': 'Начало рейса',
                                     'end_date': 'Конец рейса'})
        
        # Хапаем полученные запросы в таблички
        df2 = data[1]

        def to_int(val):
            return int(val) if val else None

        start_row = to_int(params[0])
        step = to_int(params[1])
        end_row = to_int(params[2])

        df2_filt = df2.copy()

        if start_row and end_row:
            df2_filt = df2[df2['l_big'].between(start_row,end_row)]
        elif start_row and not end_row:
            df2_filt = df2[df2['l_big'] >= start_row]
        elif (not start_row) and end_row:
            df2_filt = df2[df2['l_big'] <= end_row]
            start_row = min(df2['l_big'])
        elif (not start_row) and (not end_row):
            start_row = min(df2['l_big'])

        df2_filt['l_big'] /= 10

        step = step if step is not None else 10

        df2_bin = self.make_bins(df2_filt, 'l_big', start_row // 10, step)

        # количество и % рыбок
        def make_count_table(df):
            """ Делает и возвращает сводную таблицу с количеством рыбок по возрасту
            df: Принимаемый датафрейм
            """
            df_cnt = df.pivot_table(index='bucket_range', columns='vozr1', values=['l_big'], aggfunc='count', margins=True)
            self.rename_df_columns(df_cnt, {'l_big':'Кол-во(шт)'})

            df_cnt_T = df_cnt.T
            df_cnt_T['%'] = round(df_cnt_T['All'] / df_cnt_T['All'][-1], 3) * 100
            df_cnt = df_cnt_T.T

            return df_cnt
        

        df_cnt_per = make_count_table(df2_bin)
        df_cnt_per['%'] = round(df_cnt_per['Кол-во(шт)']['All'] / max(df_cnt_per['Кол-во(шт)']['All']), 5) * 100
        

        # Средняя длина и вес
        def make_mean_len_wes(df):
            df_mean = df.pivot_table(index='bucket_range', columns='vozr1', values=['l_big', 'wes_ob'], 
                                     aggfunc='mean', margins=True).round(3)
            self.rename_df_columns(df_mean, {'l_big':'Ср.длина(см)', 'wes_ob':'Ср.вес(г)'})

            return df_mean
        

        df_mean = make_mean_len_wes(df2_bin)
        df_mean.fillna('-', inplace=True)


        # Коэф. пересчета
        def make_coef(df):
            df_coef = pd.crosstab(index=df['vozr1'], columns=df['bucket_range'], normalize='columns', margins=True).round(4)
            df_coef_total = df_coef.T

            keys = list(map(str, map(int, df_coef_total.columns)))
            df_coef_total.columns = keys

            new_level = ['Коэф.пересч.']
            current_columns = list([new_level + list(i) for i in df_coef_total.columns])
            a = list([tuple(i) for i in current_columns])
            df_coef_total.columns = pd.MultiIndex.from_tuples(a)

            return df_coef_total
        

        df_coef = make_coef(df2_bin)

        # соединяем кол-во и коэфф.пересч.
        df_cnt_per = df_cnt_per.merge(df_coef, on=['bucket_range'], how='left')
        df_cnt_per.fillna('-', inplace=True)

        # Средние знначения отдельно (возраст, длина, вес)
        def make_avg_vals(df_age, df_mean):
            avg_age = np.mean(df_age['vozr1']).round(2)
            avg_len = df_mean['Ср.длина(см)']['All'][-1]
            avg_wes = df_mean['Ср.вес(г)']['All'][-1]
            return avg_age, avg_len, avg_wes
        
        avg_dfs = make_avg_vals(df2_bin, df_mean)
        avg_age = avg_dfs[0]
        avg_len = avg_dfs[1]
        avg_wes = avg_dfs[2]

        avg_vals = pd.DataFrame(index=['Средний возраст', 'Средняя длина', 'Средний вес'], data=[avg_age, avg_len, avg_wes])
        self.rename_df_columns(avg_vals, {0:'Значения'})
        

        return [df_cnt_per, df_mean, avg_vals]


    # ВОЗРАСТНАЯ ПРОБА
    # ФОРМА 1
    # ============= Формируем все таблички (типа выборки) для 1 формы
    def datas_for_aged_prob1(self, params):
        # print(params)
        sql_t1 = f"""
        SELECT a.ship,a.reis,a.trl,a.data,a.rloc,a.rzon,a.rikes,a.shir,a.dolg,b.proba, count(*) as kolr
        FROM tral_view a, biopap_view b
        WHERE a.ship = b.ship AND a.reis=b.reis AND a.trl=b.trl 
        AND b.vozr1 is not null AND (b.{params[0]} is not null AND b.{params[0]}>={params[1]}) 
        AND b.wes_ob is not null AND b.wes_ob > 0
        GROUP BY a.ship,a.reis,a.trl,a.data,a.rloc,a.rzon,a.rikes,a.shir,a.dolg,b.proba
        ORDER BY a.ship,a.reis,a.trl,a.data,a.rloc,a.rzon,a.rikes,a.shir,a.dolg;
        """

        sql_t2 = f"""
        SELECT b.vozr1, b.{params[0]}, b.pol, b.wes_ob
        FROM tral_view a, biopap_view b
        WHERE a.ship=b.ship AND a.reis=b.reis AND a.trl=b.trl
        AND b.vozr1 is not null AND (b.{params[0]} is not null AND b.{params[0]}>={params[1]}) 
        AND b.wes_ob is not null AND b.wes_ob > 0;
        """

        try:
            res_1 = self.make_dict_request(sql_t1)
            res_2 = self.make_dict_request(sql_t2)

            t1 = pd.DataFrame(res_1)
            t2 = pd.DataFrame(res_2)

            if res_1 or res_2:
                try:
                    result = self.counting_aged_prob1([t1, t2], params)
                    return result
                except Exception as e:
                    print(f'Ошибка: {e}')
                    return []

        except Exception as e:
            print(f'Ошибка чтения из БД: {e}')
            return []

    # ============= Вычисления формы 1
    def counting_aged_prob1(self, data, params):

        df1 = data[0]
        # Собираем данные для 1 таблички (вид)
        # Переименуем колонки
        self.rename_df_columns(df1, {'ship': 'Позывной', 'reis': 'Рейс', 'trl': 'Трал',
                                     'data': 'Дата', 'rloc': 'Лок.район', 'rzon': '408 пр.',
                                     'rikes': '520 пр.', 'shir': 'Широта', 'dolg': 'Долгота',
                                     'proba': 'Проба', 'kolr': 'Кол-во (шт.)'})
        
        df1_piv = df1.pivot_table(
            index=['Позывной', 'Рейс', 'Дата', 'Трал', 'Широта', 'Долгота', 'Проба', 'Лок.район', '408 пр.', '520 пр.'],
            values=['Кол-во (шт.)'], aggfunc='sum', margins=True)
        
        t1 = df1_piv.rename(index={'All': 'Итого:'})
        table1 = t1.reset_index()
        # table1 = df1

        # Хапаем полученные запросы в таблички
        df2 = data[1]

        len_param = params[0]
        len_value = float(params[1])
        step_value = float(params[2])

        #     Собираем данные для 2 таблички (сводник по возрасту и размеру)
        # Возьмем столбец с длиной и переведем в сантиментры (щас оно в мм.)
        df2[len_param] /= 10
        df2[len_param] = df2[len_param].apply(lambda x: float(x))
        df2['wes_ob'] = df2['wes_ob'].apply(lambda x: float(x))

        #     Делим длину на бины
        df_t2 = self.make_bins(df2, len_param, len_value / 10, step_value)

        #     Переименуем колонки
        self.rename_df_columns(df_t2, {'vozr1': 'Возраст', len_param: 'Длина', 'pol': 'Пол', 'wes_ob': 'Вес',
                                       'bucket_range': 'Размер', })

        def make_tables(table_num, df, indexes):
            #     Делаем сводник по возрасту и размеру
            #     Сначала разберемся с количеством
            t_pivot_count = df.pivot_table(index=indexes, columns=['Пол'], values='Вес',
                                               aggfunc=['count'], margins=True)
            t_pivot_count.replace(0, np.nan, inplace=True)
            t_pivot_count.dropna(how='all', inplace=True)
            t_pivot_count.fillna('-', inplace=True)

            #     Переименуем колонки
            self.rename_df_columns(t_pivot_count, {'count': ''})

            #     Удаляем лишнее
            t_pivot_count.columns = t_pivot_count.columns.rename(None, level='Пол')

            #     Добавляем нужное
            new_level_c = ('Кол-во(шт.)', '')
            current_columns = t_pivot_count.columns
            t_pivot_count.columns = pd.MultiIndex.from_tuples([new_level_c + tuple_ for tuple_ in current_columns])

            #     Делаем сводник для веса

            t_pivot_weight = df.pivot_table(index=indexes, columns=['Пол'], values='Вес',
                                            aggfunc=['sum', 'mean', self.standard_error, self.coef_variation], margins=True).round(1)
            t_pivot_weight.replace(0, np.nan, inplace=True)
            t_pivot_weight.dropna(how='all', inplace=True)
            t_pivot_weight.fillna('-', inplace=True)

            #     Удаляем лишний индекс "Пол"
            t_pivot_weight.columns = t_pivot_weight.columns.rename(None, level='Пол')

            #     Переименовываем колонки
            self.rename_df_columns(t_pivot_weight,
                              {'sum': 'Общий(г.)', 'mean': 'Средний(г.)', 'standard_error': 'Ошибка средн.',
                               'coef_variation': 'Коэфф.вариации'})

            #     Объединяем колонки под знамением "Вес"
            new_level_w = ('Вес', '')
            current_columns = t_pivot_weight.columns
            t_pivot_weight.columns = pd.MultiIndex.from_tuples([new_level_w + tuple_ for tuple_ in current_columns])

            #     Теперь считаем функции для длины
            #     Делаем сводник для длины
            t_pivot_len = df.pivot_table(index=indexes, columns=['Пол'], values='Длина',
                                          aggfunc=['mean', self.standard_error, self.coef_variation],
                                          margins=True).round(3)
            t_pivot_len.replace(0, np.nan, inplace=True)
            t_pivot_len.dropna(how='all', inplace=True)
            t_pivot_len.fillna('-', inplace=True)

            #     Удаляем лишний индекс "Пол"
            t_pivot_len.columns = t_pivot_len.columns.rename(None, level='Пол')
            self.rename_df_columns(t_pivot_len, {'mean': 'Средний(г.)', 'standard_error': 'Ошибка средн.',
                                             'coef_variation': 'Коэфф.вариации'})

            #     Объединяем колонки под знамением "Длина"
            new_level_l = ('Длина', '')
            current_columns = t_pivot_len.columns
            t_pivot_len.columns = pd.MultiIndex.from_tuples([new_level_l + tuple_ for tuple_ in current_columns])

            #     Объединяем все это дело
            temp_t1 = pd.merge(t_pivot_count, t_pivot_weight, on=indexes, how='left')

            if table_num == 2:
                result_t = pd.merge(temp_t1, t_pivot_len, on=indexes, how='left').reset_index()

            else:
                if table_num == 3:
                    per_param1 = 'Возраст'
                else:
                    per_param1 = 'Размер'

                # считаем % по возрасту и весу
                # сначала по возрасту\размеру
                t_per = pd.crosstab(index=df[per_param1], columns=df['Пол'], normalize='columns',
                                     margins=True).round(3) * 100
                t_per.columns.name = ''
                # Объединяем колонки под знамением "% по возрасту\размеру"
                current_index = t_per.columns

                # Создаем новый мультииндекс с кортежами из цикла
                t_per.columns = pd.MultiIndex.from_tuples([('', label) for label in current_index])

                new_level_w = (per_param1, '')
                current_columns = t_per.columns
                t_per.columns = pd.MultiIndex.from_tuples([new_level_w + tuple_ for tuple_ in current_columns])

                # теперь по весу
                max_weight = t_pivot_weight['Вес']['']['Общий(г.)']['All'].max()
                t_per['Весу'] = ((t_pivot_weight['Вес']['']['Общий(г.)']['All'] / max_weight) * 100).round(2)

                # Немного манипуляций с индексами
                current_index = t_per.columns
                new_tuples = list(current_index)
                new_tuples[-1] = list(new_tuples[-1])
                new_tuples[-1][-1] = 'All'
                new_tuples[-1] = tuple(new_tuples[-1])
                t_per.columns = pd.MultiIndex.from_tuples([tuple(filter(None, tup)) for tup in new_tuples])

                # Объединям все
                # % по возрасту\размеру + весу
                new_level_w = ('Процент(%) по', '')
                current_columns = t_per.columns
                t_per.columns = pd.MultiIndex.from_tuples([new_level_w + tuple_ for tuple_ in current_columns])

                # вес + длина
                t_wl = pd.merge(t_pivot_weight, t_pivot_len, on=indexes, how='left')

                # кол - во + вес\длина
                t_cwl = pd.merge(t_pivot_count, t_wl, on=indexes, how='left')

                # union all
                result_t = pd.merge(t_cwl, t_per, on=indexes, how='left')
                result_t.replace(0, np.nan, inplace=True)
                result_t.dropna(how='all', inplace=True)
                result_t.fillna('-', inplace=True)

                result_t = result_t.reset_index()

            return result_t

        table2 = make_tables(2, df_t2, ['Возраст', 'Размер'])
        table3 = make_tables(3, df_t2, ['Возраст'])
        table4 = make_tables(4, df_t2, ['Размер'])

        return [table1, table2, table3, table4]

    # ============= // Вычисления формы 1
    # // ФОРМА 1


    # ФОРМА 2
    # ============= Формируем все таблички (типа выборки) для 2 формы
    def datas_for_aged_prob2(self):
        sql_t1 = """
        SELECT a.ship,a.reis,a.trl,a.data,a.rloc,a.rzon,a.rikes,a.shir,a.dolg,b.proba, count(*) AS kolr
        FROM tral_view a, biopap_view b
        WHERE a.ship = b.ship AND a.reis=b.reis AND a.trl=b.trl AND b.vozr1 IS NOT NULL
        GROUP BY a.ship,a.reis,a.trl,a.data,a.rloc,a.rzon,a.rikes,a.shir,a.dolg,b.proba
        ORDER BY a.ship,a.reis,a.trl,a.data,a.rloc,a.rzon,a.rikes,a.shir,a.dolg,b.proba;
        """

        sql_t2 = """
        SELECT b.vozr1, a.data, count(*)
        FROM tral_view a
        JOIN biopap_view b ON a.ship = b.ship AND a.reis = b.reis AND a.trl = b.trl
        WHERE b.vozr1 IS NOT null
        GROUP BY b.vozr1, a.data
        ORDER BY b.vozr1, a.data;
        """

        sql_t3 = """
        WITH t1 AS (
        SELECT EXTRACT(MONTH FROM a.data)::INT AS month, b.proba
        FROM tral_view a
        JOIN biopap_view b ON a.ship = b.ship AND a.reis = b.reis AND a.trl = b.trl
        WHERE b.vozr1 IS NOT NULL
        ),
        t2 AS (
            SELECT month, proba, COUNT(*) AS proba_count
            FROM t1
            GROUP BY month, proba
        )
        SELECT month, COUNT(proba) AS proba_count
        FROM t2
        GROUP BY month;
        """

        try:
            res_1 = self.make_dict_request(sql_t1)
            res_2 = self.make_dict_request(sql_t2)
            res_3 = self.make_dict_request(sql_t3)

            t1 = pd.DataFrame(res_1)
            t2 = pd.DataFrame(res_2)
            t3 = pd.DataFrame(res_3)

            if res_1 or res_2 or res_3:
                try:
                    result = self.counting_aged_prob2([t1, t2, t3])
                    return result
                except Exception as e:
                    print(f'Ошибка: {e}')
                    return []

        except Exception as e:
            print(f'Ошибка чтения из БД: {e}')
            return []

    # ============= // Формируем первые таблички

    # ============= Вычисления формы 2
    def counting_aged_prob2(self, data):
        # Хапаем полученные запросы в таблички
        df = data[0]
        df_t1 = data[1]
        df_t2 = data[2]

        # Переименуем колонки
        self.rename_df_columns(df, {'ship': 'Позывной', 'reis': 'Рейс', 'trl': 'Трал',
                                    'data': 'Дата', 'rloc': 'Лок.район', 'rzon': '408 пр.',
                                    'rikes': '520 пр.', 'shir': 'Широта', 'dolg': 'Долгота',
                                    'proba': 'Проба', 'kolr': 'Кол-во (шт.)'})

        # Выделим даты - месяца
        df_t1['data'] = pd.to_datetime(df_t1['data'])
        df_t1['month'] = df_t1['data'].dt.month

        # Переименуем колонки
        self.rename_df_columns(df_t1, {'vozr1': 'Возраст', 'data': 'Дата', 'count': 'Количество', 'month': 'Месяц'})

        # Сводник по возрастам и месяцам
        t1_pivot = df_t1.pivot_table(index=['Месяц'], columns=['Возраст'], values=['Количество'], aggfunc=['sum'],
                                     margins=True)

        # Раздаем метки месяцам
        month_mapping = {1: 'Январь', 2: 'Февраль', 3: 'Март', 4: 'Апрель', 5: 'Май', 6: 'Июнь', 7: 'Июль', 8: 'Август',
                         9: 'Сентябрь', 10: 'Октябрь', 11: 'Ноябрь', 12: 'Декабрь', 'All': '% по возрасту'}
        t1_pivot = t1_pivot.rename(index=month_mapping)

        # Дропнуть мультииндексы
        self.drop_multiindex_columns(t1_pivot)

        # Переименуем колонки, которые имеют стремные названия после дропанья мутииндексов
        month_dict = self.create_dict(t1_pivot)
        self.rename_df_columns(t1_pivot, month_dict)

        # Задаем колонки, которые будем делить
        columns_to_divide = t1_pivot.columns[:-1]

        # Делим и ставим "-", если в ячейке отсутствует значение
        t1_pivot[columns_to_divide] = t1_pivot[columns_to_divide].div(t1_pivot['All'], axis=0)
        t1_pivot[columns_to_divide] *= 100
        t1_pivot = t1_pivot.astype(float).round(2)
        t1_pivot.fillna('-', inplace=True)

        # Тут еще колонка с пробами.
        # Переименуем колонки
        self.rename_df_columns(df_t2, {'month': 'Месяц', 'proba_count': 'Кол-во проб(шт.)'})

        t2_pivot = df_t2.pivot_table(index=['Месяц'], aggfunc='sum', margins=True)
        month_mapping = {1: 'Январь', 2: 'Февраль', 3: 'Март', 4: 'Апрель', 5: 'Май', 6: 'Июнь', 7: 'Июль', 8: 'Август',
                         9: 'Сентябрь', 10: 'Октябрь', 11: 'Ноябрь', 12: 'Декабрь', 'All': '% по возрасту'}
        t2_pivot = t2_pivot.rename(index=month_mapping)

        # Объединение двух табличек: первая с вычислениями + с пробами
        df_total = pd.merge(t1_pivot, t2_pivot, on=['Месяц'], how='left')

        # Немного транспонирования (чтобы таблица выглядела красивее)
        a = df_total.T
        a.columns.name = 'Возраст'

        # Подкорректируем некоторые названия строк
        month_mapping = {'All': 'Итого вида(шт.)'}
        result = a.rename(index=month_mapping)

        return [df, result]

    # ============= // Вычисления формы 2
    # // ФОРМА 2


    # ФОРМА 3
    # ============= Формируем все таблички (типа выборки) для 3 формы
    def datas_for_aged_prob3(self):
        sql_t1 = """
        SELECT a.ship,a.reis,a.trl,a.data,a.rloc,a.rzon,a.rikes,a.shir,a.dolg,b.proba, count(*) as kolr
        from tral_view a, biopap_view b
        WHERE a.ship = b.ship and a.reis=b.reis and a.trl=b.trl 
        and (b.wes_ob != 0 and b.w_pech != 0 and b.vozr1 is not null)
        GROUP BY a.ship,a.reis,a.trl,a.data,a.rloc,a.rzon,a.rikes,a.shir,a.dolg,b.proba
        ORDER BY a.ship,a.reis,a.trl,a.data,a.rloc,a.rzon,a.rikes,a.shir,a.dolg;
        """

        sql_t2 = """
        SELECT a.ship, a.reis, a.trl, a.data, a.rloc, b.pol, sl.lraj_n,
        b.wes_ob, b.w_pech, b.wes_bv
        FROM tral_view a, biopap_view b, spr_loc sl
        WHERE a.ship=b.ship and a.reis=b.reis and a.trl=b.trl and to_number(sl.lraj_k, '999999') = a.rloc
        and b.wes_ob != 0 and b.w_pech != 0 and b.vozr1 is not null;
        """

        try:
            res_1 = self.make_dict_request(sql_t1)
            res_2 = self.make_dict_request(sql_t2)

            t1 = pd.DataFrame(res_1)
            t2 = pd.DataFrame(res_2)

            if res_1 or res_2:
                try:
                    result = self.counting_aged_prob3([t1, t2])
                    return result
                except Exception as e:
                    print(f'Ошибка: {e}')
                    return []

        except Exception as e:
            print(f'Ошибка чтения из БД: {e}')
            return []

    # ============= // Формируем первые таблички


    # ============= Вычисления формы 3
    def counting_aged_prob3(self, data):
        df_t1 = data[0]
        df_t2 = data[1]

        # Переименуем колонки
        self.rename_df_columns(df_t1, {'ship': 'Позывной', 'reis': 'Рейс', 'trl': 'Трал',
                                    'data': 'Дата', 'rloc': 'Лок.район', 'rzon': '408 пр.',
                                    'rikes': '520 пр.', 'shir': 'Широта', 'dolg': 'Долгота',
                                    'proba': 'Проба', 'kolr': 'Кол-во (шт.)'})

        self.rename_df_columns(df_t2, {'ship': 'Позывной', 'reis': 'Рейс', 'trl': 'Трал',
                                'data': 'Дата', 'rloc': 'Лок.район', 'pol': 'Пол',
                                'lraj_n': 'Название района', 'wes_ob': 'Вес особи', 'w_pech': 'Вес печени',
                                'wes_bv': 'Вес без внутр.', 'kolr': 'Кол-во (шт.)'})
        # Выделим даты - месяца
        df_t2['Дата'] = pd.to_datetime(df_t2['Дата'])
        df_t2['Месяц'] = df_t2['Дата'].dt.month

        df_t2['Вес особи'] = df_t2['Вес особи'].astype(float)
        df_t2['Вес печени'] = df_t2['Вес печени'].astype(float)
        df_t2['Вес без внутр.'] = df_t2['Вес без внутр.'].astype(float)

        # Сводник по месяцу и району, колонки = пол. КОЛИЧЕСТВО
        df2_pivot = df_t2.pivot_table(index=['Месяц', 'Название района'], columns=['Пол'], values='Вес особи',
                                    aggfunc=['count'], margins=True, sort=False).round(1)
        df2_pivot.columns = df2_pivot.columns.rename(None, level='Пол')

        # Переименовываем колонки
        self.rename_df_columns(df2_pivot, {'count': ''})
        new_level_w = ['Кол-во(шт.)']
        current_columns = list([new_level_w + list(i) for i in df2_pivot.columns])
        new_colums = list([tuple(i) for i in current_columns])
        df2_pivot.columns = pd.MultiIndex.from_tuples(new_colums)

        # Сводник по месяцу и району. ВЕС ОСОБИ + ВЕС ПЕЧЕНИ
        df2_pivot_w = df_t2.pivot_table(index=['Месяц', 'Название района'], columns=['Пол'],
                                      values=['Вес особи', 'Вес печени'], aggfunc=['mean'], margins=True, sort=False).round(1)
        df2_pivot_w.columns = df2_pivot_w.columns.rename(None, level='Пол')
        self.rename_df_columns(df2_pivot_w, {'mean': 'Средний'})

        # Сводник по месяцу и району. БЕЗ ВНУТРЕННОСТЕЙ
        df2_pivot_bw = df_t2.pivot_table(index=['Месяц', 'Название района'], columns=['Пол'], values=['Вес без внутр.'],
                                       aggfunc=['mean'], margins=True, sort=False).round(1)
        df2_pivot_bw.columns = df2_pivot_bw.columns.rename(None, level='Пол')
        self.rename_df_columns(df2_pivot_bw, {'mean': 'Средний'})

        # СРЕДНЯЯ ЖИРНОСТЬ
        df2_pivot_f = df_t2.pivot_table(index=['Месяц', 'Название района'], columns=['Пол'],
                                      values=['Вес особи', 'Вес печени'], aggfunc=['sum'], margins=True, sort=False).round(1)
        df2_pivot_f['sum', 'Вес печени'] /= df2_pivot_f['sum', 'Вес особи']
        df2_pivot_f['sum', 'Вес печени'] *= 100
        df2_pivot_f.columns = df2_pivot_f.columns.rename(None, level='Пол')

        # Удаляем столбец 'Вес особи'
        df2_pivot_f.columns = df2_pivot_f.columns.droplevel(0)
        # Переименовываем оставшиеся уровни мультииндекса
        df2_pivot_f_new = df2_pivot_f.loc[:, ['Вес печени']]
        self.rename_df_columns(df2_pivot_f_new, {'Вес печени': ''})

        new_level_fat = ['Средняя жирность(%)']
        current_columns = list([new_level_fat + list(i) for i in df2_pivot_f_new.columns])
        new_cols_fat = list([tuple(i) for i in current_columns])
        df2_pivot_f_new.columns = pd.MultiIndex.from_tuples(new_cols_fat)
        df2_pivot_f_new = df2_pivot_f_new.round(1)

        # Соединяем все
        temp_t1 = pd.merge(df2_pivot, df2_pivot_w, on=['Месяц', 'Название района'], how='left')
        temp_t2 = pd.merge(temp_t1, df2_pivot_bw, on=['Месяц', 'Название района'], how='left')
        t_result = pd.merge(temp_t2, df2_pivot_f_new, on=['Месяц', 'Название района'], how='left')
        dict_mapping = {'All': 'Итого по виду'}
        t_result = t_result.rename(index=dict_mapping)

        return [df_t1, t_result.reset_index()]

    # ============= // Вычисления формы 3
    # // ФОРМА 3


    # ФОРМА 4
    # ============= Формируем все таблички (типа выборки) для 4 формы
    def datas_for_aged_prob4(self,params):
        sql_t1 = f"""
        SELECT a.ship,a.reis,a.trl,a.data,a.rloc,a.rzon,a.rikes,a.shir,a.dolg,b.proba, count(*) as kolr
        FROM tral_view a, biopap_view b
        WHERE a.ship = b.ship AND a.reis=b.reis AND a.trl=b.trl 
        AND b.vozr1 is not null AND (b.{params[0]} is not null AND b.{params[0]}>={params[1]}) 
        AND b.wes_ob is not null AND b.wes_ob > 0
        GROUP BY a.ship,a.reis,a.trl,a.data,a.rloc,a.rzon,a.rikes,a.shir,a.dolg,b.proba
        ORDER BY a.ship,a.reis,a.trl,a.data,a.rloc,a.rzon,a.rikes,a.shir,a.dolg;
        """

        sql_t2 = f"""
        SELECT b.vozr1, b.pol, b.wes_ob, b.w_pech, b.wes_bv, b.l_big, b.l_lit
        FROM tral_view a, biopap_view b
        WHERE a.ship=b.ship AND a.reis=b.reis AND a.trl=b.trl
        AND b.vozr1 is not null AND (b.{params[0]} is not null AND b.{params[0]}>={params[1]}) 
        AND b.wes_ob is not null AND b.wes_ob > 0;
        """

        try:
            res_1 = self.make_dict_request(sql_t1)
            res_2 = self.make_dict_request(sql_t2)

            t1 = pd.DataFrame(res_1)
            t2 = pd.DataFrame(res_2)

            if res_1 or res_2:
                try:
                    result = self.counting_aged_prob4([t1, t2], params)
                    return result
                except Exception as e:
                    print(f'Ошибка: {e}')
                    return []

        except Exception as e:
            print(f'Ошибка чтения из БД: {e}')
            return []

    # ============= Вычисления формы 4
    def counting_aged_prob4(self, data, params):
        df1 = data[0]
        # Собираем данные для 1 таблички (вид)
        # Переименуем колонки
        self.rename_df_columns(df1, {'ship': 'Позывной', 'reis': 'Рейс', 'trl': 'Трал',
                                     'data': 'Дата', 'rloc': 'Лок.район', 'rzon': '408 пр.',
                                     'rikes': '520 пр.', 'shir': 'Широта', 'dolg': 'Долгота',
                                     'proba': 'Проба', 'kolr': 'Кол-во (шт.)'})
        df1_piv = df1.pivot_table(
            index=['Позывной', 'Рейс', 'Дата', 'Трал', 'Широта', 'Долгота', 'Проба', 'Лок.район', '408 пр.', '520 пр.'],
            values=['Кол-во (шт.)'], aggfunc='sum', margins=True)
        t1 = df1_piv.rename(index={'All': 'Итого:'})
        table1 = t1.reset_index()

        # Хапаем полученные запросы в таблички
        df2 = data[1]

        len_param = params[0]
        len_value = float(params[1])
        step_value = float(params[2])

        #     Собираем данные для 2 таблички (сводник по возрасту и размеру)
        # Возьмем столбец с длиной и переведем в сантиментры (щас оно в мм.)
        df2['l_big'] /= 10
        df2['l_lit'] /= 10

        df2['l_big'] = df2['l_big'].apply(lambda x: float(x))
        df2['l_lit'] = df2['l_lit'].apply(lambda x: float(x))
        df2['wes_ob'] = df2['wes_ob'].apply(lambda x: float(x))
        df2['wes_bv'] = df2['wes_bv'].apply(lambda x: float(x))
        df2['w_pech'] = df2['w_pech'].apply(lambda x: float(x))

        #     Делим длину на бины
        df_t2 = self.make_bins(df2, len_param, len_value / 10, step_value)
        # self.rename_df_columns(df_t2, {'bucket_range': 'Размер'})

        self.rename_df_columns(df_t2, {'vozr1': 'Возраст', 'pol': 'Пол', 'wes_ob': 'Вес особи', 'w_pech': 'Вес печени',
                                'wes_bv': 'Вес без внутр.', 'l_big': 'Б.длина', 'l_lit': 'М.длина', 'fat': 'Жирность',
                                'fulton': 'Фультон', 'klark': 'Кларк', 'bucket_range': 'Размер'})

        def make_tables(table_num, df_t2, indexes, len_param):

            if len_param == 'l_big':
                len_param = 'Б.длина'
            else:
                len_param = 'М.длина'

            def make_pivots(df_t2, table_name, indexes, vals, funcs):
                # Делаем сводник по указанным индексам
                t_piv = df_t2.pivot_table(index=indexes, columns=['Пол'], values=vals, aggfunc=funcs,
                                          margins=True).round(2)

                # Переименовыввем колонки
                if vals == 'fulton':
                    self.rename_df_columns(t_piv, {'mean': 'Фультон', 'standard_error': 'Ошибка средн.(Ф)',
                                              'coef_variation': 'Коэфф.вариации(Ф)'})
                elif vals == 'klark':
                    self.rename_df_columns(t_piv, {'mean': 'Клрак', 'standard_error': 'Ошибка средн.(K)',
                                              'coef_variation': 'Коэфф.вариации(K)'})
                else:
                    self.rename_df_columns(t_piv, {'count': '', 'sum': 'Общий(г.)', 'mean': 'Средний',
                                              'standard_error': 'Ошибка средн.', 'coef_variation': 'Коэфф.вариации'})

                # Удаляем лишнее
                t_piv.columns = t_piv.columns.rename(None, level='Пол')

                if table_name == 'Средний' or table_name == 'Упитанность':
                    return t_piv
                else:
                    # Объединяем колонки под общим знамением
                    new_level = [table_name]
                    current_columns = list([new_level + list(i) for i in t_piv.columns])
                    new_cols = list([tuple(i) for i in current_columns])
                    t_piv.columns = pd.MultiIndex.from_tuples(new_cols)

                    return t_piv

            # количество
            t2_pivot_count = make_pivots(df_t2, 'Кол-во(шт.)', indexes, 'Вес особи', ['count'])
            # вес
            t2_pivot_weight = make_pivots(df_t2, 'Вес', indexes, 'Вес особи',
                                          ['sum', 'mean', self.standard_error, self.coef_variation])
            # жирность
            df_fat = df_t2.copy()
            filt_wes_pech = df_fat['Вес печени'] > 0
            df_fat = df_fat[filt_wes_pech]
            df_fat.loc[:, 'fat'] = (df_fat['Вес печени'] / df_fat['Вес особи']) * 100
            t2_pivot_fat = make_pivots(df_fat, 'Жирность', indexes, 'fat', ['mean', self.standard_error, self.coef_variation])
            # средний вес
            df_mean = df_t2.copy()
            filt1 = df_mean['Вес печени'] > 0
            filt2 = df_mean['Вес без внутр.'] > 0
            filt = filt1 | filt2
            df_mean = df_mean[filt]
            t2_pivot_mean = make_pivots(df_mean, 'Средний', indexes, ['Вес печени', 'Вес без внутр.'], ['mean'])
            # сводник с фультонами И кларками (полагаю тут нужно делать относительно длины, которая была выбрана в параметрах)
            df_fult_klark = df_t2.copy()
            filt2_k = df_fult_klark['Вес без внутр.'] > 0
            df_klark = df_fult_klark[filt2_k]

            df_fult_klark['fulton'] = ((df_fult_klark['Вес особи'] / (df_fult_klark[len_param]) ** 3) * 100).round(2)
            df_klark['klark'] = ((df_klark['Вес без внутр.'] / (df_klark[len_param]) ** 3) * 100).round(2)

            t2_pivot_fult = make_pivots(df_fult_klark, 'Упитанность', indexes, 'fulton',
                                        ['mean', self.standard_error, self.coef_variation])
            t2_pivot_klark = make_pivots(df_klark, 'Упитанность', indexes, 'klark',
                                         ['mean', self.standard_error, self.coef_variation])

            if df_klark.empty:
                t2_piv_upit = t2_pivot_fult
            else:
                t2_piv_upit = pd.merge(t2_pivot_fult, t2_pivot_klark, on=indexes, how='left')

            new_level_upit = ['Упитанность']
            current_columns = list([new_level_upit + list(i) for i in t2_piv_upit.columns])
            new_cols_upit = list([tuple(i) for i in current_columns])
            t2_piv_upit.columns = pd.MultiIndex.from_tuples(new_cols_upit)

            if table_num == 2:
                t2_piv_upit = t2_piv_upit
            else:
                t_per = df_t2.pivot_table(index=indexes, columns=['Пол'], values='Вес особи', aggfunc="sum", margins=True)

                t2_piv_per_wes = t_per['All']
                max_weight = t2_piv_per_wes.max()
                t2_piv_per_wes = ((t2_piv_per_wes / max_weight)*100).round(2)
                t2_piv_per_wes = t2_piv_per_wes.to_frame().round(1)

                self.rename_df_columns(t2_piv_per_wes, {'All': ''})

                new_level = ['Процент(%) по весу', '', '']
                current_columns = list([new_level + list(i) for i in t2_piv_per_wes.columns])
                new_cols = list([tuple(i) for i in current_columns])
                t2_piv_per_wes.columns = pd.MultiIndex.from_tuples(new_cols)

                t2_piv_upit = pd.merge(t2_piv_upit, t2_piv_per_wes, on=indexes, how='left')

            # Объединение
            # Кол-во + вес
            temp_t1 = pd.merge(t2_pivot_count, t2_pivot_weight, on=indexes, how='left')
            # temp1 + жирность
            temp_t2 = pd.merge(temp_t1, t2_pivot_fat, on=indexes, how='left')
            # temp2 + ср.вес
            temp_t3 = pd.merge(temp_t2, t2_pivot_mean, on=indexes, how='left')
            # result
            result_table = pd.merge(temp_t3, t2_piv_upit, on=indexes, how='left')

            result_table.replace(0, np.nan, inplace=True)
            result_table.dropna(how='all', inplace=True)
            result_table.fillna('-', inplace=True)

            dict_mapping = {'All': 'Итого'}
            result_table = result_table.rename(index=dict_mapping)

            return result_table.reset_index()

        table2 = make_tables(2, df_t2, ['Возраст', 'Размер'], len_param)
        table3 = make_tables(3, df_t2, ['Возраст'], len_param)
        table4 = make_tables(4, df_t2, ['Размер'], len_param)

        return [table1, table2, table3, table4]

    # ФОРМА 11
    # ============= Формируем все таблички (типа выборки) для 11 формы
    def datas_for_aged_prob11(self, params):
        # print(params)
        sql_t1 = f"""
        SELECT a.ship,a.reis,a.trl,a.data,a.rloc,a.rzon,a.rikes,a.shir,a.dolg,b.proba, count(*) as kolr
        FROM tral_view a, biopap_view b
        WHERE a.ship = b.ship and a.reis=b.reis and a.trl=b.trl 
        AND (b.vozr1 is not null and b.{params[0]} is not null AND b.{params[0]}>={params[1]})
        GROUP BY a.ship,a.reis,a.trl,a.data,a.rloc,a.rzon,a.rikes,a.shir,a.dolg,b.proba
        ORDER BY a.ship,a.reis,a.trl,a.data,a.rloc,a.rzon,a.rikes,a.shir,a.dolg;
        """

        sql_t2 = f"""
        SELECT b.l_big, b.vozr1, b.pol, b.wes_ob, b.w_pech
        FROM biopap_view b
        WHERE b.vozr1 is not null and (b.{params[0]} is not null AND b.{params[0]}>={params[1]});
        """

        try:
            res_1 = self.make_dict_request(sql_t1)
            res_2 = self.make_dict_request(sql_t2)

            t1 = pd.DataFrame(res_1)
            t2 = pd.DataFrame(res_2)

            if res_1 or res_2:
                try:
                    result = self.counting_aged_prob11([t1, t2], params)
                    return result
                except Exception as e:
                    print(f'Ошибка: {e}')
                    return []

        except Exception as e:
            print(f'Ошибка чтения из БД: {e}')
            return []

    # ============= Вычисления формы 11
    def counting_aged_prob11(self, data, params):
        # Первая табличка
        df1 = data[0]
        # Собираем данные для 1 таблички (вид)
        # Переименуем колонки
        self.rename_df_columns(df1, {'ship': 'Позывной', 'reis': 'Рейс', 'trl': 'Трал',
                                     'data': 'Дата', 'rloc': 'Лок.район', 'rzon': '408 пр.',
                                     'rikes': '520 пр.', 'shir': 'Широта', 'dolg': 'Долгота',
                                     'proba': 'Проба', 'kolr': 'Кол-во (шт.)'})
        table1 = df1

        # Вторая табличка
        df2 = data[1]
        len_param = params[0]
        len_value = float(params[1])
        step_value = float(params[2])

        #     Собираем данные для 2 таблички (сводник по возрасту и размеру)
        # Возьмем столбец с длиной и переведем в сантиментры (щас оно в мм.)
        df2[len_param] /= 10
        df2[len_param] = df2[len_param].apply(lambda x: float(x))
        df2['wes_ob'] = df2['wes_ob'].apply(lambda x: float(x))
        df2['w_pech'] = df2['w_pech'].apply(lambda x: float(x))

        #     Делим длину на бины
        df_t2 = self.make_bins(df2, len_param, len_value / 10, step_value)
        #     Переименуем колонки
        self.rename_df_columns(df_t2, {'vozr1': 'Возраст', 'l_lit': 'М.длина', 'l_big': 'Б.длина',
                                       'pol': 'Пол', 'wes_ob': 'Вес особи', 'bucket_range': 'Размер', 'w_pech': 'Вес печени'})
        
        # print(df_t2)

        def make_table(table_num, df_t2, value):
            count_pivot = df_t2.groupby(['Размер']).apply(lambda sub_df:
                                                          sub_df.pivot_table(index=['Пол'], values='Вес особи',
                                                                             columns='Возраст', aggfunc='count',
                                                                             margins=True, margins_name='Итого')).round(1)
            count_cols = count_pivot.columns
            new_list = []
            for i in count_cols:
                if i != 'Итого':
                    new_list.append(i)
            new_list.sort()
            new_list.append('Итого')
            count_pivot = count_pivot[new_list]

            if table_num == 2:
                # тут я делаю агрегирование для нахождения % по весу
                self.rename_df_columns(count_pivot, {'Итого': 'Кол-во(шт.)'})
                df_per_pol = df_t2.pivot_table(index=['Пол'], values='Вес особи', columns='Возраст', aggfunc='count',
                                               margins=True, margins_name='Итого')
                max_count = df_per_pol.T['Итого'].max()
                count_pivot['% по полу'] = ((count_pivot['Кол-во(шт.)'] / max_count) * 100).round(1)
                subtotal_table = count_pivot
            else:
                # средний вес\жирность
                mean_pivot = df_t2.groupby(['Размер']).apply(lambda sub_df:
                                                             sub_df.pivot_table(index=['Пол'], values=value,
                                                                                columns='Возраст', aggfunc='mean',
                                                                                margins=True,
                                                                                margins_name='Итого')).round(1)
                count_cols = mean_pivot.columns

                new_list = []
                for i in count_cols:
                    if i != 'Итого':
                        new_list.append(i)
                new_list.sort()
                new_list.append('Итого')
                mean_pivot = mean_pivot[new_list]

                mean_pivot['Итого'] = count_pivot['Итого']

                self.rename_df_columns(mean_pivot, {'Итого': 'Кол-во(шт.)'})
                subtotal_table = mean_pivot

            wes_pivot = df_t2.groupby(['Размер']).apply(lambda sub_df:
                                                        sub_df.pivot_table(index=['Пол'], values=value,
                                                                           columns='Возраст', aggfunc='mean',
                                                                           margins=True, margins_name='Итого')).round(1)
            avg_wes_pivot = wes_pivot['Итого']
            result_table = pd.merge(subtotal_table, avg_wes_pivot, on=['Размер', 'Пол'], how='left')

            result_table.replace(0, np.nan, inplace=True)
            result_table.dropna(how='all', inplace=True)
            result_table.fillna('-', inplace=True)

            mcol = pd.MultiIndex.from_tuples([('Возраст', i) for i in result_table.columns], names=['', ''])
            result_table.columns = mcol

            return result_table.reset_index()


        def make_totals(num_table, df_t2, value):
            rename_str = ''
            if value == 'Вес особи':
                rename_str = 'Ср.вес'
            elif value == 'Б.длина' or value == 'М.длина':
                rename_str = 'Ср.длина'
            if value == 'Жирность':
                rename_str = 'Ср.жирн'

            if df_t2.empty:
                return pd.DataFrame()
            else:
                # численность
                df_number = df_t2.pivot_table(index=['Пол'], columns='Возраст', values='Вес особи', aggfunc='count', margins=True)
                if num_table == 1:
                    # % по полу
                    df_number['% по полу'] = ((df_number['All'] / df_number['All'].max()) * 100).round(1)
                    df_mean = df_t2.pivot_table(index=['Пол'], values='Вес особи', columns='Возраст', aggfunc='mean',
                                                margins=True).round(1)
                    # %
                    df_per = df_number.T
                    df_per['%'] = ((df_per['All'] / df_per['All'].max()) * 100).round(1)
                    df_number = df_per.T
                    df_number = pd.merge(df_number, df_mean['All'], on=['Пол'], how='left')
                    self.rename_df_columns(df_number, {'All_x': 'Кол-во(шт.)', 'All_y': 'Ср.вес(г.)'})
                    # rename_str = 'Численность'
                    midx = pd.MultiIndex.from_tuples(
                        [('Численность', 'F'), ('Численность', 'M'), ('Численность', 'All'), ('Численность', '%'), ],
                        names=['', 'Пол'])
                    df_number.index = midx

                else:
                    if value == 'Жирность':
                        # if df_t2.empty:
                        #     return pd.DataFrame()
                        # else:
                        df_sum_w = df_t2.pivot_table(index=['Пол'], values='Вес особи', columns='Возраст', aggfunc='sum',
                                                    margins=True).round(3)
                        df_sum_wp = df_t2.pivot_table(index=['Пол'], values='Вес печени', columns='Возраст', aggfunc='sum',
                                                    margins=True).round(3)
                        df_mean = ((df_sum_wp / df_sum_w) * 100).astype(float).round(1)

                    else:
                        df_mean = df_t2.pivot_table(index=['Пол'], values=value, columns='Возраст', aggfunc='mean',
                                                    margins=True).round(1)

                    df_number = pd.merge(df_mean, df_number['All'], on=['Пол'], how='left')
                    self.rename_df_columns(df_number, {'All_y': 'Кол-во(шт.)', 'All_x': rename_str})
                    midx = pd.MultiIndex.from_tuples([(rename_str, i) for i in df_number.index], names=['', 'Пол'])
                    df_number.index = midx

                df_number.replace(0, np.nan, inplace=True)
                df_number.dropna(how='all', inplace=True)
                df_number.fillna('-', inplace=True)

                mcol = pd.MultiIndex.from_tuples([('Возраст', i) for i in df_number.columns], names=['', ''])
                df_number.columns = mcol

                return df_number

        df_fat = df_t2.copy()
        filt = df_fat['Вес печени'] > 0
        df_fat = df_fat[filt]
        if df_fat.empty:
            table4 = pd.DataFrame()
        else:
            # print(df_fat)
        # считаем колонку с жирностью
            df_fat.loc[:, 'fat'] = (df_fat['Вес печени'] / df_fat['Вес особи']) * 100
            self.rename_df_columns(df_fat, {'fat': 'Жирность'})
            table4 = make_table(4, df_fat, 'Жирность')

        # results
        table2 = make_table(2, df_t2, 'Вес особи')
        # print(table2)
        # t1_total = pd.concat([counting_table, mean_len_table, mean_wes_table])
        table3 = make_table(3, df_t2, 'Вес особи')
        # print(df_fat)
        # t2_total = mean_wes_table
        
        # print(table4)
        # t3_total = mean_fat_table

        # suntotals
        counting_table = make_totals(1, df_t2, 'Вес особи')
        mean_len_table = make_totals(2, df_t2, 'Б.длина')
        mean_wes_table = make_totals(3, df_t2, 'Вес особи')
        mean_fat_table = make_totals(4, df_t2, 'Жирность')
        t1_total = pd.concat([counting_table, mean_len_table, mean_wes_table])
        t1_total.replace(0, np.nan, inplace=True)
        t1_total.dropna(how='all', inplace=True)
        t1_total.fillna('-', inplace=True)

        t2_total = mean_wes_table
        t3_total = mean_fat_table

        return [table1, table2, table3, table4, t1_total, t2_total, t3_total]

    # ФОРМА 12
    # ============= Формируем все таблички (типа выборки) для 12 формы
    def datas_for_aged_prob12(self, params):
        # print(params)
        start_period1 = params[4]
        end_period1 = params[5]

        start_period2 = params[6]
        end_period2 = params[7]

        if start_period1 and end_period1 and start_period2 and end_period2:
            sql_t1 = f"""
            SELECT a.ship,a.reis,a.trl,a.data,a.rloc,a.rzon,a.rikes,a.shir,a.dolg,b.proba, count(*) as kolr
            FROM tral_view a, biopap_view b
            WHERE a.ship = b.ship and a.reis=b.reis and a.trl=b.trl 
            and b.vozr1 is not null and 
            ((a."data" between TO_DATE('{start_period1}', 'YYYY-MM-DD') and TO_DATE('{end_period1}', 'YYYY-MM-DD')) 
            or (a."data" between TO_DATE('{start_period2}','YYYY-MM-DD') and TO_DATE('{end_period2}', 'YYYY-MM-DD')))
            GROUP BY a.ship,a.reis,a.trl,a.data,a.rloc,a.rzon,a.rikes,a.shir,a.dolg,b.proba
            ORDER BY a.ship,a.reis,a.trl,a.data,a.rloc,a.rzon,a.rikes,a.shir,a.dolg;
            """

            sql_t2 = f"""
            SELECT b.vozr1, b.pol, b.zrel, a.data
            FROM tral_view a, biopap_view b
            WHERE a.ship=b.ship and a.reis=b.reis and a.trl=b.trl and b.vozr1 is not null and 
            ((a."data" between TO_DATE('{start_period1}', 'YYYY-MM-DD') and TO_DATE('{end_period1}', 'YYYY-MM-DD')) or 
            (a."data" between TO_DATE('{start_period2}','YYYY-MM-DD') and TO_DATE('{end_period2}', 'YYYY-MM-DD')))
            ORDER BY zrel;
            """

        else:
            sql_t1 = f"""
            SELECT a.ship,a.reis,a.trl,a.data,a.rloc,a.rzon,a.rikes,a.shir,a.dolg,b.proba, count(*) as kolr
            FROM tral_view a, biopap_view b
            WHERE a.ship = b.ship and a.reis=b.reis and a.trl=b.trl and b.vozr1 is not null
            GROUP BY a.ship,a.reis,a.trl,a.data,a.rloc,a.rzon,a.rikes,a.shir,a.dolg,b.proba
            ORDER BY a.ship,a.reis,a.trl,a.data,a.rloc,a.rzon,a.rikes,a.shir,a.dolg;
            """

            sql_t2 = f"""
            SELECT b.vozr1, b.pol, b.zrel, a.data
            FROM tral_view a, biopap_view b
            WHERE a.ship=b.ship and a.reis=b.reis and a.trl=b.trl and b.vozr1 is not null
            ORDER BY zrel;
            """

        try:
            res_1 = self.make_dict_request(sql_t1)
            res_2 = self.make_dict_request(sql_t2)

            t1 = pd.DataFrame(res_1)
            t2 = pd.DataFrame(res_2)

            if res_1 or res_2:
                try:
                    result = self.counting_aged_prob12([t1, t2], params)
                    return result
                except Exception as e:
                    print(f'Ошибка: {e}')
                    return []

        except Exception as e:
            print(f'Ошибка чтения из БД: {e}')
            return []

    def counting_aged_prob12(self, data, params):
        # Первая табличка
        df1 = data[0]
        # Собираем данные для 1 таблички (вид)
        # Переименуем колонки
        self.rename_df_columns(df1, {'ship': 'Позывной', 'reis': 'Рейс', 'trl': 'Трал',
                                     'data': 'Дата', 'rloc': 'Лок.район', 'rzon': '408 пр.',
                                     'rikes': '520 пр.', 'shir': 'Широта', 'dolg': 'Долгота',
                                     'proba': 'Проба', 'kolr': 'Кол-во (шт.)'})
        table1 = df1

        # Вторая табличка
        df2 = data[1]

        self.rename_df_columns(df2, {'vozr1': 'Возраст', 'pol': 'Пол', 'zrel': 'Зрелость', 'data': 'Дата'})
        df2['Дата'] = pd.to_datetime(df2['Дата'])

        def make_tables(df, kind):
            # сводник
            df_pivot = df.groupby(['Возраст']).apply(lambda sub_df:
                                                     sub_df.pivot_table(index=['Пол'], values='Дата',
                                                                        columns='Зрелость', aggfunc='count',
                                                                        margins=True, margins_name='Итого'))
            count_cols = df_pivot.columns
            new_list = []
            for i in count_cols:
                if i != 'Итого':
                    new_list.append(i)
            new_list.sort()
            new_list.append('Итого')
            df_pivot = df_pivot[new_list]
            self.rename_df_columns(df_pivot, {'Итого': kind})

            if kind == 'Кол-во(шт.)':
                df_per_pol = df.pivot_table(index=['Возраст', 'Пол'], columns='Зрелость', aggfunc='count', margins=True,
                                            margins_name='Итого')
                max_count = df_per_pol.T['Итого'].max()
                df_pivot['%'] = ((df_pivot['Кол-во(шт.)'] / max_count) * 100).round(1)
                df_pivot = df_pivot[['Кол-во(шт.)', '%']]

            # удаляем лишнее
            df_pivot.columns.name = None

            return df_pivot

        def make_total(df, kind):
            total_df = df.pivot_table(index=['Пол'], columns='Зрелость', values='Возраст', aggfunc='count',
                                      margins=True)
            if kind == 'Кол-во(шт.)':
                max_count = total_df.T['All'].max()
                total_df['%'] = ((total_df['All'] / max_count) * 100).round(2)
                total_df = total_df[['All', '%']]

            self.rename_df_columns(total_df, {'All': kind})
            total_df.columns.name = None

            return total_df

        # стадии возраста для неполовозрелых рыбок
        np_zrel1 = params[0]
        np_zrel2 = params[1]
        # диапазон дат второй период для деланья сдвига -1 по возрасту
        data2_period1 = params[6]
        data2_period2 = params[7]

        # если введены все параметры: стадии зрелости и даты
        if np_zrel1 and np_zrel2 and data2_period1 and data2_period2:

            mask = (df2['Дата'] >= data2_period1) & (df2['Дата'] <= data2_period2)
            df2.loc[mask, 'Возраст'] -= 1

            # фреймик неполовозрелые
            np_df = df2[(df2['Зрелость'] < int(np_zrel1)) | (df2['Зрелость'] <= int(np_zrel2))]
            # фреймик половозрелые
            pp_df = df2[~((df2['Зрелость'] < int(np_zrel1)) | (df2['Зрелость'] <= int(np_zrel2)))]
            # делаем вычисления
            np_pivot = make_tables(np_df, 'Неполовозрелые')
            pp_pivot = make_tables(pp_df, 'Половозрелые')
            count_pivot = make_tables(df2, 'Кол-во(шт.)')
            # слепляем во едино
            np_pp_df = pd.merge(np_pivot, pp_pivot, on=['Возраст', 'Пол'], how='left')
            df_result = pd.merge(np_pp_df, count_pivot, on=['Возраст', 'Пол'], how='left')
            df_result.replace(0, np.nan, inplace=True)
            df_result.dropna(how='all', inplace=True)
            df_result.fillna('-', inplace=True)
            table2 = df_result
            # итоги
            np_total = make_total(np_df, 'Неполовозрелые')
            pp_total = make_total(pp_df, 'Половозрелые')
            count_total = make_total(df2, 'Кол-во(шт.)')
            total_zrel = pd.merge(np_total, pp_total, on=['Пол'], how='left')
            total_result = pd.merge(total_zrel, count_total, on=['Пол'], how='left')

            temp_t = total_result.T
            temp_t['%'] = ((temp_t['All'] / temp_t['All'].max()) * 100).round(1)
            total_table2 = temp_t.T
            total_table2.replace(0, np.nan, inplace=True)
            total_table2.dropna(how='all', inplace=True)
            total_table2.fillna('-', inplace=True)

        # если введены только параметры: стадии зрелости
        elif (not(data2_period1 and data2_period2)) and  (np_zrel1 and np_zrel2):
            # print('here')
            # фреймик неполовозрелые
            np_df = df2[(df2['Зрелость'] < int(np_zrel1)) | (df2['Зрелость'] <= int(np_zrel2))]
            # фреймик половозрелые
            pp_df = df2[~((df2['Зрелость'] < int(np_zrel1)) | (df2['Зрелость'] <= int(np_zrel2)))]
            # делаем вычисления
            np_pivot = make_tables(np_df, 'Неполовозрелые')
            pp_pivot = make_tables(pp_df, 'Половозрелые')
            count_pivot = make_tables(df2, 'Кол-во(шт.)')
            # слепляем во едино
            np_pp_df = pd.merge(np_pivot, pp_pivot, on=['Возраст', 'Пол'], how='left')
            df_result = pd.merge(np_pp_df, count_pivot, on=['Возраст', 'Пол'], how='left')
            df_result.replace(0, np.nan, inplace=True)
            df_result.dropna(how='all', inplace=True)
            df_result.fillna('-', inplace=True)
            table2 = df_result
            # итоги
            np_total = make_total(np_df, 'Неполовозрелые')
            pp_total = make_total(pp_df, 'Половозрелые')
            count_total = make_total(df2, 'Кол-во(шт.)')
            total_zrel = pd.merge(np_total, pp_total, on=['Пол'], how='left')
            total_result = pd.merge(total_zrel, count_total, on=['Пол'], how='left')

            temp_t = total_result.T
            temp_t['%'] = ((temp_t['All'] / temp_t['All'].max()) * 100).round(1)
            total_table2 = temp_t.T
            total_table2.replace(0, np.nan, inplace=True)
            total_table2.dropna(how='all', inplace=True)
            total_table2.fillna('-', inplace=True)
        # если введены только параметры: дата
        elif (not(np_zrel1 and np_zrel2)) and (data2_period1 and data2_period2):
            mask = (df2['Дата'] >= data2_period1) & (df2['Дата'] <= data2_period2)
            df2.loc[mask, 'Возраст'] -= 1
            zrel_pivot = df2.groupby(['Возраст']).apply(lambda sub_df:
                                                        sub_df.pivot_table(index=['Пол'], values='Дата',
                                                                           columns='Зрелость',
                                                                           aggfunc='count', margins=True,
                                                                           margins_name='Итого'))
            count_cols = zrel_pivot.columns
            new_list = []
            for i in count_cols:
                if i != 'Итого':
                    new_list.append(i)
            new_list.sort()
            new_list.append('Итого')
            zrel_pivot = zrel_pivot[new_list]
            self.rename_df_columns(zrel_pivot, {'Итого': 'Кол-во(шт.)'})
            # тут я делаю агрегирование для нахождения % по весу
            df_per_pol = df2.pivot_table(index=['Возраст', 'Пол'], columns='Зрелость', aggfunc='count', margins=True,
                                         margins_name='Итого')
            max_count = df_per_pol.T['Итого'].max()
            zrel_pivot['%'] = ((zrel_pivot['Кол-во(шт.)'] / max_count) * 100).round(1)

            current_columns = list([('Стадии зрелости', i) for i in zrel_pivot.columns])
            zrel_pivot.columns = pd.MultiIndex.from_tuples(current_columns)
            zrel_pivot.replace(0, np.nan, inplace=True)
            zrel_pivot.dropna(how='all', inplace=True)
            zrel_pivot.fillna('-', inplace=True)
            table2 = zrel_pivot

            count_total = make_total(df2, 'Кол-во(шт.)')
            total_table2 = count_total
        # если ничего не введено:
        elif not (data2_period1 and data2_period2 and np_zrel1 and np_zrel2):
            count_table = df2.groupby(['Возраст']).apply(lambda sub_df: sub_df.pivot_table(index=['Пол'], values='Дата',
                                                         columns='Зрелость', aggfunc='count', margins=True, margins_name='Итого'))
            count_cols = count_table.columns
            new_list = []

            for i in count_cols:
                if i != 'Итого':
                    new_list.append(i)
            new_list.sort()
            new_list.append('Итого')
            count_table = count_table[new_list]
            self.rename_df_columns(count_table, {'Итого': 'Кол-во(шт.)'})
            # тут я делаю агрегирование для нахождения % по весу
            df_per_pol = df2.pivot_table(index=['Возраст', 'Пол'], columns='Зрелость', aggfunc='count', margins=True,
                                         margins_name='Итого')
            max_count = df_per_pol.T['Итого'].max()
            count_table['%'] = ((count_table['Кол-во(шт.)'] / max_count) * 100).round(1)
            current_columns = list([('Стадии зрелости', i) for i in count_table.columns])
            count_table.columns = pd.MultiIndex.from_tuples(current_columns)
            count_table.replace(0, np.nan, inplace=True)
            count_table.dropna(how='all', inplace=True)
            count_table.fillna('-', inplace=True)
            table2 = count_table
            # print(table2)

            df_per_pol = df2.pivot_table(index=['Пол'], columns='Зрелость', values='Дата', aggfunc='count', margins=True, margins_name='Итого')
            df_per_T = df_per_pol.T
            df_per_T['%'] = ((df_per_T['Итого'] / df_per_T['Итого'].max()) * 100).round(1)
            df_per_pol = df_per_T.T
            df_per_pol['% по полу'] = ((df_per_pol['Итого'] / df_per_pol['Итого'].max()) * 100).round(1)
            total_table2 = df_per_pol
            # print('Пустые элементы:', params[0] , params[1] , params[6] , params[7])

        return [table1, table2, total_table2]

    # ФОРМА 13
    # ============= Формируем все таблички (типа выборки) для 13 формы
    def datas_for_aged_prob13(self):
        # -- форма 13. ИНТЕНСИВНОСТЬ ПИТАНИЯ И ЧАСТОТА ВСТРЕЧАЕМОСТИ ПИЩЕВЫХ ОРГАНИЗМОВ
        sql_t1 = """
        SELECT a.ship,a.reis,a.trl,a.data,a.rloc,a.rzon,a.rikes,a.shir,a.dolg,b.proba, COUNT(*) as kolr
        FROM tral_view a, biopap_view b
        WHERE a.ship = b.ship and a.reis=b.reis and a.trl=b.trl and b.puzo is not null
        GROUP BY a.ship,a.reis,a.trl,a.data,a.rloc,a.rzon,a.rikes,a.shir,a.dolg,b.proba
        ORDER BY a.ship,a.reis,a.trl,a.data,a.rloc,a.rzon,a.rikes,a.shir,a.dolg;
        """
        # --Жирность в баллах
        sql_t2 = """
        SELECT b.salo, COUNT(*)
        FROM tral_view a, biopap_view b
        WHERE a.ship=b.ship and a.reis=b.reis and a.trl=b.trl and b.puzo is not null
        GROUP BY b.salo
        ORDER BY b.salo;
        """
        # -- наполнение желудка в баллах
        sql_t3 = """
        SELECT b.puzo, b.pol, COUNT(*) as kolr
        FROM tral_view a, biopap_view b
        WHERE a.ship=b.ship and a.reis=b.reis and a.trl=b.trl and b.puzo is not null
        GROUP BY b.puzo,b.pol
        ORDER BY b.puzo,b.pol;
        """
        # -- распредление питания
        sql_t4 = """
        SELECT b.pol, c.kodpit, kp.nam_pit, COUNT(*) as kolr
        FROM biopap_view b
        JOIN biopit c ON c.bikl=b.bikl
        JOIN kod_pit kp ON kp.kod_pit=c.kodpit
        WHERE b.puzo is not null and c.kodpit is not null and c.kodpit > 0
        GROUP BY b.pol, c.kodpit, kp.nam_pit
        ORDER BY b.pol, c.kodpit, kp.nam_pit;
        """

        try:
            res_1 = self.make_dict_request(sql_t1)
            res_2 = self.make_dict_request(sql_t2)
            res_3 = self.make_dict_request(sql_t3)
            res_4 = self.make_dict_request(sql_t4)

            t1 = pd.DataFrame(res_1)
            t2 = pd.DataFrame(res_2)
            t3 = pd.DataFrame(res_3)
            t4 = pd.DataFrame(res_4)

            if res_1 or res_2 or res_3 or res_4:
                try:
                    result = self.counting_aged_prob13([t1, t2, t3, t4])
                    return result
                except Exception as e:
                    print(f'Ошибка: {e}')
                    return []

        except Exception as e:
            print(f'Ошибка чтения из БД: {e}')
            return []

    # ============= // Формируем первые таблички
    # ============= Вычисления формы 13
    def counting_aged_prob13(self, data):
        # Хапаем полученные запросы в таблички
        df1 = data[0]
        df2 = data[1]
        df3 = data[2]
        df4 = data[3]

        # table 1 --Выборка
        # переименовываем колонки
        try:
            self.rename_df_columns(df1, {'ship': 'Позывной', 'reis': 'Рейс', 'trl': 'Трал',
                                    'data': 'Дата', 'rloc': 'Лок.район', 'rzon': '408 пр.',
                                    'rikes': '520 пр.', 'shir': 'Широта', 'dolg': 'Долгота',
                                    'proba': 'Проба', 'kolr': 'Кол-во (шт.)'})
            # подсчитываем общ кол-во проб (итого)
            df1_piv = df1.pivot_table(
                index=['Позывной', 'Рейс', 'Дата', 'Трал', 'Широта', 'Долгота', 'Проба', 'Лок.район', '408 пр.', '520 пр.'],
                values=['Кол-во (шт.)'], aggfunc='sum', margins=True)
            dict_map = {'All': 'Итого:'}
            t1 = df1_piv.rename(index=dict_map).reset_index()
        except:
            t1 = pd.DataFrame()

        # table 2 --Жирность в баллах
        try:
            self.rename_df_columns(df2, {'salo': 'Балл', 'count': 'Кол-во(шт.)'})
            # там где балл был NULL - переименовали в неопред.
            df2.fillna('Неопред.', inplace=True)
            df2_pivot = df2.pivot_table(index='Балл', aggfunc='sum', margins=True)
            df2_pivot['%'] = ((df2_pivot['Кол-во(шт.)'] / df2_pivot['Кол-во(шт.)'].max()) * 100).round(1)
            df2_pivot = df2_pivot.T
            df2_pivot.columns.name = None
            self.rename_df_columns(df2_pivot, {'All': 'Итого'})
            t2 = df2_pivot
        except:
            t2 = pd.DataFrame()

        # table 3 --Наполнение желудка в баллах
        try:
            self.rename_df_columns(df3, {'puzo': 'Балл', 'pol': 'Пол', 'kolr': 'Кол-во(шт.)'})
            df3_pivot = df3.pivot_table(index='Пол', columns='Балл', values='Кол-во(шт.)', aggfunc='sum', margins=True)
            df3_pivot.columns.name = None
            df3_pivot_T = df3_pivot.T
            df3_pivot_T['%'] = ((df3_pivot_T['All'] / df3_pivot_T['All'].max()) * 100).round(1)
            df3_pivot = df3_pivot_T.T
            t3 = df3_pivot.reset_index()
        except:
            t3 = pd.DataFrame()

        # table 4 --Распредление питания
        try:
            self.rename_df_columns(df4, {'pol': 'Пол', 'kodpit': 'Код', 'nam_pit': 'Название', 'kolr': 'Кол-во(шт.)'})
            # print(df4)
            df4_pivot = df4.pivot_table(index=['Код', 'Название'], columns='Пол', values='Кол-во(шт.)', aggfunc='sum', margins=True).round(2)
            df4_pivot = df4_pivot.reset_index()
            # % по полам
            cols = df4_pivot.columns
            N = len(cols[2:])
            df4_pivot_pol = df4_pivot.copy()
            count_dict = {}
            count_dict['Код'] = df4_pivot_pol['Код']
            count_dict['Название'] = df4_pivot_pol['Название']
            for i in range(2, N + 1):
                current_df = df4_pivot_pol.iloc[:, [i]]
                count_dict[f'{current_df.columns[0]} %'] = (
                            (current_df[current_df.columns[0]] / current_df[current_df.columns[0]].max()) * 100).round(2)
            count_dict['All %'] = ((df4_pivot_pol['All'] / df4_pivot_pol['All'].max()) * 100).round(2)
            new_df = pd.DataFrame(count_dict)
            # объединяем % с количествами
            merged_Df = pd.merge(df4_pivot, new_df, on=['Код', 'Название'], how='left')
            t4 = merged_Df
        except:
            t4 = pd.DataFrame()

        return [t1, t2, t3, t4]
        # t4 = df4_pivot.reset_index()

    # ФОРМА 15
    # ============= Формируем все таблички (типа выборки) для 12 формы
    def datas_for_aged_prob15(self, params):
        print(params)
        start_row = params[4]
        sql_t1 = f"""
            SELECT a.ship,a.reis,a.trl,a.data,a.rloc,a.rzon,a.rikes,a.shir,a.dolg,b.proba, COUNT(*) as kolr
            FROM tral_view a, biopap_view b
            WHERE a.ship = b.ship and a.reis=b.reis and a.trl=b.trl 
            and (b.l_big is not null and b.l_big >= {start_row})
            GROUP BY a.ship,a.reis,a.trl,a.data,a.rloc,a.rzon,a.rikes,a.shir,a.dolg,b.proba
            ORDER BY a.ship,a.reis,a.trl,a.data,a.rloc,a.rzon,a.rikes,a.shir,a.dolg;
            """

        sql_t2 = f"""
            SELECT b.l_big, b.pol, b.zrel, COUNT(*) as kolr
            FROM biopap_view b
            WHERE (b.l_big is not null and b.l_big >= {start_row})
            GROUP BY b.l_big, b.pol, b.zrel
            ORDER BY b.l_big, b.pol, b.zrel;
            """

        try:
            res_1 = self.make_dict_request(sql_t1)
            res_2 = self.make_dict_request(sql_t2)

            t1 = pd.DataFrame(res_1)
            t2 = pd.DataFrame(res_2)

            if res_1 or res_2:
                try:
                    result = self.counting_aged_prob15([t1, t2], params)
                    return result
                except Exception as e:
                    print(f'Ошибка: {e}')
                    return []

        except Exception as e:
            print(f'Ошибка чтения из БД: {e}')
            return []

    def counting_aged_prob15(self, data, params):
        # print(data)
        # Первая табличка
        df1 = data[0]
        # Собираем данные для 1 таблички (вид)
        # Переименуем колонки
        self.rename_df_columns(df1, {'ship': 'Позывной', 'reis': 'Рейс', 'trl': 'Трал',
                                     'data': 'Дата', 'rloc': 'Лок.район', 'rzon': '408 пр.',
                                     'rikes': '520 пр.', 'shir': 'Широта', 'dolg': 'Долгота',
                                     'proba': 'Проба', 'kolr': 'Кол-во (шт.)'})
        df1_piv = df1.pivot_table(
            index=['Позывной', 'Рейс', 'Дата', 'Трал', 'Широта', 'Долгота', 'Проба', 'Лок.район', '408 пр.', '520 пр.'],
            values=['Кол-во (шт.)'], aggfunc='sum', margins=True)
        dict_map = {'All': 'Итого:'}
        t1 = df1_piv.rename(index=dict_map).reset_index()
        table1 = t1

        # Вторая табличка
        df2 = data[1]

        # стадии возраста для неполовозрелых рыбок
        np_zrel1 = params[0]
        np_zrel2 = params[1]

        if np_zrel1:
            np_zrel1 = int(np_zrel1)

        if np_zrel2:
            np_zrel2 = int(np_zrel2)

        start_row = int(params[4])
        step = int(params[5])

        self.rename_df_columns(df2, {'l_big': 'Длина', 'pol': 'Пол', 'zrel': 'Зрелость', 'kolr': 'Кол-во (шт.)'})
        # Переведем длину в сантиметры
        df2['Длина'] /= 10
        # Делим на бины
        df_t2 = self.make_bins(df2, 'Длина', start_row/10, step)

        self.rename_df_columns(df_t2, {'bucket_range': 'Размер'})

        def make_tables(df, kind):
            # сводник
            df_pivot = df.groupby(['Размер']).apply(lambda sub_df:
                                                    sub_df.pivot_table(index=['Пол'], values='Кол-во (шт.)',
                                                                       columns='Зрелость', aggfunc='sum', margins=True,
                                                                       margins_name='Итого'))
            count_cols = df_pivot.columns
            new_list = []
            for i in count_cols:
                if i != 'Итого':
                    new_list.append(i)
            new_list.sort()
            new_list.append('Итого')
            df_pivot = df_pivot[new_list]
            self.rename_df_columns(df_pivot, {'Итого': kind})

            if kind == 'Кол-во(шт.)':
                df_per_pol = df_t2.pivot_table(index=['Размер', 'Пол'], values='Кол-во (шт.)', columns='Зрелость',
                                               aggfunc='sum', margins=True, margins_name='Итого')
                max_count = df_per_pol.T['Итого'].max()
                df_pivot['% по полу'] = ((df_pivot['Кол-во(шт.)'] / max_count) * 100)
                df_pivot = df_pivot[['Кол-во(шт.)', '% по полу']]

            # удаляем лишнее
            df_pivot.columns.name = None

            return df_pivot

        def make_total(df, kind):
            total_df = df.pivot_table(index=['Пол'], columns='Зрелость', values='Кол-во (шт.)', aggfunc='sum',
                                      margins=True)
            if kind == 'Кол-во(шт.)':
                max_count = total_df.T['All'].max()
                total_df['%'] = ((total_df['All'] / max_count) * 100)
                total_df = total_df[['All', '%']]

            self.rename_df_columns(total_df, {'All': kind})
            total_df.columns.name = None

            return total_df

        # если введены только параметры: стадии зрелости
        if (np_zrel1 and np_zrel2):
            # неполовозрелые
            np_df = df2[(df2['Зрелость'] < np_zrel1) | (df2['Зрелость'] <= np_zrel2)]
            self.rename_df_columns(np_df, {'bucket_range': 'Размер'})

            # половозрелые
            pp_df = df2[~((df2['Зрелость'] < np_zrel1) | (df2['Зрелость'] <= np_zrel2))]
            self.rename_df_columns(pp_df, {'bucket_range': 'Размер'})
            # Делаем табличку
            np_pivot = make_tables(np_df, 'Неполовозрелые')
            pp_pivot = make_tables(pp_df, 'Половозрелые')
            count_pivot = make_tables(df_t2, 'Кол-во(шт.)')

            np_pp_df = pd.merge(np_pivot, pp_pivot, on=['Размер', 'Пол'], how='left')
            df_result = pd.merge(np_pp_df, count_pivot, on=['Размер', 'Пол'], how='left')
            df_result.replace(0, np.nan, inplace=True)
            df_result.dropna(how='all', inplace=True)
            df_result.fillna('-', inplace=True)
            t2 = df_result.reset_index()

            # Табличка итогов (днище)
            np_total = make_total(np_df, 'Неполовозрелые')
            pp_total = make_total(pp_df, 'Половозрелые')
            count_total = make_total(df2, 'Кол-во(шт.)')

            total_zrel = pd.merge(np_total, pp_total, on=['Пол'], how='left')
            total_result = pd.merge(total_zrel, count_total, on=['Пол'], how='left')
            temp_t = total_result.T
            temp_t['%'] = ((temp_t['All'] / temp_t['All'].max()) * 100).round(1)
            total_prc = temp_t.T
        # Если НЕТ ограничений по зрелости
        elif not (np_zrel1 and np_zrel2):
            count_pivot = df_t2.groupby(['Размер']).apply(lambda sub_df:
                                                          sub_df.pivot_table(index=['Пол'], values='Кол-во (шт.)',
                                                                             columns='Зрелость', aggfunc='sum',
                                                                             margins=True, margins_name='Итого'))
            count_cols = count_pivot.columns
            new_list = []
            for i in count_cols:
                if i != 'Итого':
                    new_list.append(i)
            new_list.sort()
            new_list.append('Итого')
            count_pivot = count_pivot[new_list]
            self.rename_df_columns(count_pivot, {'Итого': 'Кол-во(шт.)'})
            # делаю агрегирование для нахождения % по весу
            df_per_pol = df_t2.pivot_table(index=['Размер', 'Пол'], values='Кол-во (шт.)', columns='Зрелость',
                                           aggfunc='sum', margins=True, margins_name='Итого')
            max_count = df_per_pol.T['Итого'].max()
            count_pivot['% по полу'] = ((count_pivot['Кол-во(шт.)'] / max_count) * 100)
            count_pivot.replace(0, np.nan, inplace=True)
            count_pivot.dropna(how='all', inplace=True)
            count_pivot.fillna('-', inplace=True)
            t2 = count_pivot.reset_index()

            # Днище
            df_pol = df_t2.pivot_table(index=['Пол'], values='Кол-во (шт.)', columns='Зрелость', aggfunc='sum',
                                       margins=True, margins_name='Итого')
            max_count = df_pol.T['Итого'].max()
            df_pol['% по полу'] = ((df_pol['Итого'] / max_count) * 100)
            df_pol.columns.name = None
            total_prc = df_pol.reset_index()

        return [table1, t2, total_prc]

    # ============= // ФУНКЦИИ ФОРМ =============
    





    # ============= Справка о наличии информации в базе =============
    def info_data(self): 
        sql = '''select bort, ship, reis, trl1, trl2, data1, data2, 
        ntral, nulov, nrazm, nbiopap, nbiopit, nbiokap, npandalus, ncrabs, nmorph, nacq, "operator", act_date::date 
        from tral_doc td
        order by 2,3 asc;
        '''

        try:
            res = self.make_dict_request(sql)
            df_info = pd.DataFrame(res)
            # print(df_info)
            self.rename_df_columns(df_info, {'bort':'Бортовой', 'ship':'Позывной', 'reis':'Рейс', 'trl1':'Мин.трал',
                                             'trl2':'Макс.трал', 'data1':'Начало рейса', 'data2':'Конец рейса', 'ntral':'TRAL',
                                             'nulov':'ULOV', 'nrazm':'RAZM','nbiopap':'BIOPAP','nbiopit':'BIOPIT','operator':'Оператор',
                                             'act_date':'Дата корректировки', 'npandalus': 'PANDALUS', 'ncrabs': 'CRABS', 'nmorph': 'MORPH',
                                             'nacq': 'ACQ', 'nbiokap':'BIOKAP'
                                             })
            df_info.fillna('-', inplace=True)

            # if df_info:
            return df_info
        except:
            print('Ошибка чтения из БД')
        return []
    


    # ============= // Справка о наличии информации в базе =============


    # # ============= вспомогательные функции

    def make_general_table(self, data):
        """делает первую таблицу (та шо самая верхняя в каждой формочке) с выборками"""
        df1 = data[0]
        # Собираем данные для 1 таблички (вид)
        # Переименуем колонки
        self.rename_df_columns(df1, {'ship': 'Позывной', 'reis': 'Рейс', 'trl': 'Трал',
                                     'data': 'Дата', 'rloc': 'Лок.район', 'rzon': '408 пр.',
                                     'rikes': '520 пр.', 'shir': 'Широта', 'dolg': 'Долгота',
                                     'proba': 'Проба', 'kolr': 'Кол-во (шт.)'})
        df1_piv = df1.pivot_table(
            index=['Позывной', 'Рейс', 'Дата', 'Трал', 'Широта', 'Долгота', 'Проба', 'Лок.район', '408 пр.', '520 пр.'],
            values=['Кол-во (шт.)'], aggfunc='sum', margins=True)
        t1 = df1_piv.rename(index={'All': 'Итого:'})
        table1 = t1.reset_index()
        return table1

    def coef_variation(self, x):
        """Функция для подсчета коэффициента вариации"""
        return round(np.std(x, ddof=1) / np.mean(x), 2)

    def standard_error(self, x):
        """Функция для подсчета стандартного отклонения (средняя ошибка)"""
        return round(np.std(x, ddof=1) / np.sqrt(np.size(x)), 2)

    def make_bins2(self, df, selected_length_option, start_value, step):
        # step = step / 10
        result_df = pd.DataFrame(columns=df.columns)

        # Находим максимальное значение колонки длины
        max_value = float(df[selected_length_option].max())

        # Создаем диапазоны с помощью функции cut()
        bins = [start_value + i * step for i in range(int((max_value - start_value) / step) + 2)]
        labels = [f"{bin}-{bin + step}" for bin in bins[:-1]]

        # Создаем колонку с диапазонами в исходном датафрейме
        df['bucket_range'] = pd.cut(df[selected_length_option], bins=bins, labels=labels, right=False)

        for group, group_df in df.groupby('bucket_range'):
            result_df = pd.concat([result_df, group_df], ignore_index=True)

        # result_df.dropna(how='any', inplace=True)

        return result_df

    def make_bins(self, df, selected_length_option, start_value, step):
        """Делит указанный столбец на бины с указанного значения с указанным шагом"""
        step = step / 10
        result_df = pd.DataFrame(columns=df.columns)

        # Находим максимальное значение колонки длины
        max_value = float(df[selected_length_option].max())

        # Создаем диапазоны с помощью функции cut()
        bins = [start_value + i * step for i in range(int((max_value - start_value) / step) + 2)]
        labels = [f"{bin}-{bin + step}" for bin in bins[:-1]]

        # Создаем колонку с диапазонами в исходном датафрейме
        df['bucket_range'] = pd.cut(df[selected_length_option], bins=bins, labels=labels, right=False)

        for group, group_df in df.groupby('bucket_range'):
            result_df = pd.concat([result_df, group_df], ignore_index=True)

        # result_df.dropna(how='any', inplace=True)

        return result_df

    def add_new_level(self, df, level):
        new_level = level
        current_columns = list([new_level + [i] for i in df.columns])
        new_cols = list([tuple(i) for i in current_columns])
        df.columns = pd.MultiIndex.from_tuples(new_cols)
        return df
        
    def rename_df_columns(self, report_pivot, name_cols):
        """Функция переименовывает колонки на русский язык"""
        report_pivot.rename(columns=name_cols, inplace=True)

        return report_pivot

    def drop_multiindex_columns(self, report_pivot):
        """Функция убирает мультииндексы из сводной таблицы - склеивает их"""
        report_pivot.columns = ['_'.join(str(s).strip() for s in col if s) for col in report_pivot.columns]

        return report_pivot

    def create_dict(self, df):
        """Функция создает словарь для переименования колонок """
        dict_cols = {}

        for i in df.columns:
            dict_cols[f'{i}'] = i.split('_')[-1]

        return dict_cols

    def getShipReis(self):
        sql = "SELECT DISTINCT ship, reis FROM tral ORDER BY 1, 2;"
        try:
            res = self.make_dict_request(sql)
            if res:
                return res
        except:
            print('Ошибка чтения из БД')
        return []

    def getOrlov(self):
        sql = "SELECT kod_orl, vid_orl FROM kod_orl;"
        try:
            res = self.make_dict_request(sql)
            if res:
                return res
        except:
            print('Ошибка чтения из БД')
        return []

    def getVidlov(self):
        sql = "SELECT k_lov, n_lov FROM spr_vidlov;"
        try:
            res = self.make_dict_request(sql)
            if res:
                return res
        except:
            print('Ошибка чтения из БД')
        return []

    def getKodPit(self):
        sql = "SELECT kod_pit, nam_pit FROM kod_pit ORDER by kod_pit;"
        try:
            res = self.make_dict_request(sql)
            if res:
                return res
        except:
            print('Ошибка чтения из БД')
        return []

    def getDopCode(self):
        sql = "SELECT u_key, key_full FROM dopcode ORDER BY u_key ASC;"
        try:
            res = self.make_dict_request(sql)
            if res:
                return res
        except:
            print('Ошибка чтения из БД')
        return []

    def getKodFish(self):
        sql = "SELECT distinct kod_fish, nam_fish FROM kod_fish ORDER BY nam_fish;"
        try:
            res = self.make_dict_request(sql)
            if res:
                return res
        except:
            print('Ошибка чтения из БД')
        return []

    def getIkesDistricts(self):
        sql = "SELECT ikes_k, ikes_n FROM spr_ikes ORDER BY ikes_k, ikes_n;"
        try:
            res = self.make_dict_request(sql)
            if res:
                return res
        except:
            print('Ошибка чтения из БД')
        return []

    def getEcoDistricts(self):
        sql = "SELECT zona_k, zona_n FROM spr_eco ORDER BY zona_k, zona_n"
        try:
            res = self.make_dict_request(sql)
            if res:
                return res
        except:
            print('Ошибка чтения из БД')
        return []

    def getLocalDictricts(self):
        sql = "SELECT lraj_k, lraj_n FROM spr_loc ORDER BY lraj_k, lraj_n;"
        try:
            res = self.make_dict_request(sql)
            if res:
                return res

        except:
            print('Ошибка чтения из БД')
        return []
