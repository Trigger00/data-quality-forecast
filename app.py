import sys
import time
import argparse
import json
import psycopg2
import requests
import numpy as np
from scipy.stats import t
import pandas as pd
from sklearn.preprocessing import StandardScaler
from utils.config import *

LIST_OF_ISSES = []  # Contains the list of issues detected
ISSUES_DETECTED = 0
DB_CONNECTION = ''  # Global db connection cursor


def open_db_connection():
    """Comments here."""
    global DB_CONNECTION
    try:
        con = psycopg2.connect(host=DB_HOST_PROD,
                               port=DB_PORT,
                               dbname=DB_NAME,
                               user=DB_USER,
                               password=str(DB_PASSWORD))

        con.autocommit = True
        DB_CONNECTION = con.cursor()
    except:
        print('Unable to connect!{}'.format(psycopg2.InternalError))
        sys.exit(1)


def create_log_into_newrelic(tablename, columnname, message, aniocampana, query, country, total_records, count_issues, issues_percentage, issuetype, createjiraticket="No"):
    """Comments here."""

    array_data = {
        "table_name": tablename,
        "column_name": columnname,
        "db_user": DB_USER,
        "create_jira_ticket": createjiraticket,
        "message": message,
        "campaign_year": aniocampana,
        "country": country,
        "query": query,
        "records_volume": total_records,
        "records_with_issues": count_issues,
        "accuracy": 100-issues_percentage,
        "issues_percentage": issues_percentage,
        "issue_type": issuetype
    }

    data = {
        "Masivo": {
            "DataQualityValidator": array_data
        }
    }

    headers = {"Api-Key": NR_API_KEY}

    LIST_OF_ISSES.append(array_data)

    requests.post(url=NR_API_ENDPOINT, json=data, headers=headers)


class CheckDataBaseIssues:
    """Comments here."""

    def __init__(self, anio_campana=0):

        self.aniocampana="202215"

        #if len(anio_campana) == 2:  # validar si se encuentra el parametro de año de campaña
        #    self.aniocampana = anio_campana[1]
        #else:
        #   self.aniocampana = '0'

    def validate_issues(self):
        """Comments here."""
        t1_start = 0
        t2_end = 0
        t1_start = time.time()

        # validar si el año de campaña es valido/tiene la longitud de 6 caracteres.
        #if len(self.aniocampana) == 6:

        self.__run_data_rules()
        t2_end = time.time()

        print("Proceso Terminado, Problemas encontrados: "+str(ISSUES_DETECTED)+", Tiempo de ejecución: " +
                  str(t2_end-t1_start) + " Segundos")

        #else:
        #    print(
        #        "Error: The following arguments are required: aniocampana")

    def __parse_detection_query(self, rule_item, excluded_countries):

        select, join, groupby, child_conditions = "", "", "", ""

        where = " WHERE "+rule_item['Table'] + \
            ".aniocampana="+self.aniocampana

        for col in rule_item['Join']:
            join = join+" "+col['Value']

        for col in rule_item['Where']:
            where = where + " " + col['ParentFilter']
            child_conditions = child_conditions+col['CustomFilter']

        where = where + " AND " + \
            rule_item['Table']+".codpais NOT IN ("+excluded_countries+")"

        df_columns = []
        df_columns.clear()
        for col in rule_item['GroupBy']:
            groupby = groupby+rule_item['Table']+"."+col['Column']
            df_columns.append(str(col['Column']))
            if col != rule_item['GroupBy'][len(rule_item['GroupBy']) - 1]:
                groupby = groupby + ","

                # Select clause
        select = "SELECT "+groupby+", COUNT (*) as Results FROM " + \
            rule_item['Table']+" WITH (NOLOCK)"
        groupby = " GROUP BY "+groupby

        query_without_rule = select+join+where+groupby
        query_with_rule = select+join+where+child_conditions+groupby

        return df_columns, query_without_rule, query_with_rule

    def __parse_evaluation_query(self, rule_item, excluded_countries):
        groupby_columns = rule_item['GroupBy'].split(',')

        default_column, where = "aniocampana", ""

        query = "SELECT "+default_column+","

        for grouped_columns in groupby_columns:
            query = query+grouped_columns+", "

        ci_columns = rule_item['Columns'].split(',')

        for fixed_columns in ci_columns:
            query = query + \
                rule_item['ColumnFunction']+" as Results"
            if fixed_columns != ci_columns[len(ci_columns) - 1]:
                query = query+", "

        for col in rule_item['Where']:
            where = where + " " + col['Value']

        where = where + " AND " + \
            rule_item['Table']+".codpais NOT IN ("+excluded_countries+")"

        query = query + " FROM "+rule_item['Table'] + " WITH (NOLOCK) WHERE aniocampana<="+str(self.aniocampana) + \
            where + " GROUP BY "+default_column+"," + \
            rule_item['GroupBy'] + " ORDER BY aniocampana"

        all_columns = [default_column]+groupby_columns+['Results']

        return query, all_columns, groupby_columns

    def __get_data_frame_from_db(self, query, columns):
        DB_CONNECTION.execute(query)

        return pd.DataFrame(DB_CONNECTION.fetchall(), columns=columns)

    def __get_excluded_countries(self, value):
        excludedcountries = ""
        for country_ex in value:
            excludedcountries = excludedcountries+"'"+country_ex+"'"
            if country_ex != value[len(value) - 1]:
                excludedcountries = excludedcountries + ","
        return excludedcountries

    def __get_message_to_newrelic(self, percentage_issues, total_records, records_with_issues, issue_type, fields_combination, additionalmessage=""):

        message = "Error: " + issue_type + \
            " para la siguiente combinación de campos: "+fields_combination + "Porcentaje de errores detectados: " + \
            percentage_issues+", " + "Total de Datos: " + \
            total_records+", "+"Datos con errores: " + \
            records_with_issues

        if additionalmessage:
            message = message+", "+additionalmessage

        return message

    # function that returns [1=the interval confidence must be reviewed] or [0=nothing weird was found]
    # it calculates the interval confidence of a dataset

    def __confidence_interval_calculator(self, data_frame, column_name, multiplier_factor):

        data_frame[column_name] = StandardScaler().fit_transform(data_frame[[column_name]])
        m = data_frame[column_name].mean()
        s = data_frame[column_name].std()
        dof = len(data_frame)-1
        confidence = 0.95
        t_crit = (np.abs(t.ppf((1-confidence)/2,dof)))*multiplier_factor
        (m-s*t_crit/np.sqrt(len(data_frame)), m+s*t_crit/np.sqrt(len(data_frame)))
        data_frame['ci_results'] = np.where((data_frame[column_name]> m+s*t_crit/np.sqrt(len(data_frame))) | (data_frame[column_name]< (m-s*t_crit/np.sqrt(len(data_frame)))) , 1, 0)
        return data_frame['ci_results'].iloc[-1]


    def __run_data_rules(self):  # validate the fields with zero o null issues
        print('Verificando la calidad de los datos del Forecast Masivo')

        open_db_connection()

        rules_json = open('rules.json')
        data = json.load(rules_json)
        global ISSUES_DETECTED
        excluded_countries = self.__get_excluded_countries(
            data['ExcludedCountries'])

        for rule_item in data['RulesEngine']:

            if (rule_item['ValidationType'] == "Detection"):
                parsed_query = self.__parse_detection_query(
                    rule_item, excluded_countries)

                df_columns = parsed_query[0]
                df_merged_cols_index = df_columns.copy()
                df_columns.append('Results')

                query_without_rule = parsed_query[1]
                query_with_rule = parsed_query[2]

                data_with_rule = self.__get_data_frame_from_db(
                    query_without_rule, df_columns)
                data_without_rule = self.__get_data_frame_from_db(
                    query_with_rule, df_columns)

                df_merged = pd.merge(
                    data_with_rule, data_without_rule, how="inner", on=df_merged_cols_index)

                df_merged['Results_p'] = round(
                    (df_merged['Results_y'] / df_merged['Results_x'])*100, 3)

                for df_merged_row in df_merged.iterrows():
                    fieldswithissues = ""

                    for _row in df_merged_cols_index:
                        fieldswithissues = f'{_row}' + " = " + \
                            df_merged_row[1][f'{_row}']+", "

                    message = self.__get_message_to_newrelic(str(df_merged_row[1].Results_p), str(df_merged_row[1].Results_x),
                                                                str(df_merged_row[1].Results_y), rule_item['IssueType'], fieldswithissues)

                    ISSUES_DETECTED = ISSUES_DETECTED+1

                    # enviar a new relic
                    create_log_into_newrelic(rule_item['Table'], rule_item['Column'], message, self.aniocampana,
                                                query_with_rule, df_merged_row[1].codpais, df_merged_row[1].Results_x, df_merged_row[1].Results_y, df_merged_row[1].Results_p, rule_item['IssueType'], "Yes")

            if (rule_item['ValidationType'] == "Evaluation"):

                parsed_query = self.__parse_evaluation_query(
                    rule_item, excluded_countries)

                query = parsed_query[0]
                all_columns = parsed_query[1]
                groupby_columns = parsed_query[2]

                data_frame = self.__get_data_frame_from_db(
                    query, all_columns)

                grouped_data_frame = data_frame.groupby(
                    groupby_columns)[all_columns]

                for df_filter in grouped_data_frame:

                    df_filter = df_filter[1]
                    #print(st.t.interval(confidence=0.95, df=len(df_filter['Results'])-1, loc=np.mean(df_filter['Results']), scale=st.sem(df_filter['Results'])))

                    fieldswithissues = ""

                    if self.__confidence_interval_calculator(df_filter, 'Results', CI_MULTIPLIER_FACTOR_HIGH) == 0:
                        if self.__confidence_interval_calculator(df_filter, 'Results', CI_MULTIPLIER_FACTOR_MID) == 1:

                            df_values_error_matching = df_filter.iloc[-1]

                            if (df_values_error_matching.aniocampana == self.aniocampana):
                                for x in groupby_columns:
                                    fieldswithissues = fieldswithissues + x + "=" + \
                                        str(
                                            df_values_error_matching[f'{x}']) + ","

                                message = self.__get_message_to_newrelic(str(round((1/len(df_filter))*100, 3)), str(len(df_filter)),
                                                                            str(1), rule_item['IssueType'], fieldswithissues, "Nivel MEDIO")

                                country=df_values_error_matching.codpais
                                tablename=rule_item['Table']
                                column_name=rule_item['Columns']
                                results_x=0
                                results_y=0
                                results_p=0
                                ticket_to_jira="No"

                                create_log_into_newrelic(tablename, column_name, message, self.aniocampana,
                                                query, country, results_x, results_y, results_p, rule_item['IssueType'], ticket_to_jira)

                                ISSUES_DETECTED = ISSUES_DETECTED+1

                    else:

                        df_values_error_matching = df_filter.iloc[-1]

                        if (df_values_error_matching.aniocampana == self.aniocampana):
                            for x in groupby_columns:
                                fieldswithissues = fieldswithissues + x + "=" + \
                                    str(
                                        df_values_error_matching[f'{x}']) + ","

                            message = self.__get_message_to_newrelic(str(round((1/len(df_filter))*100, 3)), str(len(df_filter)),
                                                                        str(1), rule_item['IssueType'], fieldswithissues, "Nivel ALTO")

                            country=df_values_error_matching.codpais
                            tablename=rule_item['Table']
                            column_name=rule_item['Columns']
                            results_x=0
                            results_y=0
                            results_p=0
                            ticket_to_jira="Yes"

                            create_log_into_newrelic(tablename, column_name, message, self.aniocampana,
                                                query, country, results_x, results_y, results_p, rule_item['IssueType'], ticket_to_jira)

                            ISSUES_DETECTED = ISSUES_DETECTED+1

            rules_json.close()


parser = argparse.ArgumentParser()
#parser.add_argument("aniocampana", help="Campaign Year", type=int)
args = parser.parse_args()

# # Instantiation of the class
start_checking_db_issues = CheckDataBaseIssues(sys.argv)
start_checking_db_issues.validate_issues()

