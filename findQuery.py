import json
import psycopg2
import pandas.io.sql as psql
import pandas
import os


def lambda_handler(event, context):
    pandas.set_option('display.max_columns', 500)
    usr = ""
    pas = ""
    event = {'QueryReference': '',
             'PeriodQueryRelates': '',
             'QueryType': '',
             'RUReference': '77700000001',
             'SurveyOutputCode': '',
             'QueryStatus': ''}
    search_list = ['QueryReference',
                   'PeriodQueryRelates',
                   'QueryType',
                   'RUReference',
                   'SurveyOutputCode',
                   'QueryStatus']
    all_query_sql = "SELECT * FROM es_db_test.Query WHERE"
    added_query_sql = ""
    for criteria in search_list:
        if event[criteria] == "":
            continue

        if added_query_sql == "":
            added_query_sql += " " + criteria + " = %(" + criteria + ")s"
        else:
            added_query_sql += " AND " + criteria + " = %(" + criteria + ")s"

    if added_query_sql == "":
        added_query_sql = " QueryStatus = 'open'"
    all_query_sql += added_query_sql + ";"
    connection = psycopg2.connect(host="", database="postgres", user=usr, password=pas)
    query = psql.read_sql(all_query_sql, connection, params={'QueryReference': event['QueryReference'], 'PeriodQueryRelates': event['PeriodQueryRelates'], 'QueryType': event['QueryType'], 'RUReference': event['RUReference'], 'SurveyOutputCode': event['SurveyOutputCode'], 'QueryStatus': event['QueryStatus']})
    outJSON = '{"Queries":[ '
    for index, query_row in query.iterrows():
        curr_query = query[query['queryreference'] == query_row['queryreference']]
        if curr_query.empty:
            continue
        Ref = curr_query['queryreference'].iloc[0]
        RU = curr_query['rureference'].iloc[0]
        Period = str(curr_query['periodqueryrelates'].iloc[0])
        Survey = curr_query['surveyoutputcode'].iloc[0]

        try:
            step_exceptions = psql.read_sql("SELECT * FROM es_db_test.Step_Exception WHERE QueryReference = %(Ref)s;", connection, params={'Ref': Ref})
            question_anomaly = psql.read_sql("SELECT * FROM es_db_test.Question_Anomaly WHERE SurveyPeriod = %(Period)s AND RUReference = %(RU)s AND SurveyOutputCode = %(Survey)s;", connection, params={'RU': RU, 'Period': Period, 'Survey': Survey})
            VETs = psql.read_sql("SELECT a.*,b.Description FROM es_db_test.Failed_VET a, es_db_test.VET b WHERE a.SurveyPeriod = %(Period)s AND a.RUReference = %(RU)s AND a.SurveyOutputCode = %(Survey)s AND a.FailedVET = b.VET;", connection, params={'RU': RU, 'Period': Period, 'Survey': Survey})
            query_tasks = psql.read_sql("SELECT * FROM es_db_test.Query_Task WHERE QueryReference = %(Ref)s;", connection, params={'Ref': Ref})
            query_task_updates = psql.read_sql("SELECT * FROM es_db_test.Query_Task_Update WHERE QueryReference = %(Ref)s;", connection, params={'Ref': Ref})
        except:
            return json.loads('{"queryreference":"Error","querytype":"Something Has Gone Wrong"}')
        curr_query = json.dumps(curr_query.to_dict(orient='records'), sort_keys=True, default=str)
        curr_query = curr_query[1:-2]
        outJSON += curr_query + ',"Exceptions":[ '
        for index, step_row in step_exceptions.iterrows():
            row_step = step_row['step']
            curr_step = step_exceptions[(step_exceptions['step'] == row_step) & (step_exceptions['runreference'] == step_row['runreference'])]
            if curr_step.empty:
                continue

            curr_step = json.dumps(curr_step.to_dict(orient='records'), sort_keys=True, default=str)
            curr_step = curr_step[1:-2]
            outJSON += (curr_step + ',"Anomalies":[')
            for index, ano_row in question_anomaly.iterrows():
                curr_ano = question_anomaly[(question_anomaly['step'] == ano_row['step']) & (question_anomaly['questionno'] == ano_row['questionno']) & (question_anomaly['step'] == row_step)]
                if curr_ano.empty:
                    continue

                curr_ano = json.dumps(curr_ano.to_dict(orient='records'), sort_keys=True, default=str)
                curr_ano = curr_ano[1:-2]

                outJSON += curr_ano + ',"FailedVETs":'
                curr_per = VETs[(VETs['step'] == row_step) & (VETs['questionno'] == ano_row['questionno'])]
                curr_per = json.dumps(curr_per.to_dict(orient='records'), sort_keys=True, default=str)
                outJSON += (curr_per + '},')

            outJSON = outJSON[:-1]
            outJSON += ']},'
        outJSON = outJSON[:-1]
        outJSON += '],"QueryTasks":['
        for index, tas_row in query_tasks.iterrows():
            curr_tas = query_tasks[(query_tasks['queryreference'] == tas_row['queryreference']) & (
                    query_tasks['taskseqno'] == tas_row['taskseqno'])]
            if curr_tas.empty:
                continue
            curr_tas = json.dumps(curr_tas.to_dict(orient='records'), sort_keys=True, default=str)
            curr_tas = curr_tas[1:-2]
            outJSON += curr_tas
            outJSON = outJSON + ',"QueryTaskUpdates":'
            curr_up = query_task_updates[(query_task_updates['queryreference'] == tas_row['queryreference']) & (
                    query_task_updates['taskseqno'] == tas_row['taskseqno'])]
            curr_up = json.dumps(curr_up.to_dict(orient='records'), sort_keys=True, default=str)
            outJSON += curr_up + '},'
        outJSON = outJSON[:-1]
        outJSON += ']},'
    connection.close()
    outJSON = outJSON[:-1]
    outJSON += ']}'

    return json.loads(outJSON)


y = lambda_handler('', '')
print(y)
