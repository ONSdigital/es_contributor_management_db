import json
import psycopg2
import pandas.io.sql as psql
import pandas
import os


def lambda_handler(event, context):
    usr = os.environ['UserName']
    pas = os.environ['Password']

    try:

        connection = psycopg2.connect(host="", database="es_results_db", user=usr, password=pas)

        querySQL = ("UPDATE es_db_test.Query SET"
                    + "  QueryType = %(querytype)s"
                    + ", GeneralSpecificFlag = %(generalspecificflag)s"
                    + ", IndustryGroup = %(industrygroup)s"
                    + ", LastQueryUpdate = %(lastqueryupdate)s"
                    + ", QueryActive = %(queryactive)s"
                    + ", QueryDescription = %(querydescription)s"
                    + ", QueryStatus = %(querystatus)s"
                    + ", ResultsState = %(resultsstate)s"
                    + ", TargetResolutionDate = %(targetresolutiondate)s"
                    + "  WHERE QueryReference = %(queryreference)s"
                    + ";")
        psql.execute(querySQL, connection, params={"querytype": event["querytype"], "generalspecificflag": event["generalspecificflag"],
                                                   "industrygroup": event["industrygroup"], "lastqueryupdate": event["lastqueryupdate"],
                                                   "queryactive": event["queryactive"], "querydescription": event["querydescription"],
                                                   "querystatus": event["querystatus"], "resultsstate": event["resultsstate"],
                                                   "targetresolutiondate": event["targetresolutiondate"], "queryreference": event["queryreference"]})

        UpdateTasks = event["QueryTasks"]
        for task in UpdateTasks:
            taskSQL = ("UPDATE es_db_test.Query_Task SET"
                       + "  ResponseRequiredBy = %(responserequiredby)s"
                       + ", TaskDescription = %(taskdescription)s"
                       + ", TaskResponsibility = %(taskresponsibility)s"
                       + ", TaskStatus = %(taskstatus)s"
                       + ", NextPlannedAction = %(nextplannedaction)s"
                       + ", WhenActionRequired = %(whenactionrequired)s"
                       + "  WHERE QueryReference = %(queryreference)s"
                       + "  AND   TaskSeqNo = %(taskseqno)s"
                       + ";")
            psql.execute(taskSQL, connection, params={"responserequiredby": task["responserequiredby"], "taskdescription": task["taskdescription"],
                                                      "taskresponsibility": task["taskresponsibility"], "taskstatus": task["taskstatus"],
                                                      "nextplannedaction": task["nextplannedaction"], "whenactionrequired": task["whenactionrequired"],
                                                      "queryreference": task["queryreference"], "taskseqno": task["taskseqno"]})

            UpdateUpdate = task["QueryTaskUpdates"]
            for update in UpdateUpdate:
                updateSQL = ("INSERT INTO es_db_test.Query_Task_Update " +
                             "VALUES (%(taskseqno)s, %(queryreference)s, %(lastupdated)s, %(taskupdatedescription)s, %(updatedby)s)" +
                             "ON CONFLICT (TaskSeqNo,QueryReference,LastUpdated) " +
                             "DO NOTHING;")

                psql.execute(updateSQL, connection, params={"taskseqno": update["taskseqno"], "queryreference": update["queryreference"],
                                                            "lastupdated": update["lastupdated"], "taskupdatedescription": update["taskupdatedescription"],
                                                            "updatedby": update["updatedby"]})
        connection.commit()
        connection.close()
    except:
        return json.loads('{"UpdateData":"Something Went Wrong"}')
    return json.loads('{"UpdateData":"Something Went Well"}')
