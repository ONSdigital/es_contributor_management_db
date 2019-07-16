import psycopg2
import pandas.io.sql as psql


def main():

    usr = ''
    pas = ''

    search_list = ['Failed_VET',
                       'Question_Anomaly',
                       'Step_Exception',
                       'Query_Task_Update',
                       'Query_Task',
                       'Query',
                       'Survey_Contact',
                       'Contact',
                       'Contributor_Survey_Period',
                       'Survey_Period',
                       'Survey_Enrolment',
                       'Contributor',
                       'Survey',
                       'Query_Type',
                       'VET',
                       'GOR_Regions',
                       'SSR_Old_Regions']

    connection = psycopg2.connect(host="",
                                  database="es_results_db", user=usr, password=pas)

    #for table in search_list:
    #    print(table)
    #    psql.execute("DELETE FROM es_db_test." + table + ";", connection)
    #connection.commit()

    insert_list = [
        # "INSERT INTO es_db_test.SSR_Old_Regions (IDBR, SSRReference, RegionName) VALUES ('AA',1,'Northern'), ('BA',1,'Northern'), ('BB',3,'North West'), ('CB',3,'North West'), ('DC',2,'Yorks & Humberside'), ('ED',6,'East Midlands'), ('FE',5,'West Midlands'), ('GF',7,'East Anglia'), ('GG',9,'South East'), ('HH',9,'South East'), ('JG',9,'South East'), ('KJ',10,'South West'), ('WW',11,'Wales'), ('XX',12,'Scotland'), ('YY',0,'Northern Ireland'), ('ZZ',0,'Non-UK');",
        # "INSERT INTO es_db_test.GOR_Regions (IDBR, GORReference, RegionName) VALUES ('AA',1,'North East'), ('BA',3,'North West'), ('BB',3,'North West'), ('CB',3,'North West'), ('DC',2,'Yorks & Humberside'), ('ED',6,'East Midlands'), ('FE',5,'West Midlands'), ('GF',7,'Eastern'), ('GG',7,'Eastern'), ('HH',9,'South East'), ('JG',9,'South East'), ('KJ',10,'South West'), ('WW',11,'Wales'), ('XX',12,'Scotland'), ('YY',0,'Northern Ireland'), ('ZZ',0,'Non-UK');",
        "INSERT INTO es_db_test.VET (VET, Description) VALUES (1,'Value Present'), (2,'Question v Derived Question');",
        "INSERT INTO es_db_test.Query_Type (QueryType, Description) VALUES ('Register','Queries raised to manage register change requests'), ('Data Cleaning','Queries raised as a consequence of validation and editing anomalies'), ('Results Processing','Queries raised as a conseqence of aggregated processing anomalies'), ('Results QA','Queries raised as a consequence of QA on aggregated processing');",
        "INSERT INTO es_db_test.Survey (SurveyOutputCode,SurveyName) VALUES ('066','Sand & Gravel (Land Won)'), ('068','BMI Concrete Tiles'), ('071','BMI Slate'), ('073','BMI Blocks'), ('074','BMI Bricks'), ('076','Sand & Gravel (Marine Dredged)');",
        "INSERT INTO es_db_test.Contributor (ParentRUReference, RUReference, HouseNameNo, Street, AdditionalAddressLine, TownCity, County, Country, Postcode, BirthDate, BusinessProfilingTeamCase, Contact, DeathDate, EnforcementFlag, EnforcementStatus, Fax, ContributorName, ProfileInformation, SIC2003, SIC2007, Telephone) VALUES (null,'77700000001','1','Somewhere Street',null,'Newport','Newport','Wales','NP10 AXG',null,false,'Di Jones',null,null,null,null,'Sandy Banks Ltd','Life''s a beach',8120,8120,null),(null,'77700000002','28','SomewhereElse Rd',null,'Cardiff','Cardiff','Wales','CF10 1AA',null,false,'Di Jones Bach',null,null,null,null,'Rock Quarry & Son','Expecting second son to be added to business',23630,23630,null),(null,'77700000003','7','HappyDays',null,'Cardiff','Cardiff','Wales','CF3 9HH',null,false,'Will.I.Ams',null,null,null,null,'Rolling Stones & Co','Gathers no moss',23630,23630,null),(null,'77700000004','36','Haol',null,'Casnewydd','Casnewydd','Cymru','NP35 8ER',null,true,'Di Jones 3',null,null,null,null,'Newport Mining Inc.',null,23630,23630,null),(null,'77700000005','5','No Longer There Street',null,'Newport','Newport','Wales','NP1 0OO',null,true,'Di Jones Sequel',null,null,null,null,'Gwent Gravel Ltd',null,23630,23630,null),(null,'77700000006','412','Gravel Drive',null,'Newport','Newport','Wales','NP11 9PT',null,false,'Di Jones 2',null,null,null,null,'Anything Sandy Ltd',null,8120,8120,null);",
        "INSERT INTO es_db_test.Survey_Enrolment (RUReference, SurveyOutputCode, NoOfCurrentConsecutivePeriodsOfNonResponse, NoOfPeriodsWithOutstandingQueries, PeriodOfEnrolment) VALUES ('77700000001','066',0,null,'201701'),('77700000001','076',0,null,'201801'),('77700000002','066',0,null,'201801'),('77700000002','076',0,null,'201801'),('77700000003','066',0,null,'201801'),('77700000004','066',0,null,'201801'),('77700000005','066',0,null,'201801'),('77700000006','066',0,null,'201802');",
        "INSERT INTO es_db_test.Survey_Period (SurveyPeriod, SurveyOutputCode, ActivePeriod, NoOfResponses, NumberCleared, NumberClearedFirstTime, SampleSize) VALUES ('201712','066',false,0,0,0,20),('201803','066',true,0,0,0,20),('201803','076',true,0,0,0,20),('201801','076',false,0,0,0,20),('201802','076',true,0,0,0,20),('201801','066',false,0,0,0,20);",
        "INSERT INTO es_db_test.Contributor_Survey_Period (RUReference, SurveyOutputCode, SurveyPeriod, AdditionalComments, ContributorComments, LastUpdated, NoOfActiveQueriesInPeriod, NoOfContributorInteractionsInPeriod, PriorityResponseList, ResponseStatus, ShortDescription, WhenStatusLastChanged) VALUES ('77700000001','066','201712',null,'',null,null,null,null,null,null,null),('77700000001','066','201803',null,'',null,null,null,null,null,null,null),('77700000001','076','201803',null,'',null,null,null,null,null,null,null),('77700000002','066','201803',null,'',null,null,null,null,null,null,null),('77700000002','076','201801',null,'',null,null,null,null,null,null,null),('77700000003','066','201803',null,'',null,null,null,null,null,null,null),('77700000004','066','201803',null,'',null,null,null,null,null,null,null),('77700000005','066','201803',null,'',null,null,null,null,null,null,null),('77700000001','066','201801',null,'',null,null,null,null,null,null,null),('77700000001','076','201802',null,'',null,null,null,null,null,null,null),('77700000002','076','201803',null,'',null,null,null,null,null,null,null),('77700000006','066','201803',null,'',null,null,null,null,null,null,null);",
        "INSERT INTO es_db_test.Contact (ContactReference, HouseNameNo, Street, AdditionalAddressLine, TownCity, County, Country, Postcode, ContactConstraints, ContactEmail, ContactFax, ContactName, ContactOrganisation, ContactPreferences, ContactTelephone) VALUES ('SG3658190','1','Somewhere Street',null,'Newport','Newport','Wales','NP10 AXG',null,null,null,'Mr B Castle', 'Site Office Sandy Banks Ltd','Call between 10am and 4pm Mon-Fri','01633 999999'),('SG3663572','23','Wet Cement Walk',null,'Llamberis','Gwynedd','Wales','SN2 3HI','Tues Non-Working Day',null,null,'Mr Flint', 'Special Slate Agent','e-mail only',null);",
        "INSERT INTO es_db_test.Survey_Contact (ContactReference, RUReference, SurveyOutputCode, EffectiveEndDate, EffectiveStartDate) VALUES ('SG3658190','77700000001','066',null,'2017-10-01'),('SG3658190','77700000001','076',null,'2018-01-06'),('SG3663572','77700000002','066',null,'2018-01-06'),('SG3663572','77700000002','076',null,'2018-01-06'),('SG3663572','77700000003','066',null,'2018-01-06'),('SG3663572','77700000004','066',null,'2018-01-06'),('SG3663572','77700000005','066',null,'2018-01-06'),('SG3663572','77700000006','066',null,'2018-01-06');",
        "INSERT INTO es_db_test.Query (QueryType, RUReference, SurveyOutputCode, PeriodQueryRelates, CurrentPeriod, RunReference, DateTimeRaised, GeneralSpecificFlag, IndustryGroup, LastQueryUpdate, QueryActive, QueryDescription, QueryStatus, RaisedBy, ResultsState, TargetResolutionDate) VALUES ('Register','77700000001','066','201712','201803','1','2017-02-08',true,null,'2017-02-12',false,'Change of HO Contact Details.','Closed - Resolved','BDOD','Final',null),('Data Cleaning','77700000001','076','201802','201803','1',null,false,null,'2018-02-21',false,'Validation Anomalies.','Closed - Resolved','BDOD','Final',null),('Results Processing','77700000001','066','201801','201803','1',null,false,null,null,true,'Imputation Anomalies.','Closed - Resolved','BDOD','Provisional',null),('Data Cleaning','77700000001','066','201801','201803','3',null,false,null,null,true,'Validation Anomalies.','Closed - Resolved','BDOD','Final',null),('Register','77700000002','066','201803','201803','1',null,true,null,null,true,'Move from  SEFT to Paper.','Open','BDOD',null,null),('Data Cleaning','77700000002','076','201803','201803','1',null,false,null,null,true,'Validation Anomalies.','Open','BDOD',null,null);",
        "INSERT INTO es_db_test.Query_Task (TaskSeqNo, QueryReference, ResponseRequiredBy, TaskDescription, TaskResponsibility, TaskStatus, NextPlannedAction, WhenActionRequired) VALUES (1,1,null,'Manual Data Investigation','BRO',null,null,null),(1,2,null,'Manual Data Investigation','BDOD BSO',null,null,null),(1,3,null,'Manual Data Investigation','RAP',null,null,null),(1,4,null,'Manual Data Investigation','BDOD BSO',null,null,null),(1,5,null,'Manual Data Investigation','BRO',null,null,null),(1,6,null,'Manual Data Investigation','BDOD BSO',null,null,null);",
        # "INSERT INTO es_db_test.Query_Task_Update (TaskSeqNo, QueryReference, LastUpdated, TaskUpdateDescription, UpdatedBy) VALUES ();",
        "INSERT INTO es_db_test.Step_Exception (QueryReference, SurveyPeriod, RunReference, RUReference, Step, SurveyOutputCode, ErrorCode, ErrorDescription) VALUES (1,'201712',1,'77700000001','VETs','066','ERR0867','QuestionNo Failed'),(2,'201802',1,'77700000001','VETs','076','ERR0867','QuestionNo Failed'),(3,'201801',1,'77700000001','VETs','066','ERR0867','QuestionNo Failed'),(4,'201801',3,'77700000001','VETs','066','ERR0867','QuestionNo Failed'),(5,'201803',1,'77700000002','VETs','066','ERR0867','QuestionNo Failed'),(6,'201803',1,'77700000002','VETs','076','ERR0867','QuestionNo Failed');",
        "INSERT INTO es_db_test.Question_Anomaly (SurveyPeriod, RunReference, RUReference, Step, SurveyOutputCode, QuestionNo, Description) VALUES ('201712',1,'77700000001','VETs','066','Q601','Asphalting Sand'),('201802',1,'77700000001','VETs','076','Q700','Gritty Sand'),('201801',1,'77700000001','VETs','066','Q602','Building Soft Sand'),('201801',3,'77700000001','VETs','066','Q601','Asphalting Sand'),('201803',1,'77700000002','VETs','066','Q602','Building Soft Sand'),('201803',1,'77700000002','VETs','076','Q701','Fine Gravel');",
        "INSERT INTO es_db_test.Failed_VET (SurveyPeriod, RunReference, RUReference, Step, SurveyOutputCode, QuestionNo, FailedVET) VALUES ('201712',1,'77700000001','VETs','066','Q601',2),('201802',1,'77700000001','VETs','076','Q700',1),('201801',1,'77700000001','VETs','066','Q602',2),('201801',3,'77700000001','VETs','066','Q601',2),('201803',1,'77700000002','VETs','066','Q602',1),('201803',1,'77700000002','VETs','076','Q701',1);"
    ]

    for insert in insert_list:
        print(insert)
        psql.execute(insert, connection)
    connection.commit()
    connection.close()
    return "Done"


y = main()
print(y)