import psycopg2
import pandas.io.sql as psql


def main():
    #usr = 'postgres'
    #pas = 'mysecretpassword'

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
        "INSERT INTO es_db_test.SSR_Old_Regions (IDBR, SSRReference, RegionName) VALUES ('AA',1,'Northern'), ('BA',1,'Northern'), ('BB',3,'North West'), ('CB',3,'North West'), ('DC',2,'Yorks & Humberside'), ('ED',6,'East Midlands'), ('FE',5,'West Midlands'), ('GF',7,'East Anglia'), ('GG',9,'South East'), ('HH',9,'South East'), ('JG',9,'South East'), ('KJ',10,'South West'), ('WW',11,'Wales'), ('XX',12,'Scotland'), ('YY',0,'Northern Ireland'), ('ZZ',0,'Non-UK');",
        "INSERT INTO es_db_test.GOR_Regions (IDBR, GORReference, RegionName) VALUES ('AA',1,'North East'), ('BA',3,'North West'), ('BB',3,'North West'), ('CB',3,'North West'), ('DC',2,'Yorks & Humberside'), ('ED',6,'East Midlands'), ('FE',5,'West Midlands'), ('GF',7,'Eastern'), ('GG',7,'Eastern'), ('HH',9,'South East'), ('JG',9,'South East'), ('KJ',10,'South West'), ('WW',11,'Wales'), ('XX',12,'Scotland'), ('YY',0,'Northern Ireland'), ('ZZ',0,'Non-UK');",
        "INSERT INTO es_db_test.VET (VET, Description) VALUES (123,'First Validation'), (115,'Other Validation'), (189,'NOT Validation'), (193,'BMI Validation');",
        "INSERT INTO es_db_test.Query_Type (QueryType, Description) VALUES ('Register','Register'), ('Data Cleaning','Data Cleaning'), ('Results Processing','Results Processing'), ('Results QA','Results QA');",
        "INSERT INTO es_db_test.Survey (SurveyOutputCode,SurveyName) VALUES ('066','BMI SG Land'), ('068','BMI Concrete Tiles'), ('071','BMI Slate'), ('073','BMI Blocks'), ('074','BMI Bricks'), ('076','BMI SG Marine');",
        "INSERT INTO es_db_test.Contributor (ParentRUReference, RUReference, HouseNameNo, Street, AdditionalAddressLine, TownCity, County, Country, Postcode, BirthDate, BusinessProfilingTeamCase, Contact, DeathDate, EnforcementFlag, EnforcementStatus, Fax, ContributorName, ProfileInformation, SIC2003, SIC2007, Telephone) VALUES ('770000001','77700000001','999','Letsby Avenue','A','Metropolis','A','A','MP01 11AB','1982-09-17',true,'Officer Dibble',null,false,'Active','01336 216999','Dirty Harry','Do you feel lucky punk',200301,200701,'01336 216999'),('7700000002','77700000002','18','Slingsby Road','A','Pontypool','A','A','NP4 6AD','1999-01-01',true,'Jazzy Jim','2019-03-20',true,'De-active','01662 123989','Jimi Hendrix','Excuse me while I kiss the sky',200302,200702,'01662 123987'),('7700000003','77700000003','22','Avenue Road','A','Trowbridge','A','A','BA22 1ST','1997-02-24',false,'Lord Luke',null,true,'Active','07462 456239','Kate Bush','I just know that something good is gonna happen',200303,200703,'03647 345987'),('7700000004','77700000004','88','Gurner Terrice','A','Melksham','A','A','SW4 6LP','1994-08-21',false,'Duke David',null, true,'Active','01234 654321','Jimmi Page','And it makes me wonder',200304,200704,'01422 456757'),('7700000005','77700000005','4','Pete Place','A','Petersfield','A','A','GU32 1RT','2001-11-3',true,'Baron Binay',null,false,'Active','01999 987990','Paul Simon','I can call you Betty when you call me Al',200305,200705,'03665 083727');",
        "INSERT INTO es_db_test.Survey_Enrolment (RUReference, SurveyOutputCode, NoOfCurrentConsecutivePeriodsOfNonResponse, NoOfPeriodsWithOutstandingQueries, PeriodOfEnrolment) VALUES ('77700000001','066',0,0,'190003'),('77700000001','076',3,1,'190003'),('77700000002','066',1,1,'190003'),('77700000003','066',1,1,'190003'),('77700000004','066',1,1,'190003'),('77700000005','066',0,0,'190003');",
        "INSERT INTO es_db_test.Survey_Period (SurveyPeriod, SurveyOutputCode, ActivePeriod, NoOfResponses, NumberCleared, SampleSize) VALUES ('201712','066',false,10,8,11),('201803','076',false,18,5,20),('201803','066',false,20,20,21);",
        "INSERT INTO es_db_test.Contributor_Survey_Period (RUReference, SurveyOutputCode, SurveyPeriod, AdditionalComments, ContributorComments, LastUpdated, NoOfActiveQueriesInPeriod, NoOfContributorInteractionsInPeriod, PriorityResponseList, ResponseStatus, ShortDescription, WhenStatusLastChanged) VALUES ('77700000001','066','201712','We like this guy','We are very nice people','2015-04-18',2,5,true,'Active','this is a description of us','2018-03-12' ),('77700000001','066','201803','We dont like you people','Bad attitude','2017-03-15',1,2,true,'Sleeping','this is to explain what we are','2019-02-25' ),('77700000001','076','201803','We think there maybe a problem','we have issues','2016-09-10',0,1,false,'Deactive','WHO are you','2019-01-03'),('77700000002','066','201803','We think isnt working for us','we are just a business','2019-05-02',0,0,true,'Active','we arent very good at explaining','2018-02-08'),('77700000003','066','201803','Person on the phone had a nice speaking voice','Call back required','2019-05-02',0,0,true,'Active','we arent very good at explaining','2018-02-08'),('77700000004','066','201803','Feel free to call anytime','May the force be with you','2019-05-02',0,0,true,'Active','we arent very good at explaining','2018-02-08'),('77700000005','066','201803','Hostile takeover imminent','Hostile takeover cancelled','2015-04-18',2,5,true,'Active','We are evil','2018-06-11');",
        "INSERT INTO es_db_test.Contact (ContactReference, HouseNameNo, Street, AdditionalAddressLine, TownCity, County, Country, Postcode, ContactConstraints, ContactEmail, ContactFax, ContactName, ContactOrganisation, ContactPreferences, ContactTelephone) VALUES ('001','999','Letsby Avenue','A','Metropolis','A','A','MP01 11AB','Tied up','dibble@topcat.com','01179 087654','Officer Dibble', 'This one','doesnt like to be called between 6am and 11pm','01179 087655'),('002','18','Slingsby Road','A','Pontypool','A','A','NP4 6AD','Locked up','jazzy@jim.com','01234 045658','Jazzy Jim', 'Many different ones','Likes to be called Jazz','01234 045657'),('003','22','Avenue Road','A','Trowbridge','A','A','BA22 1ST','Fed up','lord@luke.com','07462 456239','Lord Luke', 'Lord of all','not telephone or fax or eamil or sms or post','07462 456238'),('004','88','Gurner Terrice','A','Melksham','A','A','SW4 6LP','available','duke@david.com','07658 823671','Duke David', 'MI5','Please call me any time except pancake day','07658 823672'),('005', '4','Pete Place','A','Petersfield','A','A','GU32 1RT','unavailable','baron@binay.com','01579 082664','Baron Binay', 'All of them','Dont leave me hanging on the telephone','01579 082665');",
        "INSERT INTO es_db_test.Survey_Contact (ContactReference, RUReference, SurveyOutputCode, EffectiveEndDate, EffectiveStartDate) VALUES ('001','77700000001','066',null,'1999-03-09'),('001','77700000001','076',null,'1999-03-09'),('002','77700000002','066',null,'1999-03-09'),('003','77700000003','066',null,'1999-03-09'),('004','77700000004','066',null,'1999-03-09'),('005','77700000005','066',null,'1999-03-09');",
        "INSERT INTO es_db_test.Query (QueryType, RUReference, SurveyOutputCode, PeriodQueryRelates, CurrentPeriod, RunReference, DateTimeRaised, GeneralSpecificFlag, IndustryGroup, LastQueryUpdate, QueryActive, QueryDescription, QueryStatus, RaisedBy, ResultsState, TargetResolutionDate) VALUES ('Register','77700000001','066','201803','201806','1','2019-06-27',false,'Construction','2019-06-27',true,'Company Data Suspisous.','open','Anna McCaffery','NA','2019-06-30'),('Data Cleaning','77700000001','066','201803','201806','1','2019-06-27',false,'Construction','2019-06-27',true,'Company Data Dubious.','open','Anna McCaffery','NA','2019-06-30'),('Results Processing','77700000001','066','201803','201806','1','2019-06-27',true,'Construction','2019-06-27',true,'Disclosable?','open','Anna McCaffery','NA','2019-06-30'),('Results QA','77700000001','066','201803','201806','1','2019-06-27',true,'Construction','2019-06-27',true,'Questionable.','open','Anna McCaffery','NA','2019-06-30'),('Data Cleaning','77700000002','066','201803','201806','1','2019-06-27',true,'Construction','2019-06-27',true,'Company Data Dubious.','open','Anngela Caffa','NA','2019-06-30'),('Results QA','77700000002','066','201803','201806','1','2019-06-27',true,'Construction','2019-06-27',true,'Company Data Dubious.','open','Anngela Caffa','NA','2019-06-30'),('Data Cleaning','77700000003','066','201803','201806','1','2019-06-27',true,'Construction','2019-06-27',true,'Company Data Suspisous.','open','Derick Berlow','NA','2019-06-30'),('Results QA','77700000003','066','201803','201806','1','2019-06-27',true,'Construction','2019-06-27',true,'Company Data Dubious.','open','Derick Berlow','NA','2019-06-30'),('Results QA','77700000004','066','201803','201806','1','2019-06-27',true,'Construction','2019-06-27',true,'Company Data Needs Updating.','open','Martin Merrly','NA','2019-07-01'),('Results QA','77700000005','066','201803','201806','1','2019-06-27',true,'Construction','2019-06-27',true,'Company Data Needs Updating.','open','Willow Wonka','NA','2019-07-01');",
        "INSERT INTO es_db_test.Query_Task (TaskSeqNo, QueryReference, ResponseRequiredBy, TaskDescription, TaskResponsibility, TaskStatus, NextPlannedAction, WhenActionRequired) VALUES (1,1,'2019-06-28','Call Company To See If Figures Incorrect.','Bill Bob','In Progress','Update Data','2018-07-12'),(1,2,'2019-06-28','Call Company To See If Figures Incorrect.','Bill Bob','In Progress','Update Data','2018-07-12'),(1,3,'2019-06-28','Call Company To See If Figures Incorrect.','Bill Bob','In Progress','Update Data','2018-07-12'),(1,4,'2019-06-28','Call Company To See If Figures Incorrect.','Bill Bob','In Progress','Update Data','2018-07-12'),(1,5,'2019-06-28','Discover Why Data Flagged.','Matt Hill','In Progress','Unknown','2018-07-12'),(1,6,'2019-06-28','Discover Why Data Flagged..','Matt Hill','In Progress','Unknown','2018-07-12'),(1,7,'2019-06-28','Call Company To See If Figures Incorrect.','Bella Rina','In Progress','Update Data','2018-07-12'),(1,8,'2019-06-28','Call Company To See If Figures Incorrect.','Bella Rina','In Progress','Update Data','2018-07-12'),(1,9,'2019-06-28','Run Update Proccess.','Lisa Fletcher','In Progress','Unknown','2018-07-12'),(1,10,'2019-06-28','Run Update Proccess.','Lisa Fletcher','In Progress','Unknown','2018-07-12');",
        "INSERT INTO es_db_test.Query_Task_Update (TaskSeqNo, QueryReference, LastUpdated, TaskUpdateDescription, UpdatedBy) VALUES (1,1,'2019-06-27','Contact Not In Office Until Tomorrow','Bill Bob');",
        "INSERT INTO es_db_test.Step_Exception (QueryReference, SurveyPeriod, RunReference, RUReference, Step, SurveyOutputCode, ErrorCode, ErrorDescription) VALUES (2,'201803',1,'77700000001','VETs','066','ERR0867','QuestionNo Failed'),(3,'201803',1,'77700000001','Imputation','066','ERR0453','Imputation Failed'),(5,'201803',1,'77700000002','VETs','066','ERR0867','QuestionNo Failed'),(7,'201803',1,'77700000003','VETs','066','ERR0867','QuestionNo Failed');",
        "INSERT INTO es_db_test.Question_Anomaly (SurveyPeriod, QuestionNo, RunReference, RUReference, Step, SurveyOutputCode, Description) VALUES ('201803','Q605',1,'77700000001','VETs','066','Validation Failure On Concrete Agg.'),('201803','Q608',1,'77700000001','Imputation','066','Imputation Failure On Total Column.'),('201803','Q604',1,'77700000002','VETs','066','Validation Failure On Gravel Building.'),('201803','Q607',1,'77700000003','VETs','066','Validation Failure On Gravel Concrete.');",
        "INSERT INTO es_db_test.Failed_VET (FailedVET, SurveyPeriod, QuestionNo, RunReference, RUReference, Step, SurveyOutputCode)VALUES ('123','201803','Q605',1,'77700000001','VETs','066'),('193','201803','Q608',1,'77700000001','Imputation','066'),('123','201803','Q604',1,'77700000002','VETs','066'),('115','201803','Q607',1,'77700000003','VETs','066');"
        ]

    for insert in insert_list:
        print(insert)
        psql.execute(insert, connection)
    connection.commit()
    connection.close()
    return "Done"


y = main()
print(y)