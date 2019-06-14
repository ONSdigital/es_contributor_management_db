delete from es_db_test.Failed_VET;
delete from es_db_test.Question_Anomaly;
delete from es_db_test.VET;
delete from es_db_test.Step_Exception;
delete from es_db_test.QueryTaskUpdate;
delete from es_db_test.Query_Task;
delete from es_db_test.Link_Query;
delete from es_db_test.Query;
delete from es_db_test.Contact;
delete from es_db_test.Survey_Contact;
delete from es_db_test.Contributor_Survey_Period;
delete from es_db_test.Survey_Enrolment;
delete from es_db_test.Contributor;
delete from es_db_test.Survey_Period;
delete from es_db_test.Survey;
delete from es_db_test.Query_Type;

Insert Into es_db_test.Contributor
(
    ParentRUReference,
    RUReference,
    HouseNameNo,
    Street,
    AdditionalAddressLine,
    TownCity,
    County,
    Country,
    Postcode,
    BirthDate,
    BusinessProfilingTeamCase,
    Contact,
    DeathDate,
    EnforcementFlag,
    EnforcementStatus,
    Fax,
    ContributorName,
    ProfileInformation,
    SIC2003,
    SIC2007,
    Telephone
)
Values
    ('500000123','ABC00001','999','Letsby Avenue','none' ,'Metropolis','Metro','India', 'MP01 11AB', '1982-09-17', true, 'Officer Dibble','3018-05-01',false,'Active',
                '01336 216999', 'Police Academy', 'This is some comments about the business', 200301,200701, '01336 216999'),
    ('500000456','ABC00002','18','Slingsby Road','none','Pontypool','Gwent','Wales','NP4 6AD','1999-01-01', true, 'King Mike', '2019-03-20', true,'De-active',
                '01662 123989', 'Alice Cooper', 'This is me making some random comments', 200302,200702, '01662 123987'),
    ('500000789','ABC00003','22','Avenue Road','stallards','Trowbridge','Wiltshire','England','BA22 1ST', '1997-02-24',false, 'Lord Luke', '3018-05-01', true,'Active',
                '07462 456239', 'Bon Scott', 'Im on a highway to hell', 200303,200703, '03647 345987'),
    ('500000321','ABC00004','88','Gurner Terrice','none','Melksham','aha','Sudan', 'SW4 6LP', '1994-08-21',false, 'Duke David', '3018-05-01', true,'Active',
                '01234 654321', 'Jimmi Page', 'Its been a long time since I Rock and Roll', 200304,200704, '01422 456757'),
    ('500000876','ABC00005','4','Pete Place', 'peaty', 'Petersfield','Hants','England', 'GU32 1RT', '2001-11-3', true, 'Baron Binay', '3018-05-01',false,'Active',
                '01999 987990', 'Paul Simon', 'You can call me Betty Betty when you call me Al', 200305,200705, '03665 083727');


Insert Into es_db_test.Contact
(
    ContactReference,
    HouseNameNo,
    Street,
    AdditionalAddressLine,
    TownCity,
    County,
    Country,
    Postcode,
    ContactConstraints,
    ContactEmail,
    ContactFax,
    ContactName,
    ContactOrganisation,
    ContactPreferences,
    ContactTelephone
)
Values
    ('001','999','Letsby Avenue','none' ,'Metropolis','Metro','India', 'MP01 11AB','Tied up','dibble@topcat.com','01179 087654',
                'Officer Dibble', 'This one','doesnt like to be called between 1 and 12','01179 087655'),
    ('002','18','Slingsby Road','none','Pontypool','Gwent','Wales','NP4 6AD','Locked up','king@mike.com','01234 045658',
                'King Mike', 'Many different secret ones','Should only be called King Mike','01234 045657'),
    ('003','22','Avenue Road','stallards','Trowbridge','Wiltshire','England','BA22 1ST','Fed up','lord@luke.com','07462 456239',
                'Lord Luke', 'Lord of all','not telephone or fax or eamil or sms or post','07462 456238'),
    ('004','88','Gurner Terrice','none','Melksham','aha','Sudan', 'SW4 6LP','available','duke@david.com','07658 823671',
                'Duke David', 'MI5','Please call me any time and all the time','07658 823672'),
    ('005', '4','Pete Place', 'peaty', 'Petersfield','Hants','England', 'GU32 1RT','unavailable','baron@binay.com','01579 082664',
                'Baron Binay', 'All of them','Must be really polite or else','01579 082665');


Insert Into es_db_test.Survey
(
    SurveyOutputCode,
    SurveyName
)
Values
    ('066','BMI SG Land'),
    ('068','BMI Concrete Tiles'),
    ('071','BMI Slate'),
    ('073','BMI Blocks'),
    ('074','BMI Bricks'),
    ('076','BMI SG Marine');


Insert Into es_db_test.Survey_Enrolment
(
    RUReference,
    SurveyOutputCode,
    NoOfCurrentConsecutivePeriodsOfNonResponse,
    NoOfPeriodsWithOutstandingQueries,
    PeriodOfEnrolment
)
Values
    ('ABC00001','066',0,0,'1900-03-21'),
    ('ABC00002','068',3,1,'1900-03-12'),
    ('ABC00003','071',1,1,'1900-03-05'),
    ('ABC00004','073',1,1,'1900-03-01'),
    ('ABC00005','073',0,0,'1900-03-30');


Insert Into es_db_test.Survey_Contact
(
    ContactReference,
    RUReference,
    SurveyOutputCode,
    EffectiveEndDate,
    EffectiveStartDate

)
Values
    ('001','ABC00001','066','1900-01-01','1999-03-09'),
    ('002','ABC00002','068','1900-01-01','1999-03-09'),
    ('003','ABC00003','071','1900-01-01','1999-03-09'),
    ('004','ABC00004','073','1900-01-01','1999-03-09'),
    ('005','ABC00005','073','1900-01-01','1999-03-09');


Insert Into es_db_test.Survey_Period
(
    SurveyPeriod,
    SurveyOutputCode,
    ActivePeriod,
    NoOfResponses,
    NumberCleared,
    SampleSize
)
Values
    ('201701','066',false,10,8,11),
    ('201801','068',false,18,5,20),
    ('201803','071',true,23,15,23),
    ('201309','073',false,13,11,20),
    ('201701','073',false,20,20,21);

Insert Into es_db_test.Contributor_Survey_Period
(
    RunReference,
    RUReference,
    SurveyOutputCode,
    SurveyPeriod,
    AdditionalComments,
    ContributorComments,
    LastUpdated,
    NoOfActiveQueriesInPeriod,
    NoOfContributorInteractionsInPeriod,
    PriorityResponseList,
    ResponseStatus,
    ShortDescription,
    WhenStatusLastChanged
)
Values
    (1,'ABC00001','066','201701','We like this guy','we are nice honest','2015-04-18',2,5,
                'this then this','Active','this is a description of us','2018-03-12' ),
    (1,'ABC00002','068','201801','We dont like this guy','we are nice maybe','2017-03-15',1,2,
                'this then that','Sleeping','this is to explain what we are','2019-02-25' ),
    (1,'ABC00003','071','201803','We think there maybe a problem','we have issues','2016-09-10',0,1,
                'that then this','Deactive','WHO are you','2019-01-03'),
    (1,'ABC00004','073','201309','We think this could work','we are just a business','2019-05-02',0,0,
                'a then b then c','Active','we arent very good at explaining','2018-02-08'),
    (1,'ABC00005','073','201701','This is my business now Dave','I do not think so','2015-04-18',2,5,
                'to take over you will first need to...','Active','We are evil','2018-06-11');


insert into es_db_test.Query_Type
(
    QueryType,
    Description
)
values
    ('Register','First registration of the raw data'),
    ('Data Cleaning','Lots of soap and a good scrub'),
    ('Results Processing','Well process the results'),
    ('Resut QA','QA contractor mess about with the data');


insert into es_db_test.Query
(

   QueryReference,
   QueryType,
   RUReference,
   SurveyOutputCode,
   PeriodQueryRelates,
   CurrentPeriod,
   RunReference,
   DateTimeRaised,
   GeneralSpecificFlag,
   IndustryGroup,
   LastQueryUpdate,
   QueryActive,
   QueryDescription,
   QueryStatus,
   RaisedBy,
   ResultsState,
   TargetResolutionDate

)

values
    ('Q0000001','Results Processing','ABC00001','066','201701','201810',1,'2019-03-30',true,'Construction','2019-04-10',true,
        'I dont like this table','open','JLS','incomplete','2019-09-01'),
    ('Q0000002','Results Processing','ABC00001','066','201701','201906',1,'2019-03-30',true,'Construction','2019-04-10',false,
        'time for bed said zebedee','open','AHD','incomplete','2019-10-02'),
    ('Q0000003','Results Processing','ABC00002','068','201801','201912',1,'2019-02-28',false,'Construction','2019-04-10',true,
        'dirty deeds done dirt cheap','open','LSD','incomplete','2019-09-03'),
    ('Q0000004','Results Processing','ABC00003','071','201803','201711',1,'2019-08-30',false,'Construction','2016-04-10',true,
        'Im singing in the rain','open','JLS','complete','2019-09-10'),
    ('Q0000005','Results Processing','ABC00003','071','201803','201809',1,'2019-03-30',true,'Construction','2018-02-22',false,
        'Someone get me out of here','open','JPL','incomplete','2019-07-20');


insert into es_db_test.Query_Task
(
    TaskSeqNo,
    QueryReference,
    ResponseRequiredBy,
    TaskDescription,
    TaskResponsibility,
    TaskStatus
)

values
    (1,'Q0000001','2019-2-09','I didnt like the figures','Micky Mouses responsibility now','Active'),
    (2,'Q0000001','2019-09-09','Failed a particular VET need to check','Micky Mouses responsibility now','Ongoing'),
    (3,'Q0000003','2020-01-01','The data doesnt smell right','Daffy Ducks responsibility now','unresolved'),
    (4,'Q0000004','2019-11-10','I didnt like the figures','Billy Pilgrims responsibility now','deactive unresolved'),
    (5,'Q0000005','2019-06-22','I didnt like the figures','Frodo Baggins responsibility now','error');

insert into es_db_test.Query_Task_Update
(
    TaskSeqNo,
    QueryReference,
    LastUpdated,
    TaskUpdateDescription,
    UpdatedBy
)
values
     (1,'Q0000001','2019-02-22','Changed the figures I didnt like pass onto Micky','Alice Cooper'),
     (2,'Q0000001','2019-12-25','Looked at the VETS it was just a missing value','Shakin Stephens'),
     (3,'Q0000003','2019-05-09','added deodrant pass to Steve','Sea Sick Steve'),
     (4,'Q0000004','2019-05-09','I talked to them and we worked it out','Lynyrd Skynyrd'),
     (5,'Q0000005','2019-05-09','I still dont like them but we have agreed to work together','Sergeant Pepper');

insert into es_db_test.Step_Exception
(
    QueryReference,
    SurveyPeriod,
    RunReference,
    RUReference,
    Step,
    SurveyOutputCode,
    ErrorCode,
    ErrorDescription
)
values
    ('Q0000001','201701',1,'ABC00001','Cleaning','066','err00145','The data is unclean'),
    ('Q0000002','201701',1,'ABC00001','Smelling','066','err28476','The data smells bad'),
    ('Q0000003','201801',1,'ABC00002','Inventing','068','err3958','The data should be made up'),
    ('Q0000004','201803',1,'ABC00003','discovering','071','err00045','Cant find the data'),
    ('Q0000005','201803',1,'ABC00003','loitering','071','err00467','The data seems to be hanging around');

insert into es_db_test.VET
(
    VET,
    Description
)
values
    (123,'First Validation'),
    (115,'Other Validation'),
    (189,'NOT Validation'),
    (193,'BMI Validation');

insert into es_db_test.Question_Anomaly
    (
    SurveyPeriod,
    QuestionNo,
    RunReference,
    RUReference,
    Step,
    SurveyOutputCode,
    Description
)
values
    ('201701',1,1,'ABC00001','Cleaning','066','Was told this data was filthy'),
    ('201701',2,1,'ABC00001','Smelling','066','Was told this data smells'),
    ('201801',3,1,'ABC00002','Inventing','068','Told the contributor had lied'),
    ('201803',4,1,'ABC00003','discovering','071','Was told the data is missing'),
    ('201803',5,1,'ABC00003','loitering','071','Was told this data wont go away');

insert into es_db_test.Failed_VET
(
    FailedVET,
    SurveyPeriod,
    QuestionNo,
    RunReference,
    RUReference,
    Step,
    SurveyOutputCode
)
values
    (123,'201701',1,1,'ABC00001','Cleaning','066'),
    (115,'201701',2,1,'ABC00001','Smelling','066'),
    (189,'201801',3,1,'ABC00002','Inventing','068'),
    (193,'201803',4,1,'ABC00003','discovering','071'),
    (123,'201803',5,1,'ABC00003','loitering','071');


insert into es_db_test.GOR_Regions
(
    IDBR,
    GORReference,
    RegionName

)
values
   ('AA',1,'North East'),
   ('BA',3,'North West'),
   ('BB',3,'North West'),
   ('CB',3,'North West'),
   ('DC',2,'Yorks & Humberside'),
   ('ED',6,'East Midlands'),
   ('FE',5,'West Midlands'),
   ('GF',7,'Eastern'),
   ('GG',7,'Eastern'),
   ('HH',9,'South East'),
   ('JG',9,'South East'),
   ('KJ',10,'South West'),
   ('WW',11,'Wales'),
   ('XX',12,'Scotland'),
   ('YY',0,'Northern Ireland'),
   ('ZZ',0,'Non-UK');

insert into es_db_test.SSR_Old_Regions
(
    IDBR,
    SSRReference,
    RegionName

)
values
    ('AA',1,'Northern'),
    ('BA',1,'Northern'),
    ('BB',3,'North West'),
    ('CB',3,'North West'),
    ('DC',2,'Yorks & Humberside'),
    ('ED',6,'East Midlands'),
    ('FE',5,'West Midlands'),
    ('GF',7,'East Anglia'),
    ('GG',9,'South East'),
    ('HH',9,'South East'),
    ('JG',9,'South East'),
    ('KJ',10,'South West'),
    ('WW',11,'Wales'),
    ('XX',12,'Scotland'),
    ('YY',0,'Northern Ireland'),
    ('ZZ',0,'Non-UK');

