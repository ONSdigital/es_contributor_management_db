delete from es_db_test.Contributor_Survey_Period;
delete from es_db_test.Survey_Period;
delete from es_db_test.Survey_Contact;
delete from es_db_test.Survey_Enrolment;
delete from es_db_test.Survey;
delete from es_db_test.Contact;
delete from es_db_test.Contributor;
delete from es_db_test.Query_Type;


Insert Into es_db_test.Contributor
(
    ParentRUReference,
    RUReference,
    Address,
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
    ('500000123','ABC00001','999 Letsby Avenue, Metropolis, MP01 11AB', '1982-09-17', true, 'Officer Dibble', null,false,'Active',
                '01336 216999', 'Police Academy', 'This is some comments about the business', 200301,200701, '01336 216999'),
    ('500000456','ABC00002','18 Slingsby Road, Pontypool, NP4 6AD', '1999-01-01', true, 'King Mike', '2019-03-20', true,'De-active',
                '01662 123989', 'Alice Cooper', 'This is me making some random comments', 200302,200702, '01662 123987'),
    ('500000789','ABC00003','22 Avenue Road, Trowbridge, BA22 1ST', '1997-02-24',false, 'Lord Luke', null, true,'Active',
                '07462 456239', 'Bon Scott', 'Im on a highway to hell', 200303,200703, '03647 345987'),
    ('500000321','ABC00004','88 Gurner Terrice, Melksham, SW4 6LP', '1994-08-21',false, 'Duke David', null, true,'Active',
                '01234 654321', 'Jimmi Page', 'Its been a long time since I Rock and Roll', 200304,200704, '01422 456757'),
    ('500000876','ABC00005','4 Pete Place, Petersfield, GU32 1RT', '2001-11-3', true, 'Baron Binay', null,false,'Active',
                '01999 987990', 'Paul Simon', 'You can call me Betty Betty when you call me Al', 200305,200705, '03665 083727');


Insert Into es_db_test.Contact
(
    ContactReference,
    ContactAddress,
    ContactConstraints,
    ContactEmail,
    ContactFax,
    ContactName,
    ContactOrganisation,
    ContactPreferences,
    ContactTelephone
)
Values
    ('001','999 Letsby Avenue, Metropolis, MP01 11AB','Tied up','dibble@topcat.com','01179 087654',
                'Officer Dibble', 'This one','doesnt like to be called between 1 and 12','01179 087655'),
    ('002','18 Slingsby Road, Pontypool, NP4 6AD','Locked up','king@mike.com','01234 045658',
                'King Mike', 'Many different secret ones','Should only be called King Mike','01234 045657'),
    ('003','22 Avenue Road, Trowbridge, BA22 1ST','Fed up','lord@luke.com','07462 456239',
                'Lord Luke', 'Lord of all','not telephone or fax or eamil or sms or post','07462 456238'),
    ('004','88 Gurner Terrice, Melksham, SW4 6LP','available','duke@david.com','07658 823671',
                'Duke David', 'MI5','Please call me any time and all the time','07658 823672'),
    ('005', '4 Pete Place, Petersfield, GU32 1RT','unavailable','baron@binay.com','01579 082664',
                'Baron Binay', 'All of them','Must be really polite or else','01579 082665');


Insert Into es_db_test.Survey
(
    SurveyOutputCode,
    SurveyName
)
Values
    ('ZYX0000000000000000012345','Books'),
    ('ZYX0000000000000000023451','Bread'),
    ('ZYX0000000000000000034512','Baptisms'),
    ('ZYX0000000000000000045123','Barbarians'),
    ('ZYX0000000000000000051234','Bulldozers');


Insert Into es_db_test.Survey_Enrolment
(
    RUReference,
    SurveyOutputCode,
    NoOfCurrentConsecutivePeriodsOfNonResponse,
    NoOfPeriodsWithOutstandingQueries,
    PeriodOfEnrolment
)
Values
    ('ABC00001','ZYX0000000000000000012345',0,0,'1900-03-21'),
    ('ABC00002','ZYX0000000000000000023451',3,1,'1900-03-12'),
    ('ABC00003','ZYX0000000000000000034512',1,1,'1900-03-05'),
    ('ABC00004','ZYX0000000000000000045123',1,1,'1900-03-01'),
    ('ABC00005','ZYX0000000000000000051234',0,0,'1900-03-30');


Insert Into es_db_test.Survey_Contact
(
    ContactReference,
    RUReference,
    SurveyOutputCode,
    EffectiveEndDate,
    EffectiveStartDate

)
Values
    ('001','ABC00001','ZYX0000000000000000012345',null,'1999-03-09'),
    ('002','ABC00002','ZYX0000000000000000023451',null,'1999-03-09'),
    ('003','ABC00003','ZYX0000000000000000034512',null,'1999-03-09'),
    ('004','ABC00004','ZYX0000000000000000045123',null,'1999-03-09'),
    ('005','ABC00005','ZYX0000000000000000051234',null,'1999-03-09');


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
    ('201701','ZYX0000000000000000012345',false,10,8,11),
    ('201801','ZYX0000000000000000023451',false,18,5,20),
    ('201803','ZYX0000000000000000034512',true,23,15,23),
    ('201309','ZYX0000000000000000045123',false,13,11,20),
    ('201701','ZYX0000000000000000051234',false,20,20,21);

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
    PeriodActive,
    PriorityResponseList,
    ResponseStatus,
    ShortDescription,
    WhenStatusLastChanged
)
Values
    (1,'ABC00001','ZYX0000000000000000012345','201701','We like this guy','we are nice honest','2015-04-18',2,5,
                true,'this then this','Active','this is a description of us','2018-03-12' ),
    (1,'ABC00002','ZYX0000000000000000023451','201801','We dont like this guy','we are nice maybe','2017-03-15',1,2,
                true,'this then that','Sleeping','this is to explain what we are','2019-02-25' ),
    (1,'ABC00003','ZYX0000000000000000034512','201803','We think there maybe a problem','we have issues','2016-09-10',0,1,
                false,'that then this','Deactive','WHO are you','2019-01-03'),
    (1,'ABC00004','ZYX0000000000000000045123','201309','We think this could work','we are just a business','2019-05-02',0,0,
                true,'a then b then c','Active','we arent very good at explaining','2018-02-08'),
    (1,'ABC00005','ZYX0000000000000000051234','201701','This is my business now Dave','I do not think so','2015-04-18',2,5,
                true,'to take over you will first need to...','Active','We are evil','2018-06-11');


insert into es_db_test.Query_Type
(
    QueryType,
    Description
)
values
    ('temp','temp');
