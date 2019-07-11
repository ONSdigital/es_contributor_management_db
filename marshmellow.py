from marshmallow import Schema, fields, ValidationError


class ContributorSurveyPeriod_SurveyPeriod(Schema):
    rureference = fields.Str()
    surveyoutputcode = fields.Str()
    surveyperiod = fields.Str()
    additionalcomments = fields.Str()
    contributorcomments = fields.Str()
    lastupdated = fields.Date()
    noofactivequeriesinperiod = fields.Int()
    noofcontributorinteractionsinperiod = fields.Int()
    priorityresponselist = fields.Str()
    responsestatus = fields.Str()
    shortdescription = fields.Str()
    whenstatuslastchanged = fields.Date()
    activeperiod = fields.Bool()
    noofresponses = fields.Int()
    numbercleared = fields.Int()
    samplesize = fields.Int()
    numberclearedfirsttime = fields.Int()


class SurveyContact_Contact(Schema):
    contactreference = fields.Str()
    housenameno = fields.Str()
    street = fields.Str()
    additionaladdressline = fields.Str()
    towncity = fields.Str()
    county = fields.Str()
    country = fields.Str()
    postcode = fields.Str()
    contactconstraints = fields.Str()
    contactfax = fields.Str()
    contactname = fields.Str()
    contactorganisation = fields.Str()
    contactpreferences = fields.Str()
    contacttelephone = fields.Str()
    rureference = fields.Str()
    surveyoutputcode = fields.Str()
    effectiveenddate = fields.Date()
    effectivestartdate = fields.Date()


class SurveyEnrolment(Schema):
    rureference = fields.Str()
    surveyoutputcode = fields.Str()
    noofcurrentconsecutiveperiodsofnonresponse = fields.Int()
    noofperiodswithoutstandingqueries = fields.Int()
    periodofenrolment = fields.Str()
    Period = fields.Nested(ContributorSurveyPeriod_SurveyPeriod())
    Contacts = fields.Nested(SurveyContact_Contact())


class Contributor(Schema):
    rureference = fields.Str()
    parentrureference = fields.Str()
    housenameno = fields.Str()
    street = fields.Str()
    additionaladdressline = fields.Str()
    towncity = fields.Str()
    county = fields.Str()
    country = fields.Str()
    postcode = fields.Str()
    birthdate = fields.Date()
    businessprofilingteamcase = fields.Bool()
    contact = fields.Str()
    deathdate = fields.Date()
    enforcementflag = fields.Bool()
    enforcementstatus = fields.Str()
    fax = fields.Str()
    contributorname = fields.Str()
    profileinformation = fields.Str()
    sic2003 = fields.Int()
    sic2007 = fields.Int()
    telephone = fields.Str()
    Surveys = fields.Nested(SurveyEnrolment())


class QueryTaskUpdates(Schema):
    taskseqno = fields.Int()
    queryreference = fields.Int()
    lastupdated = fields.Date()
    taskupdateddescription = fields.Str()
    updatedby = fields.Str()


class QueryTasks(Schema):
    taskseqno = fields.Int()
    queryreference = fields.Int()
    responserequiredby = fields.Date()
    taskdescription = fields.Str()
    taskresponsibility = fields.Str()
    taskstatus = fields.Str()
    nextplannedaction = fields.Str()
    whenactionrequired = fields.Date()
    QueryTaskUpdates = fields.Nested(QueryTaskUpdates())


class FailedVETs_VETs(Schema):
    failedvet = fields.Int()
    surveyperiod = fields.Str()
    questionno = fields.Str()
    runreference = fields.Int()
    rureference = fields.Str()
    step = fields.Str()
    surveyoutputcode = fields.Str()
    description = fields.Str()


class Anomalies(Schema):
    surveyperiod = fields.Str()
    questionno = fields.Str()
    runreference = fields.Int()
    rureference = fields.Str()
    step = fields.Str()
    surveyoutputcode = fields.Str()
    description = fields.Str()
    FailedVETs = fields.Nested(FailedVETs_VETs())


class Exceptions(Schema):
    queryreference = fields.Int()
    surveyperiod = fields.Str()
    runreference = fields.Int()
    rureference = fields.Str()
    step = fields.Str()
    surveyoutputcode = fields.Str()
    errorcode = fields.Str()
    errordescription = fields.Str()
    Anomalies = fields.Nested(Anomalies())


class Query(Schema):
    queryreference = fields.Int()
    query = fields.Str()
    rureference = fields.Str()
    surveyoutputcode = fields.Str()
    periodqueryrelates = fields.Str()
    currentperiod = fields.Str()
    datetimeraised = fields.Date()
    generalspecificflag = fields.Bool()
    industrygroup = fields.Str()
    lastqueryupdate = fields.Date()
    queryactive = fields.Bool()
    querydescription = fields.Str()
    querystatus = fields.Str()
    raisedby = fields.Str()
    resultsstate = fields.Str()
    targetresolutiondate = fields.Date()
    Exceptions = fields.Nested(Exceptions())
    QueryTasks = fields.Nested(QueryTasks())


def main():
    VET = dict(failedvet=1, surveyperiod='201809', questionno='1', runreference=1, rureference='77700000001', step='1', surveyoutputcode=66, description='I Work.')

    try:
        result = FailedVETs_VETs().load(data=VET)
        print("Test")
        return 2
    except ValidationError as err:
        print("Trump")
        print(err.messages)
        print(err.valid_data)
        return 1
    return 3

y = main()
print(y)