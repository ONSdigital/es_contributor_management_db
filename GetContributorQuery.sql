SELECT * FROM es_db_test.Contributor RUReference = :RuInput;

SELECT * FROM es_db_test.Survey_Enrolment where RUReference = :RuInput;

SELECT * FROM es_db_test.Contributor_Survey_Period where RUReference = :RuInput;

SELECT * FROM es_db_test.Survey_Period where es_db_test.Survey_Period.SurveyOutputCode =
                (select SurveyOutputCode from es_db_test.Contributor_Survey_Period where RUReference = :RuInput);
SELECT * FROM es_db_test.Survey_Contact where RUReference = :RuInput;

SELECT * FROM es_db_test.Survey_Period where es_db_test.Survey_Period.SurveyOutputCode =
                (select SurveyOutputCode from es_db_test.Contributor_Survey_Period where RUReference = :RuInput);

SELECT * FROM es_db_test.Contact where ContactReference =
                (select ContactReference from es_db_test.Survey_Contact where RUReference = :RuInput);

