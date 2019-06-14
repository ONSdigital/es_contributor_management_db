select * from es_db_test.Query where QueryReference = :QueryInput;

select * from es_db_test.Query_Task where QueryReference = :QueryInput;

select * from es_db_test.Query_Task_Update where QueryReference = :QueryInput;

select * from es_db_test.Step_Exception where QueryReference = :QueryInput;

select * from es_db_test.Question_Anomaly where SurveyPeriod = :SurveyPeriod and RUReference = :RUReference
                                        and SurveyOutputCode = :SurveyOutputCode and RunReference = :RunReference
                                        and Step = :Step;

select * from es_db_test.Failed_VET where SurveyPeriod = :SurveyPeriod and RUReference = :RUReference
                                        and SurveyOutputCode = :SurveyOutputCode and RunReference = :RunReference
                                        and Step = :Step and QuestionNo = :QuestionNo;
