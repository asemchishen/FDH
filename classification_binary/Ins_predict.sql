--https://docs.oracle.com/en/database/oracle/oracle-database/18/arpls/DBMS_DATA_MINING.html#GUID-7F525A4E-9C93-44D6-BFFF-10BC018CD4A6

--!!!!!!!!!!!!!!!!!!!!!!Creating mod_set tables for classification algs NB(1),DT(2),GLM(3),SVM(4)
DROP TABLE mod_set1;
DROP TABLE mod_set2;
DROP TABLE mod_set3;
DROP TABLE mod_set4;

CREATE TABLE mod_set1 (
      SETTING_NAME     VARCHAR2(30),
      SETTING_VALUE    VARCHAR2(4000) 
);
INSERT INTO mod_set1 VALUES ('ALGO_NAME', 'ALGO_NAIVE_BAYES');

CREATE TABLE mod_set2 (
      SETTING_NAME     VARCHAR2(30),
      SETTING_VALUE    VARCHAR2(4000) 
);
INSERT INTO mod_set2 VALUES ('ALGO_NAME', 'ALGO_DECISION_TREE');
--INSERT INTO mod_set2 VALUES ('TREE_TERM_MAX_DEPTH', TO_CHAR(15));
--INSERT INTO mod_set2 VALUES ('TREE_TERM_MINREC_NODE', TO_CHAR(3));


CREATE TABLE mod_set3 (
      SETTING_NAME     VARCHAR2(30),
      SETTING_VALUE    VARCHAR2(4000) 
);
INSERT INTO mod_set3 VALUES ('ALGO_NAME', 'ALGO_GENERALIZED_LINEAR_MODEL');

CREATE TABLE mod_set4 (
      SETTING_NAME     VARCHAR2(30),
      SETTING_VALUE    VARCHAR2(4000) 
);
INSERT INTO mod_set4 VALUES ('ALGO_NAME', 'ALGO_SUPPORT_VECTOR_MACHINES');
COMMIT;


--!!!!!!!!!Creating model accuracy table for all algorythms

DROP TABLE mod_accuracy;

CREATE TABLE mod_accuracy (
                MODEL_NAME     VARCHAR2(30),
                ACCURACY       NUMBER, 
                ROC_AREA       NUMBER
            );
    
INSERT INTO mod_accuracy (MODEL_NAME) VALUES ('NB');
INSERT INTO mod_accuracy (MODEL_NAME) VALUES ('DT');
INSERT INTO mod_accuracy (MODEL_NAME) VALUES ('GLM');
INSERT INTO mod_accuracy (MODEL_NAME) VALUES ('SVM');
COMMIT;

--!!!!!!!!!!!!!!!!!!!!!!!Creating train and test sets
--splitting ins sales in test and train views by random sampling 60%
CREATE OR REPLACE VIEW ins_train AS SELECT * FROM ins_sales SAMPLE (60) SEED (1);
CREATE OR REPLACE VIEW ins_test AS SELECT * FROM ins_sales MINUS SELECT * FROM ins_train;
--joining sales facts with customers data for both sets
CREATE OR REPLACE VIEW ins_train_set AS
SELECT 
    n.cust_id, 
    n.gender,
    round((sysdate-n.dob)/365, 0) AS age,
    LPAD(TO_CHAR(n.ZIP),5,'0') AS zip,
    n.city, n.state,
    --z.lat, z.lng,
    m.descision
FROM
    customers n
INNER JOIN
    ins_train m
ON m.cust_id=n.cust_id
LEFT JOIN
    zcodes z
ON z.zip=n.zip;

CREATE OR REPLACE VIEW ins_test_set AS
SELECT 
    n.cust_id, 
    n.gender,
    round((sysdate-n.dob)/365, 0) AS age,
    LPAD(TO_CHAR(n.ZIP),5,'0') AS zip,
    n.city, n.state,
    --z.lat, z.lng,
    m.descision
FROM
    customers n
INNER JOIN
    ins_test m
ON m.cust_id=n.cust_id
LEFT JOIN
    zcodes z
ON z.zip=n.zip;

--!!!!!!!!!!!!!!!!!!!!!!Creating 4 models
BEGIN
DBMS_DATA_MINING.DROP_MODEL('ins_prediction1');
DBMS_DATA_MINING.DROP_MODEL('ins_prediction2');
DBMS_DATA_MINING.DROP_MODEL('ins_prediction3');
DBMS_DATA_MINING.DROP_MODEL('ins_prediction4');
END;
/

BEGIN
DBMS_DATA_MINING.CREATE_MODEL(
    model_name          => 'ins_prediction1', 
    --Name of the model in the form [schema_name.]model_name.
    mining_function     => dbms_data_mining.classification,
    --The mining function. 
    data_table_name     => 'ins_train_set',
    -- Table or view containing the build data
    case_id_column_name => 'cust_id',
    --Case identifier column in the build data.
    target_column_name  => 'descision',
    --For supervised models, the target column in the build data. NULL for unsupervised models.
    settings_table_name => 'mod_set1'
    );
END;
/
BEGIN
DBMS_DATA_MINING.CREATE_MODEL(
    model_name          => 'ins_prediction2', 
    --Name of the model in the form [schema_name.]model_name.
    mining_function     => dbms_data_mining.classification,
    --The mining function. 
    data_table_name     => 'ins_train_set',
    -- Table or view containing the build data
    case_id_column_name => 'cust_id',
    --Case identifier column in the build data.
    target_column_name  => 'descision',
    --For supervised models, the target column in the build data. NULL for unsupervised models.
    settings_table_name => 'mod_set2'
    );
END;
/
BEGIN
DBMS_DATA_MINING.CREATE_MODEL(
    model_name          => 'ins_prediction3', 
    --Name of the model in the form [schema_name.]model_name.
    mining_function     => dbms_data_mining.classification,
    --The mining function. 
    data_table_name     => 'ins_train_set',
    -- Table or view containing the build data
    case_id_column_name => 'cust_id',
    --Case identifier column in the build data.
    target_column_name  => 'descision',
    --For supervised models, the target column in the build data. NULL for unsupervised models.
    settings_table_name => 'mod_set3'
    );
END;
/
BEGIN
DBMS_DATA_MINING.CREATE_MODEL(
    model_name          => 'ins_prediction4', 
    --Name of the model in the form [schema_name.]model_name.
    mining_function     => dbms_data_mining.classification,
    --The mining function. 
    data_table_name     => 'ins_train_set',
    -- Table or view containing the build data
    case_id_column_name => 'cust_id',
    --Case identifier column in the build data.
    target_column_name  => 'descision',
    --For supervised models, the target column in the build data. NULL for unsupervised models.
    settings_table_name => 'mod_set4'
    );
END;
/


--Creationg apply results tables (applying model to test set)
DROP TABLE ins_apply_results1;
DROP TABLE ins_apply_results2;
DROP TABLE ins_apply_results3;
DROP TABLE ins_apply_results4;

CREATE TABLE ins_apply_results1 AS
       SELECT cust_id,
              PREDICTION(ins_prediction1 USING *) AS prediction,
              PREDICTION_PROBABILITY(ins_prediction1 USING *) AS probability
       FROM ins_test_set;
CREATE TABLE ins_apply_results2 AS
       SELECT cust_id,
              PREDICTION(ins_prediction2 USING *) AS prediction,
              PREDICTION_PROBABILITY(ins_prediction2 USING *) AS probability
       FROM ins_test_set;
CREATE TABLE ins_apply_results3 AS
       SELECT cust_id,
              PREDICTION(ins_prediction3 USING *) AS prediction,
              PREDICTION_PROBABILITY(ins_prediction3 USING *) AS probability
       FROM ins_test_set;
CREATE TABLE ins_apply_results4 AS
       SELECT cust_id,
              PREDICTION(ins_prediction4 USING *) AS prediction,
              PREDICTION_PROBABILITY(ins_prediction4 USING *) AS probability
       FROM ins_test_set;


--Creating confussion matrixes and populating accuracy table   
DROP TABLE ins_confusion_matrix1;
DROP TABLE ins_confusion_matrix2;
DROP TABLE ins_confusion_matrix3;
DROP TABLE ins_confusion_matrix4;

DECLARE
   v_accuracy1    NUMBER;
   v_accuracy2    NUMBER;
   v_accuracy3    NUMBER;
   v_accuracy4    NUMBER;
BEGIN
    DBMS_DATA_MINING.COMPUTE_CONFUSION_MATRIX (
               accuracy                     => v_accuracy1,
               apply_result_table_name      => 'ins_apply_results1',
               target_table_name            => 'ins_test_set',
               case_id_column_name          => 'cust_id',
               target_column_name           => 'descision',
               confusion_matrix_table_name  => 'ins_confusion_matrix1',
               score_column_name            => 'PREDICTION',
               score_criterion_column_name  => 'PROBABILITY',
               score_criterion_type         => 'PROBABILITY');
        DBMS_DATA_MINING.COMPUTE_CONFUSION_MATRIX (
               accuracy                     => v_accuracy2,
               apply_result_table_name      => 'ins_apply_results2',
               target_table_name            => 'ins_test_set',
               case_id_column_name          => 'cust_id',
               target_column_name           => 'descision',
               confusion_matrix_table_name  => 'ins_confusion_matrix2',
               score_column_name            => 'PREDICTION',
               score_criterion_column_name  => 'PROBABILITY',
               score_criterion_type         => 'PROBABILITY');
        DBMS_DATA_MINING.COMPUTE_CONFUSION_MATRIX (
               accuracy                     => v_accuracy3,
               apply_result_table_name      => 'ins_apply_results3',
               target_table_name            => 'ins_test_set',
               case_id_column_name          => 'cust_id',
               target_column_name           => 'descision',
               confusion_matrix_table_name  => 'ins_confusion_matrix3',
               score_column_name            => 'PREDICTION',
               score_criterion_column_name  => 'PROBABILITY',
               score_criterion_type         => 'PROBABILITY');
        DBMS_DATA_MINING.COMPUTE_CONFUSION_MATRIX (
               accuracy                     => v_accuracy4,
               apply_result_table_name      => 'ins_apply_results4',
               target_table_name            => 'ins_test_set',
               case_id_column_name          => 'cust_id',
               target_column_name           => 'descision',
               confusion_matrix_table_name  => 'ins_confusion_matrix4',
               score_column_name            => 'PREDICTION',
               score_criterion_column_name  => 'PROBABILITY',
               score_criterion_type         => 'PROBABILITY');

    UPDATE mod_accuracy SET ACCURACY=ROUND(v_accuracy1,4) WHERE MODEL_NAME='NB';
    UPDATE mod_accuracy SET ACCURACY=ROUND(v_accuracy2,4) WHERE MODEL_NAME='DT';
    UPDATE mod_accuracy SET ACCURACY=ROUND(v_accuracy3,4) WHERE MODEL_NAME='GLM';
    UPDATE mod_accuracy SET ACCURACY=ROUND(v_accuracy4,4) WHERE MODEL_NAME='SVM';
END;
/

--Creating ROC table and calculating ROC area
DROP TABLE roc1;
DROP TABLE roc2;
DROP TABLE roc3;
DROP TABLE roc4;

DECLARE
     v_area_under_curve1 NUMBER;
     v_area_under_curve2 NUMBER;
     v_area_under_curve3 NUMBER;
     v_area_under_curve4 NUMBER;
  BEGIN
         DBMS_DATA_MINING.COMPUTE_ROC (
               roc_area_under_curve          => v_area_under_curve1,
               apply_result_table_name       => 'ins_apply_results1',
               target_table_name             => 'ins_test_set',
               case_id_column_name           => 'cust_id',
               target_column_name            => 'descision',
               roc_table_name                => 'roc1',
               positive_target_value         => 'Y',
               score_column_name             => 'PREDICTION',
               score_criterion_column_name   => 'PROBABILITY');
         DBMS_DATA_MINING.COMPUTE_ROC (
               roc_area_under_curve          => v_area_under_curve2,
               apply_result_table_name       => 'ins_apply_results2',
               target_table_name             => 'ins_test_set',
               case_id_column_name           => 'cust_id',
               target_column_name            => 'descision',
               roc_table_name                => 'roc2',
               positive_target_value         => 'Y',
               score_column_name             => 'PREDICTION',
               score_criterion_column_name   => 'PROBABILITY');
         DBMS_DATA_MINING.COMPUTE_ROC (
               roc_area_under_curve          => v_area_under_curve3,
               apply_result_table_name       => 'ins_apply_results3',
               target_table_name             => 'ins_test_set',
               case_id_column_name           => 'cust_id',
               target_column_name            => 'descision',
               roc_table_name                => 'roc3',
               positive_target_value         => 'Y',
               score_column_name             => 'PREDICTION',
               score_criterion_column_name   => 'PROBABILITY');
         DBMS_DATA_MINING.COMPUTE_ROC (
               roc_area_under_curve          => v_area_under_curve4,
               apply_result_table_name       => 'ins_apply_results4',
               target_table_name             => 'ins_test_set',
               case_id_column_name           => 'cust_id',
               target_column_name            => 'descision',
               roc_table_name                => 'roc4',
               positive_target_value         => 'Y',
               score_column_name             => 'PREDICTION',
               score_criterion_column_name   => 'PROBABILITY');
    UPDATE mod_accuracy SET ROC_AREA=ROUND(v_area_under_curve1,4) WHERE MODEL_NAME='NB';
    UPDATE mod_accuracy SET ROC_AREA=ROUND(v_area_under_curve2,4) WHERE MODEL_NAME='DT';
    UPDATE mod_accuracy SET ROC_AREA=ROUND(v_area_under_curve3,4) WHERE MODEL_NAME='GLM';
    UPDATE mod_accuracy SET ROC_AREA=ROUND(v_area_under_curve4,4) WHERE MODEL_NAME='SVM';
  END;
 /


--Create confussion matrixes summary
DROP TABLE confusion;
CREATE TABLE confusion (
                actual_target_value     VARCHAR2(1),
                predicted_target_value  VARCHAR2(1)
            );
   
INSERT INTO confusion VALUES ('Y','Y');
INSERT INTO confusion VALUES ('Y','N');
INSERT INTO confusion VALUES ('N','N');
INSERT INTO confusion VALUES ('N','Y');
COMMIT;
CREATE OR REPLACE VIEW confusion_matrix AS
SELECT v.actual_target_value, v.predicted_target_value,
       c.value AS NB, b.value AS DT, n.value AS GLM, m.value AS SVM 
       FROM confusion v
       LEFT JOIN ins_confusion_matrix1 c
       ON (v.actual_target_value=c.actual_target_value AND v.predicted_target_value=c.predicted_target_value)
       LEFT JOIN ins_confusion_matrix2 b
       ON (v.actual_target_value=b.actual_target_value AND v.predicted_target_value=b.predicted_target_value)
       LEFT JOIN ins_confusion_matrix3 n
       ON (v.actual_target_value=n.actual_target_value AND v.predicted_target_value=n.predicted_target_value)
       LEFT JOIN ins_confusion_matrix4 m
       ON (v.actual_target_value=m.actual_target_value AND v.predicted_target_value=m.predicted_target_value);
       
--View accuracy and ROC
SELECT * from mod_accuracy;
--View confussion matrixes summary
SELECT * from confusion_matrix;

--adding costs!!!
DROP TABLE costs;
CREATE TABLE costs (
  actual_target_value           CHAR(1),
  predicted_target_value        CHAR(1),
  cost                          NUMBER);
INSERT INTO costs values ('Y', 'Y', 0);
INSERT INTO costs values ('Y', 'N', 0.75);
INSERT INTO costs values ('N', 'Y', 0.25);
INSERT INTO costs values ('N', 'N', 0);
COMMIT;

EXEC dbms_data_mining.remove_cost_matrix('ins_prediction2');
EXEC dbms_data_mining.add_cost_matrix('ins_prediction2', 'costs');

DROP TABLE ins_apply_results2c;
CREATE TABLE ins_apply_results2c AS
       SELECT cust_id,
              PREDICTION(ins_prediction2 COST MODEL USING *) AS prediction,
              PREDICTION_PROBABILITY(ins_prediction2 USING *) AS probability
       FROM ins_test_set;

DROP TABLE ins_confusion_matrix2c;
DECLARE
   v_accuracy1    NUMBER;
BEGIN
    DBMS_DATA_MINING.COMPUTE_CONFUSION_MATRIX (
               accuracy                     => v_accuracy1,
               apply_result_table_name      => 'ins_apply_results2c',
               target_table_name            => 'ins_test_set',
               case_id_column_name          => 'cust_id',
               target_column_name           => 'descision',
               confusion_matrix_table_name  => 'ins_confusion_matrix2c',
               score_column_name            => 'PREDICTION',
               score_criterion_column_name  => 'PROBABILITY',
               score_criterion_type         => 'PROBABILITY');
               DBMS_OUTPUT.PUT_LINE(ROUND(v_accuracy1, 4));
END;
/

SELECT v.actual_target_value, v.predicted_target_value,
       v.value AS DT_nocost, b.value AS DT_cost,  b.value/v.value
FROM ins_confusion_matrix2 v 
LEFT JOIN ins_confusion_matrix2c b
ON (v.actual_target_value=b.actual_target_value AND v.predicted_target_value=b.predicted_target_value);