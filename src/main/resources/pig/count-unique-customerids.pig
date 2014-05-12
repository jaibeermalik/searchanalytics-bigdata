SEARCHEVENTS = load '/searchevents/${YEAR}/${MONTH}/${DAY}/${HOUR}' using JsonLoader('${JSONSCHEMA}');
-- DUMP SEARCHEVENTS;
SEARCHEVENTS_data = FOREACH SEARCHEVENTS GENERATE eventid, customerid, clickeddocid  ;
-- DUMP SEARCHEVENTS_data;
STORE SEARCHEVENTS_data INTO '/tmp/PIGSEARCHEVENTS_data_${YEAR}_${MONTH}_${DAY}_${HOUR}' using PigStorage(',');



