CREATE TABLE #Codesets (
  codeset_id int NOT NULL,
  concept_id bigint NOT NULL
)
;

INSERT INTO #Codesets (codeset_id, concept_id)
SELECT 0 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (45887628)and invalid_reason is null
UNION  select c.concept_id
  from @vocabulary_database_schema.CONCEPT c
  join @vocabulary_database_schema.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (45887628)
  and c.invalid_reason is null

) I
) C;
INSERT INTO #Codesets (codeset_id, concept_id)
SELECT 1 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (4303663)and invalid_reason is null
UNION  select c.concept_id
  from @vocabulary_database_schema.CONCEPT c
  join @vocabulary_database_schema.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (4303663)
  and c.invalid_reason is null

) I
) C;
INSERT INTO #Codesets (codeset_id, concept_id)
SELECT 2 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (4300757,4169785,2006738,2006752)and invalid_reason is null
UNION  select c.concept_id
  from @vocabulary_database_schema.CONCEPT c
  join @vocabulary_database_schema.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (4300757,4169785,2006738,2006752)
  and c.invalid_reason is null

) I
LEFT JOIN
(
  select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (2211716,2211717,2103013,37108819,37109496,37109471,37109139,37108858,37117477,37109096,37108822,37109497,37109472,37109137,37108859,37108797,2006973,4325536,2211801,2211803,2211800,2211802,4151587,4080293,2211824,2100847,4248586,4169274,4168502,4302055,4303948,4305202,4304120,4304422,4166894,4304719,4301907,4302799,4168669,4305686,4169300,4168821,4167547,4170752,4031025,4079574,4328579,4332366,4328658,4329507,4296011,46273782,40486676,46271426,46271519,37396236,4082482,4306153,4332667,4324447,4326489,46272782,45765533,4161770,4161120,4198156,4201653,4203388,4329643,4326491,4325534,4160993,36713620,44808417,44808423,44808419,44809284,44808421,44813844,40479300,4325531,4333364,4333356,4198609,4327272,4196753,4198481,4199383,46272837,4230144,4335678,45773198,4258282,4230133,4230134,4258558,4208613,4258699,37396807,37399550,37396813,44806016,44806014,44806015,44811689,4207900,44805970,46272429,4330933,4330373,40481084,4331376,4330931,44807906,4332801,4160714,4335102,4198633,46286923,46284190,36713064,37395665,2110615,40756941,40757042,40756863,40757127,40756835,40757030,40756797,40756805,40756801,37109079,2006754,4173965,43531453,37109089,43531454,43531455,37109088,43531457,2006740,506677,44808386,42689556,42689555,42689557,506543,44808387,2720567,2720569,2212056,2212057,2212058,40482317,44782531,40480994,40481954,36713668,40480968,40481955,40482393,40482394,42872445,40481886,40480117,40480581,40480559,37111256,40480962,46285945,40482398,37395922,37397339,44514205,37109091,36716950,46285929,37109092,40480597,44783489,37109428,40483661,37111259,40485465,36713067,46272904,40482338,40479420,40479375,42873024,2006741,2006739,37017307,37017371,37017303,37017309,37017306,37017308,37018723,40484132,37017302,37017897,37018836,40481001,40480992,40484586,40481437,40482810,40482811,37017310,40480118,42872447,40480560,40480582,44514206,40481465,40481466,40481417,40481418,40484593,40482340,40481956,40481957,40482355,40482356,40480165,40480167,40480119,40482403,36717005,40482739,40480138,40483236,40480583,40480584,40487512,40481440,40480966,40480967,40481419,40481420,40482319,40480617,36713598,44808543,44808546,2110542,2110543,2110544,2110559,2110560,2110557,2110558,2006735)and invalid_reason is null
UNION  select c.concept_id
  from @vocabulary_database_schema.CONCEPT c
  join @vocabulary_database_schema.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (2211716,2211717,2103013,37108819,37109496,37109471,37109139,37108858,37117477,37109096,37108822,37109497,37109472,37109137,37108859,37108797,2006973,4325536,2211801,2211803,2211800,2211802,4151587,4080293,2211824,2100847,4248586,4169274,4168502,4302055,4303948,4305202,4304120,4304422,4166894,4304719,4301907,4302799,4168669,4305686,4169300,4168821,4167547,4170752,4031025,4079574,4328579,4332366,4328658,4329507,4296011,46273782,40486676,46271426,46271519,37396236,4082482,4306153,4332667,4324447,4326489,46272782,45765533,4161770,4161120,4198156,4201653,4203388,4329643,4326491,4325534,4160993,36713620,44808417,44808423,44808419,44809284,44808421,44813844,40479300,4325531,4333364,4333356,4198609,4327272,4196753,4198481,4199383,46272837,4230144,4335678,45773198,4258282,4230133,4230134,4258558,4208613,4258699,37396807,37399550,37396813,44806016,44806014,44806015,44811689,4207900,44805970,46272429,4330933,4330373,40481084,4331376,4330931,44807906,4332801,4160714,4335102,4198633,46286923,46284190,36713064,37395665,2110615,40756941,40757042,40756863,40757127,40756835,40757030,40756797,40756805,40756801,37109079,2006754,4173965,43531453,37109089,43531454,43531455,37109088,43531457,2006740,506677,44808386,42689556,42689555,42689557,506543,44808387,2720567,2720569,2212056,2212057,2212058,40482317,44782531,40480994,40481954,36713668,40480968,40481955,40482393,40482394,42872445,40481886,40480117,40480581,40480559,37111256,40480962,46285945,40482398,37395922,37397339,44514205,37109091,36716950,46285929,37109092,40480597,44783489,37109428,40483661,37111259,40485465,36713067,46272904,40482338,40479420,40479375,42873024,2006741,2006739,37017307,37017371,37017303,37017309,37017306,37017308,37018723,40484132,37017302,37017897,37018836,40481001,40480992,40484586,40481437,40482810,40482811,37017310,40480118,42872447,40480560,40480582,44514206,40481465,40481466,40481417,40481418,40484593,40482340,40481956,40481957,40482355,40482356,40480165,40480167,40480119,40482403,36717005,40482739,40480138,40483236,40480583,40480584,40487512,40481440,40480966,40480967,40481419,40481420,40482319,40480617,36713598,44808543,44808546,2110542,2110543,2110544)
  and c.invalid_reason is null

) E ON I.concept_id = E.concept_id
WHERE E.concept_id is null
) C;
INSERT INTO #Codesets (codeset_id, concept_id)
SELECT 3 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (4013636)and invalid_reason is null
UNION  select c.concept_id
  from @vocabulary_database_schema.CONCEPT c
  join @vocabulary_database_schema.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (4013636)
  and c.invalid_reason is null

) I
LEFT JOIN
(
  select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (2211716,2211717,37111202,37109190,37109819,37109191,2100847,2100935,2100934,40480193,2211357,2211356,2211358,2211804,2211805,4120040,4329651,4233443,4208030,4235229,506540,4335386,4082483,40480641,4161374,40482948,4161658,4335219,4205922,4259410,4209492,4261938,4232871,4231061,4203993,44811133,46271516,4328167,4160840,44806667,4080294,4330244,44807907,4257899,44811139,44812237,4326182,44806544,4160726,46286832,4205516,4199388,44792667,45766054,4332384,4328561,37399464,4332376,37396023,4198015,44806546,44809219,37396024,4326166,42690671,2006961,4082479,4079685,4044941,2110542)and invalid_reason is null
UNION  select c.concept_id
  from @vocabulary_database_schema.CONCEPT c
  join @vocabulary_database_schema.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (2211716,2211717,37111202,37109190,37109819,37109191,2100847,2100935,2100934,40480193,2211357,2211356,2211358,2211804,2211805,4120040,4329651,4233443,4208030,4235229,506540,4335386,40480641,4161374,40482948,4161658,4335219,4205922,4259410,4209492,4261938,4232871,4231061,4203993,44811133,46271516,4328167,4160840,44806667,4080294,4330244,44807907,4257899,44811139,44812237,4326182,44806544,4160726,46286832,4205516,4199388,44792667,45766054,4332384,4328561,37399464,4332376,37396023,4198015,44806546,44809219,37396024,4326166,42690671,2006961,4082479,4079685,4044941,2110542)
  and c.invalid_reason is null

) E ON I.concept_id = E.concept_id
WHERE E.concept_id is null
) C;
INSERT INTO #Codesets (codeset_id, concept_id)
SELECT 4 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (2211345,45889762,2211348,4167390)and invalid_reason is null
UNION  select c.concept_id
  from @vocabulary_database_schema.CONCEPT c
  join @vocabulary_database_schema.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (2211345,45889762,2211348,4167390)
  and c.invalid_reason is null

) I
) C;
INSERT INTO #Codesets (codeset_id, concept_id)
SELECT 5 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (45888788)and invalid_reason is null
UNION  select c.concept_id
  from @vocabulary_database_schema.CONCEPT c
  join @vocabulary_database_schema.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (45888788)
  and c.invalid_reason is null

) I
) C;


with primary_events (event_id, person_id, start_date, end_date, op_start_date, op_end_date, visit_occurrence_id) as
(
-- Begin Primary Events
select P.ordinal as event_id, P.person_id, P.start_date, P.end_date, op_start_date, op_end_date, cast(P.visit_occurrence_id as bigint) as visit_occurrence_id
FROM
(
  select E.person_id, E.start_date, E.end_date, row_number() OVER (PARTITION BY E.person_id ORDER BY E.start_date ASC) ordinal, OP.observation_period_start_date as op_start_date, OP.observation_period_end_date as op_end_date, cast(E.visit_occurrence_id as bigint) as visit_occurrence_id
  FROM 
  (
  -- Begin Procedure Occurrence Criteria
select C.person_id, C.procedure_occurrence_id as event_id, C.procedure_date as start_date, DATEADD(d,1,C.procedure_date) as END_DATE, C.procedure_concept_id as TARGET_CONCEPT_ID, C.visit_occurrence_id
from 
(
  select po.* 
  FROM @cdm_database_schema.PROCEDURE_OCCURRENCE po
JOIN #Codesets codesets on ((po.procedure_concept_id = codesets.concept_id and codesets.codeset_id = 0))
) C


-- End Procedure Occurrence Criteria

  ) E
	JOIN @cdm_database_schema.observation_period OP on E.person_id = OP.person_id and E.start_date >=  OP.observation_period_start_date and E.start_date <= op.observation_period_end_date
  WHERE DATEADD(day,0,OP.OBSERVATION_PERIOD_START_DATE) <= E.START_DATE AND DATEADD(day,0,E.START_DATE) <= OP.OBSERVATION_PERIOD_END_DATE
) P
WHERE P.ordinal = 1
-- End Primary Events

)
SELECT event_id, person_id, start_date, end_date, op_start_date, op_end_date, visit_occurrence_id
INTO #qualified_events
FROM 
(
  select pe.event_id, pe.person_id, pe.start_date, pe.end_date, pe.op_start_date, pe.op_end_date, row_number() over (partition by pe.person_id order by pe.start_date ASC) as ordinal, cast(pe.visit_occurrence_id as bigint) as visit_occurrence_id
  FROM primary_events pe
  
) QE

;

--- Inclusion Rule Inserts

select 0 as inclusion_rule_id, person_id, event_id
INTO #Inclusion_0
FROM 
(
  select pe.person_id, pe.event_id
  FROM #qualified_events pe
  
JOIN (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
FROM
(
  select E.person_id, E.event_id 
  FROM #qualified_events E
  INNER JOIN
  (
    -- Begin Correlated Criteria
SELECT 0 as index_id, p.person_id, p.event_id
FROM #qualified_events P
LEFT JOIN
(
  -- Begin Procedure Occurrence Criteria
select C.person_id, C.procedure_occurrence_id as event_id, C.procedure_date as start_date, DATEADD(d,1,C.procedure_date) as END_DATE, C.procedure_concept_id as TARGET_CONCEPT_ID, C.visit_occurrence_id
from 
(
  select po.* 
  FROM @cdm_database_schema.PROCEDURE_OCCURRENCE po
JOIN #Codesets codesets on ((po.procedure_concept_id = codesets.concept_id and codesets.codeset_id = 5))
) C


-- End Procedure Occurrence Criteria

) A on A.person_id = P.person_id and A.START_DATE >= P.OP_START_DATE AND A.START_DATE <= P.OP_END_DATE AND A.START_DATE >= DATEADD(day,-1,P.START_DATE) and A.START_DATE <= DATEADD(day,0,P.START_DATE)
GROUP BY p.person_id, p.event_id
HAVING COUNT(A.TARGET_CONCEPT_ID) = 0
-- End Correlated Criteria

  ) CQ on E.person_id = CQ.person_id and E.event_id = CQ.event_id
  GROUP BY E.person_id, E.event_id
  HAVING COUNT(index_id) = 1
) G
-- End Criteria Group
) AC on AC.person_id = pe.person_id AND AC.event_id = pe.event_id
) Results
;

SELECT inclusion_rule_id, person_id, event_id
INTO #inclusion_events
FROM (select inclusion_rule_id, person_id, event_id from #Inclusion_0) I;
TRUNCATE TABLE #Inclusion_0;
DROP TABLE #Inclusion_0;


with cteIncludedEvents(event_id, person_id, start_date, end_date, op_start_date, op_end_date, ordinal) as
(
  SELECT event_id, person_id, start_date, end_date, op_start_date, op_end_date, row_number() over (partition by person_id order by start_date ASC) as ordinal
  from
  (
    select Q.event_id, Q.person_id, Q.start_date, Q.end_date, Q.op_start_date, Q.op_end_date, SUM(coalesce(POWER(cast(2 as bigint), I.inclusion_rule_id), 0)) as inclusion_rule_mask
    from #qualified_events Q
    LEFT JOIN #inclusion_events I on I.person_id = Q.person_id and I.event_id = Q.event_id
    GROUP BY Q.event_id, Q.person_id, Q.start_date, Q.end_date, Q.op_start_date, Q.op_end_date
  ) MG -- matching groups

  -- the matching group with all bits set ( POWER(2,# of inclusion rules) - 1 = inclusion_rule_mask
  WHERE (MG.inclusion_rule_mask = POWER(cast(2 as bigint),1)-1)

)
select event_id, person_id, start_date, end_date, op_start_date, op_end_date
into #included_events
FROM cteIncludedEvents Results
WHERE Results.ordinal = 1
;



-- generate cohort periods into #final_cohort
with cohort_ends (event_id, person_id, end_date) as
(
	-- cohort exit dates
  -- By default, cohort exit at the event's op end date
select event_id, person_id, op_end_date as end_date from #included_events
),
first_ends (person_id, start_date, end_date) as
(
	select F.person_id, F.start_date, F.end_date
	FROM (
	  select I.event_id, I.person_id, I.start_date, E.end_date, row_number() over (partition by I.person_id, I.event_id order by E.end_date) as ordinal 
	  from #included_events I
	  join cohort_ends E on I.event_id = E.event_id and I.person_id = E.person_id and E.end_date >= I.start_date
	) F
	WHERE F.ordinal = 1
)
select person_id, start_date, end_date
INTO #cohort_rows
from first_ends;

with cteEndDates (person_id, end_date) AS -- the magic
(	
	SELECT
		person_id
		, DATEADD(day,-1 * 0, event_date)  as end_date
	FROM
	(
		SELECT
			person_id
			, event_date
			, event_type
			, MAX(start_ordinal) OVER (PARTITION BY person_id ORDER BY event_date, event_type ROWS UNBOUNDED PRECEDING) AS start_ordinal 
			, ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY event_date, event_type) AS overall_ord
		FROM
		(
			SELECT
				person_id
				, start_date AS event_date
				, -1 AS event_type
				, ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY start_date) AS start_ordinal
			FROM #cohort_rows
		
			UNION ALL
		

			SELECT
				person_id
				, DATEADD(day,0,end_date) as end_date
				, 1 AS event_type
				, NULL
			FROM #cohort_rows
		) RAWDATA
	) e
	WHERE (2 * e.start_ordinal) - e.overall_ord = 0
),
cteEnds (person_id, start_date, end_date) AS
(
	SELECT
		 c.person_id
		, c.start_date
		, MIN(e.end_date) AS era_end_date
	FROM #cohort_rows c
	JOIN cteEndDates e ON c.person_id = e.person_id AND e.end_date >= c.start_date
	GROUP BY c.person_id, c.start_date
)
select person_id, min(start_date) as start_date, end_date
into #final_cohort
from cteEnds
group by person_id, end_date
;

IF OBJECT_ID('@target_database_schema.@target_cohort_table', 'U') IS NOT NULL
DROP TABLE @target_database_schema.@target_cohort_table;

CREATE TABLE @target_database_schema.@target_cohort_table (
cohort_definition_id INT,
cohort_start_date DATE,
cohort_end_date DATE,
subject_id BIGINT
);
INSERT INTO @target_database_schema.@target_cohort_table (cohort_definition_id, cohort_start_date, cohort_end_date, subject_id)
select @target_cohort_id as cohort_definition_id, start_date, end_date, person_id
FROM #final_cohort CO
;



TRUNCATE TABLE #cohort_rows;
DROP TABLE #cohort_rows;

TRUNCATE TABLE #final_cohort;
DROP TABLE #final_cohort;

TRUNCATE TABLE #inclusion_events;
DROP TABLE #inclusion_events;

TRUNCATE TABLE #qualified_events;
DROP TABLE #qualified_events;

TRUNCATE TABLE #included_events;
DROP TABLE #included_events;

TRUNCATE TABLE #Codesets;
DROP TABLE #Codesets;