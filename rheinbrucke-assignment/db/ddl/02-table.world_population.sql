-- Table: rheinbrucke.world_population

-- DROP TABLE rheinbrucke.world_population;

CREATE TABLE IF NOT EXISTS rheinbrucke.world_population
(
	rank							BIGINT PRIMARY KEY,
	CCA3							CHARACTER VARYING(100) NOT NULL,
	country							CHARACTER VARYING(100) NOT NULL,
	capital							CHARACTER VARYING(100) NOT NULL,
	continent						CHARACTER VARYING(100) NOT NULL,
	population_2022 				BIGINT NOT NULL,
	population_2020 				BIGINT NOT NULL,
	population_2015 				BIGINT NOT NULL,
	population_2010 				BIGINT NOT NULL,
	population_2000 				BIGINT NOT NULL,
	population_1990 				BIGINT NOT NULL,
	population_1980 				BIGINT NOT NULL,
	population_1970 				BIGINT NOT NULL,
	area							BIGINT NOT NULL,
	density							NUMERIC(22, 6) NOT NULL,
	growth_rate						NUMERIC(22, 6) NOT NULL,
	world_population_percentage		NUMERIC(22, 2) NOT NULL
)
TABLESPACE pg_default;

ALTER TABLE rheinbrucke.world_population
	OWNER to rheinbrucke_tracker;
