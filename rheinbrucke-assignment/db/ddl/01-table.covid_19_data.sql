-- Table: rheinbrucke.covid_19_data

-- DROP TABLE rheinbrucke.covid_19_data;

CREATE TABLE IF NOT EXISTS rheinbrucke.covid_19_data
(
	_id				    BIGINT PRIMARY KEY,
	observation_date	DATE NOT NULL,
	--observation_date	CHARACTER VARYING(50) NOT NULL,
	province			CHARACTER VARYING(200)  NULL,
	country			    CHARACTER VARYING(200) NOT NULL,
	last_updated_date	DATE NOT NULL,
	--last_updated_date	CHARACTER VARYING(50) NOT NULL,
	confirmed			BIGINT NOT NULL,
	deaths				BIGINT NOT NULL,
	recovered			BIGINT NOT NULL
)
TABLESPACE pg_default;

ALTER TABLE rheinbrucke.covid_19_data
	OWNER to rheinbrucke_tracker;