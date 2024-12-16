CREATE SCHEMA IF NOT EXISTS cricinfo
    AUTHORIZATION postgres;


CREATE SCHEMA IF NOT EXISTS cricinfo_staging;

-- Table: cricinfo.batting_scorecard

-- DROP TABLE IF EXISTS cricinfo.batting_scorecard;

CREATE TABLE IF NOT EXISTS cricinfo.batting_scorecard
(
    match_id integer NOT NULL,
    innings_number integer NOT NULL,
    player_id integer NOT NULL,
    runs integer,
    balls integer,
    fours integer,
    sixes integer,
    strike_rate numeric(5,2),
    in_at character varying(50) COLLATE pg_catalog."default",
    is_out boolean,
    bowler_id integer,
    wicket_id integer,
    wicket_number integer,
    wicket_fell_at character varying(50) COLLATE pg_catalog."default",
    CONSTRAINT batting_scorecard_pkey PRIMARY KEY (match_id, innings_number, player_id),
    CONSTRAINT batting_scorecard_bowler_id_fkey FOREIGN KEY (bowler_id)
        REFERENCES cricinfo.player (player_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE SET NULL,
    CONSTRAINT batting_scorecard_match_id_fkey FOREIGN KEY (match_id)
        REFERENCES cricinfo.match (match_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE CASCADE,
    CONSTRAINT batting_scorecard_player_id_fkey FOREIGN KEY (player_id)
        REFERENCES cricinfo.player (player_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE CASCADE
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS cricinfo.batting_scorecard
    OWNER to postgres;


-- Table: cricinfo.bowling_scorecard

-- DROP TABLE IF EXISTS cricinfo.bowling_scorecard;

CREATE TABLE IF NOT EXISTS cricinfo.bowling_scorecard
(
    match_id integer NOT NULL,
    innings_number integer NOT NULL,
    player_id integer NOT NULL,
    overs integer,
    runs integer,
    wickets integer,
    economy double precision,
    dots integer,
    fours integer,
    sixes integer,
    wides integer,
    no_balls integer,
    CONSTRAINT bowling_scorecard_pkey PRIMARY KEY (match_id, innings_number, player_id),
    CONSTRAINT bowling_scorecard_match_id_fkey FOREIGN KEY (match_id)
        REFERENCES cricinfo.match (match_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE CASCADE,
    CONSTRAINT bowling_scorecard_player_id_fkey FOREIGN KEY (player_id)
        REFERENCES cricinfo.player (player_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE CASCADE
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS cricinfo.bowling_scorecard
    OWNER to postgres;



-- Table: cricinfo.innings

-- DROP TABLE IF EXISTS cricinfo.innings;

CREATE TABLE IF NOT EXISTS cricinfo.innings
(
    match_id TEXT NOT NULL,
    team_id TEXT NOT NULL,
    runs integer,
    wickets integer,
    overs_played numeric(5,2),
    inning_number integer NOT NULL,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT innings_pkey PRIMARY KEY (match_id, team_id, inning_number),
    CONSTRAINT innings_match_id_fkey FOREIGN KEY (match_id)
        REFERENCES cricinfo.match (match_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE CASCADE,
    CONSTRAINT innings_team_id_fkey FOREIGN KEY (team_id)
        REFERENCES cricinfo.teams (team_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE CASCADE
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS cricinfo.innings
    OWNER to postgres;




-- Table: cricinfo.match

-- DROP TABLE IF EXISTS cricinfo.match;

CREATE TABLE IF NOT EXISTS cricinfo.match
(
    match_id TEXT NOT NULL ,
    series_id TEXT NOT NULL,
    match_no character varying(50) COLLATE pg_catalog."default",
    date date,
    venue character varying(150) COLLATE pg_catalog."default",
    team1 TEXT,
    team2 TEXT,
    match_url character varying(500) COLLATE pg_catalog."default",
    team1_innings TEXT,
    team2_innings TEXT,
    result TEXT,
    winner TEXT,
    win_by character varying(50) COLLATE pg_catalog."default",
    win_margin integer,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT match_pkey PRIMARY KEY (match_id),
    CONSTRAINT match_team1_fkey FOREIGN KEY (team1)
        REFERENCES cricinfo.teams (team_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE CASCADE,
    CONSTRAINT match_team2_fkey FOREIGN KEY (team2)
        REFERENCES cricinfo.teams (team_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE CASCADE,
    CONSTRAINT match_winner_fkey FOREIGN KEY (winner)
        REFERENCES cricinfo.teams (team_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE SET NULL,
    CONSTRAINT match_series_id_fkey FOREIGN KEY (series_id)
        REFERENCES cricinfo.series (series_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE CASCADE
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS cricinfo.match
    OWNER to postgres;



-- Table: cricinfo.player

-- DROP TABLE IF EXISTS cricinfo.player;

CREATE TABLE IF NOT EXISTS cricinfo.player
(
    player_id integer NOT NULL DEFAULT nextval('cricinfo.player_player_id_seq'::regclass),
    card_short character varying(50) COLLATE pg_catalog."default" NOT NULL,
    known_as character varying(100) COLLATE pg_catalog."default",
    team_id integer,
    dob date,
    batting_hand character varying(10) COLLATE pg_catalog."default",
    batting_style character varying(100) COLLATE pg_catalog."default",
    bowling_hand character varying(10) COLLATE pg_catalog."default",
    bowling_pacespin character varying(50) COLLATE pg_catalog."default",
    bowling_style character varying(100) COLLATE pg_catalog."default",
    CONSTRAINT player_pkey PRIMARY KEY (player_id),
    CONSTRAINT player_team_id_fkey FOREIGN KEY (team_id)
        REFERENCES cricinfo.teams (team_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE SET NULL
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS cricinfo.player
    OWNER to postgres;




-- Table: cricinfo.teams

-- DROP TABLE IF EXISTS cricinfo.teams;

CREATE TABLE IF NOT EXISTS cricinfo.teams
(
    team_id TEXT NOT NULL ,
    team_name character varying(100) COLLATE pg_catalog."default" NOT NULL,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT teams_pkey PRIMARY KEY (team_id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS cricinfo.teams
    OWNER to postgres;


-- Table: cricinfo.seasons

-- DROP TABLE IF EXISTS cricinfo.seasons;

CREATE TABLE cricinfo.seasons (
    period TEXT NOT NULL PRIMARY KEY,  -- Ensures unique identification
    link_prefix TEXT NOT NULL,          -- Will be updated if period exists
	last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP 
);




CREATE TABLE cricinfo.series (
    series_id TEXT NOT NULL PRIMARY KEY,  -- Ensures unique identification
    year TEXT,
    series_name TEXT,
    match_type TEXT,
    url TEXT,
	last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP 
);



TABLESPACE pg_default;

ALTER TABLE IF EXISTS cricinfo.seasons
    OWNER to postgres;