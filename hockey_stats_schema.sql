--
-- PostgreSQL database dump
--

\restrict kSpt9F3sRkfLe11aQnt0AombGdk7RjasJF1YqxuVxYXmRnUDBCpmGC6IfP6bHm6

-- Dumped from database version 17.6
-- Dumped by pg_dump version 18.0

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: admin; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA admin;


--
-- Name: derived; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA derived;


--
-- Name: dim; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA dim;


--
-- Name: mart; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA mart;


--
-- Name: raw; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA raw;


--
-- Name: scratch; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA scratch;


--
-- Name: unaccent; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS unaccent WITH SCHEMA public;


--
-- Name: EXTENSION unaccent; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON EXTENSION unaccent IS 'text search dictionary that removes accents';


--
-- Name: norm_name(text); Type: FUNCTION; Schema: dim; Owner: -
--

CREATE FUNCTION dim.norm_name(s text) RETURNS text
    LANGUAGE sql IMMUTABLE PARALLEL SAFE
    AS $$
  SELECT
    regexp_replace(
      regexp_replace(
        regexp_replace(
          regexp_replace(
            regexp_replace(
              regexp_replace(lower(coalesce(s,'')),
                E'\\b([od])[\\''’]', E'\\1', 'g'
              ),
              E'[\\.\\-’\\'']', ' ', 'g'
            ),
            E'[^a-z ]', '', 'g'
          ),
          E'\\s+', ' ', 'g'
        ),
        E'\\b([a-z])\\s+([a-z])\\b', E'\\1\\2', 'g'
      ),
      E'\\b([a-z])\\s+([a-z])\\b', E'\\1\\2', 'g'
    );
$$;


--
-- Name: norm_name(text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.norm_name(text) RETURNS text
    LANGUAGE sql IMMUTABLE PARALLEL SAFE
    AS $_$
  SELECT dim.norm_name($1);
$_$;


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: game_backup_20260110; Type: TABLE; Schema: admin; Owner: -
--

CREATE TABLE admin.game_backup_20260110 (
    game_id integer,
    season integer,
    type character varying,
    "date_time_GMT" timestamp without time zone,
    away_team_id integer,
    home_team_id integer,
    away_goals integer,
    home_goals integer,
    outcome character varying,
    home_rink_side_start character varying,
    venue character varying,
    venue_link character varying,
    venue_time_zone_id character varying,
    venue_time_zone_offset integer,
    venue_time_zone_tz character varying
);


--
-- Name: game_plays; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.game_plays (
    play_id character varying(20) NOT NULL,
    game_id bigint,
    team_id_for integer,
    team_id_against integer,
    event character varying(50),
    "secondaryType" character varying(50),
    x double precision,
    y double precision,
    period integer,
    "periodType" character varying(50),
    "periodTime" integer,
    "periodTimeRemaining" integer,
    "dateTime" timestamp without time zone,
    goals_away integer,
    goals_home integer,
    description character varying(255),
    st_x integer,
    st_y integer
);


--
-- Name: corsi_events_raw; Type: VIEW; Schema: derived; Owner: -
--

CREATE VIEW derived.corsi_events_raw AS
 SELECT play_id,
    game_id,
    team_id_for,
    team_id_against,
    event,
    "secondaryType" AS secondarytype,
    x,
    y,
    period,
    "periodType" AS periodtype,
    "periodTime" AS periodtime,
    "periodTime" AS seconds_in_period,
    (((period - 1) * 1200) + "periodTime") AS game_time_sec,
    "dateTime" AS datetime,
    goals_home,
    goals_away,
    description,
    st_x,
    st_y
   FROM raw.game_plays;


--
-- Name: corsi_events_corsi; Type: VIEW; Schema: derived; Owner: -
--

CREATE VIEW derived.corsi_events_corsi AS
 SELECT play_id,
    game_id,
    team_id_for,
    team_id_against,
    event,
    secondarytype,
    x,
    y,
    period,
    periodtype,
    periodtime,
    seconds_in_period,
    game_time_sec,
    datetime,
    goals_home,
    goals_away,
    description,
    st_x,
    st_y
   FROM derived.corsi_events_raw
  WHERE (((event)::text ~~* '%shot%'::text) OR ((event)::text ~~* 'goal%'::text));


--
-- Name: game_team_map_20182019; Type: TABLE; Schema: derived; Owner: -
--

CREATE TABLE derived.game_team_map_20182019 (
    game_id bigint,
    team_code character varying(3),
    team_id integer
);


--
-- Name: raw_pbp_20182019; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.raw_pbp_20182019 (
    season integer NOT NULL,
    game_id bigint NOT NULL,
    game_date date NOT NULL,
    session character varying(1) NOT NULL,
    event_index integer,
    game_period smallint,
    game_seconds integer,
    clock_time character varying(5),
    event_type character varying(50),
    event_description text,
    event_detail text,
    event_zone character varying(20),
    event_team character varying(3),
    event_player_1 text,
    event_player_2 text,
    event_player_3 text,
    event_length integer,
    coords_x double precision,
    coords_y double precision,
    num_on double precision,
    num_off double precision,
    players_on text,
    players_off text,
    home_on_1 text,
    home_on_2 text,
    home_on_3 text,
    home_on_4 text,
    home_on_5 text,
    home_on_6 text,
    home_on_7 text,
    away_on_1 text,
    away_on_2 text,
    away_on_3 text,
    away_on_4 text,
    away_on_5 text,
    away_on_6 text,
    away_on_7 text,
    home_goalie text,
    away_goalie text,
    home_team character varying(3),
    away_team character varying(3),
    home_skaters smallint,
    away_skaters smallint,
    home_score smallint,
    away_score smallint,
    game_score_state character varying(20),
    game_strength_state character varying(20),
    home_zone character varying(20),
    pbp_distance double precision,
    event_distance double precision,
    event_angle double precision,
    home_zonestart double precision,
    face_index integer,
    pen_index integer,
    shift_index integer,
    pred_goal double precision,
    id bigint NOT NULL
);


--
-- Name: game_plays_20182019_from_raw_pbp; Type: VIEW; Schema: derived; Owner: -
--

CREATE VIEW derived.game_plays_20182019_from_raw_pbp AS
 WITH pbp AS (
         SELECT r.season,
            r.game_id,
            r.event_index,
            r.id AS pbp_id,
            r.game_period AS period,
            r.game_seconds AS "time",
            (r.game_seconds - ((r.game_period - 1) * 1200)) AS "periodTime",
            r.event_type,
            r.event_team
           FROM raw.raw_pbp_20182019 r
          WHERE ((r.event_type)::text = ANY (ARRAY['SHOT'::text, 'MISS'::text, 'BLOCK'::text, 'GOAL'::text]))
        ), pbp_one AS (
         SELECT DISTINCT ON (p.game_id, p.event_index) p.season,
            p.game_id,
            p.event_index,
            p.pbp_id,
            p.period,
            p."time",
            p."periodTime",
            p.event_type,
            p.event_team
           FROM pbp p
          ORDER BY p.game_id, p.event_index, p.pbp_id
        ), pbp_mapped AS (
         SELECT p.season,
            p.game_id,
            p.event_index,
            p.pbp_id,
            p.period,
            p."time",
            p."periodTime",
            p.event_type,
            p.event_team,
            m_for.team_id AS team_id_for
           FROM (pbp_one p
             LEFT JOIN derived.game_team_map_20182019 m_for ON (((m_for.game_id = p.game_id) AND ((m_for.team_code)::text = (p.event_team)::text))))
        )
 SELECT season,
    game_id,
    period,
    "periodTime",
    "time",
        CASE event_type
            WHEN 'SHOT'::text THEN 'Shot'::text
            WHEN 'MISS'::text THEN 'Missed Shot'::text
            WHEN 'BLOCK'::text THEN 'Blocked Shot'::text
            WHEN 'GOAL'::text THEN 'Goal'::text
            ELSE NULL::text
        END AS event,
    team_id_for,
    ( SELECT m2.team_id
           FROM derived.game_team_map_20182019 m2
          WHERE ((m2.game_id = pbp_mapped.game_id) AND (m2.team_id <> pbp_mapped.team_id_for))
         LIMIT 1) AS team_id_against
   FROM pbp_mapped
  WHERE (team_id_for IS NOT NULL);


--
-- Name: game_team_map_20192020; Type: TABLE; Schema: derived; Owner: -
--

CREATE TABLE derived.game_team_map_20192020 (
    game_id bigint,
    team_code character varying(3),
    team_id integer
);


--
-- Name: raw_pbp_20192020; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.raw_pbp_20192020 (
    season integer NOT NULL,
    game_id bigint NOT NULL,
    game_date date NOT NULL,
    session character varying(1) NOT NULL,
    event_index integer,
    game_period smallint,
    game_seconds integer,
    clock_time character varying(5),
    event_type character varying(50),
    event_description text,
    event_detail text,
    event_zone character varying(20),
    event_team character varying(3),
    event_player_1 text,
    event_player_2 text,
    event_player_3 text,
    event_length integer,
    coords_x double precision,
    coords_y double precision,
    num_on double precision,
    num_off double precision,
    players_on text,
    players_off text,
    home_on_1 text,
    home_on_2 text,
    home_on_3 text,
    home_on_4 text,
    home_on_5 text,
    home_on_6 text,
    home_on_7 text,
    away_on_1 text,
    away_on_2 text,
    away_on_3 text,
    away_on_4 text,
    away_on_5 text,
    away_on_6 text,
    away_on_7 text,
    home_goalie text,
    away_goalie text,
    home_team character varying(3),
    away_team character varying(3),
    home_skaters smallint,
    away_skaters smallint,
    home_score smallint,
    away_score smallint,
    game_score_state character varying(20),
    game_strength_state character varying(20),
    home_zone character varying(20),
    pbp_distance double precision,
    event_distance double precision,
    event_angle double precision,
    home_zonestart double precision,
    face_index integer,
    pen_index integer,
    shift_index integer,
    pred_goal double precision,
    id bigint NOT NULL
);


--
-- Name: game_plays_20192020_from_raw_pbp; Type: VIEW; Schema: derived; Owner: -
--

CREATE VIEW derived.game_plays_20192020_from_raw_pbp AS
 WITH pbp AS (
         SELECT r.season,
            r.game_id,
            r.event_index,
            r.id AS pbp_id,
            r.game_period AS period,
            r.game_seconds AS "time",
            (r.game_seconds - ((r.game_period - 1) * 1200)) AS "periodTime",
            r.event_type,
            r.event_team
           FROM raw.raw_pbp_20192020 r
          WHERE ((r.event_type)::text = ANY (ARRAY['SHOT'::text, 'MISS'::text, 'BLOCK'::text, 'GOAL'::text]))
        ), pbp_one AS (
         SELECT DISTINCT ON (p.game_id, p.event_index) p.season,
            p.game_id,
            p.event_index,
            p.pbp_id,
            p.period,
            p."time",
            p."periodTime",
            p.event_type,
            p.event_team
           FROM pbp p
          ORDER BY p.game_id, p.event_index, p.pbp_id
        ), pbp_mapped AS (
         SELECT p.season,
            p.game_id,
            p.event_index,
            p.pbp_id,
            p.period,
            p."time",
            p."periodTime",
            p.event_type,
            p.event_team,
            m_for.team_id AS team_id_for
           FROM (pbp_one p
             LEFT JOIN derived.game_team_map_20192020 m_for ON (((m_for.game_id = p.game_id) AND ((m_for.team_code)::text = (p.event_team)::text))))
        )
 SELECT season,
    game_id,
    period,
    "periodTime",
    "time",
        CASE event_type
            WHEN 'SHOT'::text THEN 'Shot'::text
            WHEN 'MISS'::text THEN 'Missed Shot'::text
            WHEN 'BLOCK'::text THEN 'Blocked Shot'::text
            WHEN 'GOAL'::text THEN 'Goal'::text
            ELSE NULL::text
        END AS event,
    team_id_for,
    ( SELECT m2.team_id
           FROM derived.game_team_map_20192020 m2
          WHERE ((m2.game_id = pbp_mapped.game_id) AND (m2.team_id <> pbp_mapped.team_id_for))
         LIMIT 1) AS team_id_against
   FROM pbp_mapped
  WHERE (team_id_for IS NOT NULL);


--
-- Name: game_team_map_20202021; Type: TABLE; Schema: derived; Owner: -
--

CREATE TABLE derived.game_team_map_20202021 (
    game_id bigint,
    team_code character varying(3),
    team_id integer
);


--
-- Name: raw_pbp_20202021; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.raw_pbp_20202021 (
    season integer NOT NULL,
    game_id bigint NOT NULL,
    game_date date NOT NULL,
    session character varying(1) NOT NULL,
    event_index integer,
    game_period smallint,
    game_seconds integer,
    clock_time character varying(5),
    event_type character varying(50),
    event_description text,
    event_detail text,
    event_zone character varying(20),
    event_team character varying(3),
    event_player_1 text,
    event_player_2 text,
    event_player_3 text,
    event_length integer,
    coords_x double precision,
    coords_y double precision,
    num_on double precision,
    num_off double precision,
    players_on text,
    players_off text,
    home_on_1 text,
    home_on_2 text,
    home_on_3 text,
    home_on_4 text,
    home_on_5 text,
    home_on_6 text,
    home_on_7 text,
    away_on_1 text,
    away_on_2 text,
    away_on_3 text,
    away_on_4 text,
    away_on_5 text,
    away_on_6 text,
    away_on_7 text,
    home_goalie text,
    away_goalie text,
    home_team character varying(3),
    away_team character varying(3),
    home_skaters smallint,
    away_skaters smallint,
    home_score smallint,
    away_score smallint,
    game_score_state character varying(20),
    game_strength_state character varying(20),
    home_zone character varying(20),
    pbp_distance double precision,
    event_distance double precision,
    event_angle double precision,
    home_zonestart double precision,
    face_index integer,
    pen_index integer,
    shift_index integer,
    pred_goal double precision,
    id bigint NOT NULL
);


--
-- Name: game_plays_20202021_from_raw_pbp; Type: VIEW; Schema: derived; Owner: -
--

CREATE VIEW derived.game_plays_20202021_from_raw_pbp AS
 WITH pbp AS (
         SELECT r.season,
            r.game_id,
            r.event_index,
            r.id AS pbp_id,
            r.game_period AS period,
            r.game_seconds AS "time",
            (r.game_seconds - ((r.game_period - 1) * 1200)) AS "periodTime",
            r.event_type,
            r.event_team
           FROM raw.raw_pbp_20202021 r
          WHERE ((r.event_type)::text = ANY (ARRAY['SHOT'::text, 'MISS'::text, 'BLOCK'::text, 'GOAL'::text]))
        ), pbp_one AS (
         SELECT DISTINCT ON (p.game_id, p.event_index) p.season,
            p.game_id,
            p.event_index,
            p.pbp_id,
            p.period,
            p."time",
            p."periodTime",
            p.event_type,
            p.event_team
           FROM pbp p
          ORDER BY p.game_id, p.event_index, p.pbp_id
        ), pbp_mapped AS (
         SELECT p.season,
            p.game_id,
            p.event_index,
            p.pbp_id,
            p.period,
            p."time",
            p."periodTime",
            p.event_type,
            p.event_team,
            m_for.team_id AS team_id_for
           FROM (pbp_one p
             LEFT JOIN derived.game_team_map_20202021 m_for ON (((m_for.game_id = p.game_id) AND ((m_for.team_code)::text = (p.event_team)::text))))
        )
 SELECT season,
    game_id,
    period,
    "periodTime",
    "time",
        CASE event_type
            WHEN 'SHOT'::text THEN 'Shot'::text
            WHEN 'MISS'::text THEN 'Missed Shot'::text
            WHEN 'BLOCK'::text THEN 'Blocked Shot'::text
            WHEN 'GOAL'::text THEN 'Goal'::text
            ELSE NULL::text
        END AS event,
    team_id_for,
    ( SELECT m2.team_id
           FROM derived.game_team_map_20202021 m2
          WHERE ((m2.game_id = pbp_mapped.game_id) AND (m2.team_id <> pbp_mapped.team_id_for))
         LIMIT 1) AS team_id_against
   FROM pbp_mapped
  WHERE (team_id_for IS NOT NULL);


--
-- Name: game_team_map_20212022; Type: TABLE; Schema: derived; Owner: -
--

CREATE TABLE derived.game_team_map_20212022 (
    game_id bigint,
    team_code character varying(3),
    team_id integer
);


--
-- Name: raw_pbp_20212022; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.raw_pbp_20212022 (
    season integer NOT NULL,
    game_id bigint NOT NULL,
    game_date date NOT NULL,
    session character varying(1) NOT NULL,
    event_index integer,
    game_period smallint,
    game_seconds integer,
    clock_time character varying(5),
    event_type character varying(50),
    event_description text,
    event_detail text,
    event_zone character varying(20),
    event_team character varying(3),
    event_player_1 text,
    event_player_2 text,
    event_player_3 text,
    event_length integer,
    coords_x double precision,
    coords_y double precision,
    num_on double precision,
    num_off double precision,
    players_on text,
    players_off text,
    home_on_1 text,
    home_on_2 text,
    home_on_3 text,
    home_on_4 text,
    home_on_5 text,
    home_on_6 text,
    home_on_7 text,
    away_on_1 text,
    away_on_2 text,
    away_on_3 text,
    away_on_4 text,
    away_on_5 text,
    away_on_6 text,
    away_on_7 text,
    home_goalie text,
    away_goalie text,
    home_team character varying(3),
    away_team character varying(3),
    home_skaters smallint,
    away_skaters smallint,
    home_score smallint,
    away_score smallint,
    game_score_state character varying(20),
    game_strength_state character varying(20),
    home_zone character varying(20),
    pbp_distance double precision,
    event_distance double precision,
    event_angle double precision,
    home_zonestart double precision,
    face_index integer,
    pen_index integer,
    shift_index integer,
    pred_goal double precision,
    id bigint NOT NULL
);


--
-- Name: game_plays_20212022_from_raw_pbp; Type: VIEW; Schema: derived; Owner: -
--

CREATE VIEW derived.game_plays_20212022_from_raw_pbp AS
 WITH pbp AS (
         SELECT r.season,
            r.game_id,
            r.event_index,
            r.id AS pbp_id,
            r.game_period AS period,
            r.game_seconds AS "time",
            (r.game_seconds - ((r.game_period - 1) * 1200)) AS "periodTime",
            r.event_type,
            r.event_team
           FROM raw.raw_pbp_20212022 r
          WHERE ((r.event_type)::text = ANY (ARRAY['SHOT'::text, 'MISS'::text, 'BLOCK'::text, 'GOAL'::text]))
        ), pbp_one AS (
         SELECT DISTINCT ON (p.game_id, p.event_index) p.season,
            p.game_id,
            p.event_index,
            p.pbp_id,
            p.period,
            p."time",
            p."periodTime",
            p.event_type,
            p.event_team
           FROM pbp p
          ORDER BY p.game_id, p.event_index, p.pbp_id
        ), pbp_mapped AS (
         SELECT p.season,
            p.game_id,
            p.event_index,
            p.pbp_id,
            p.period,
            p."time",
            p."periodTime",
            p.event_type,
            p.event_team,
            m_for.team_id AS team_id_for
           FROM (pbp_one p
             LEFT JOIN derived.game_team_map_20212022 m_for ON (((m_for.game_id = p.game_id) AND ((m_for.team_code)::text = (p.event_team)::text))))
        )
 SELECT season,
    game_id,
    period,
    "periodTime",
    "time",
        CASE event_type
            WHEN 'SHOT'::text THEN 'Shot'::text
            WHEN 'MISS'::text THEN 'Missed Shot'::text
            WHEN 'BLOCK'::text THEN 'Blocked Shot'::text
            WHEN 'GOAL'::text THEN 'Goal'::text
            ELSE NULL::text
        END AS event,
    team_id_for,
    ( SELECT m2.team_id
           FROM derived.game_team_map_20212022 m2
          WHERE ((m2.game_id = pbp_mapped.game_id) AND (m2.team_id <> pbp_mapped.team_id_for))
         LIMIT 1) AS team_id_against
   FROM pbp_mapped
  WHERE (team_id_for IS NOT NULL);


--
-- Name: game_team_map_20222023; Type: TABLE; Schema: derived; Owner: -
--

CREATE TABLE derived.game_team_map_20222023 (
    game_id bigint,
    team_code character varying(3),
    team_id integer
);


--
-- Name: raw_pbp_20222023; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.raw_pbp_20222023 (
    season integer NOT NULL,
    game_id bigint NOT NULL,
    game_date date NOT NULL,
    session character varying(1) NOT NULL,
    event_index integer,
    game_period smallint,
    game_seconds integer,
    clock_time character varying(5),
    event_type character varying(50),
    event_description text,
    event_detail text,
    event_zone character varying(20),
    event_team character varying(3),
    event_player_1 text,
    event_player_2 text,
    event_player_3 text,
    event_length integer,
    coords_x double precision,
    coords_y double precision,
    num_on double precision,
    num_off double precision,
    players_on text,
    players_off text,
    home_on_1 text,
    home_on_2 text,
    home_on_3 text,
    home_on_4 text,
    home_on_5 text,
    home_on_6 text,
    home_on_7 text,
    away_on_1 text,
    away_on_2 text,
    away_on_3 text,
    away_on_4 text,
    away_on_5 text,
    away_on_6 text,
    away_on_7 text,
    home_goalie text,
    away_goalie text,
    home_team character varying(3),
    away_team character varying(3),
    home_skaters smallint,
    away_skaters smallint,
    home_score smallint,
    away_score smallint,
    game_score_state character varying(20),
    game_strength_state character varying(20),
    home_zone character varying(20),
    pbp_distance double precision,
    event_distance double precision,
    event_angle double precision,
    home_zonestart double precision,
    face_index integer,
    pen_index integer,
    shift_index integer,
    pred_goal double precision,
    id bigint NOT NULL
);


--
-- Name: game_plays_20222023_from_raw_pbp; Type: VIEW; Schema: derived; Owner: -
--

CREATE VIEW derived.game_plays_20222023_from_raw_pbp AS
 WITH pbp AS (
         SELECT r.season,
            r.game_id,
            r.event_index,
            r.id AS pbp_id,
            r.game_period AS period,
            r.game_seconds AS "time",
            (r.game_seconds - ((r.game_period - 1) * 1200)) AS "periodTime",
            r.event_type,
            r.event_team
           FROM raw.raw_pbp_20222023 r
          WHERE ((r.event_type)::text = ANY (ARRAY['SHOT'::text, 'MISS'::text, 'BLOCK'::text, 'GOAL'::text]))
        ), pbp_one AS (
         SELECT DISTINCT ON (p.game_id, p.event_index) p.season,
            p.game_id,
            p.event_index,
            p.pbp_id,
            p.period,
            p."time",
            p."periodTime",
            p.event_type,
            p.event_team
           FROM pbp p
          ORDER BY p.game_id, p.event_index, p.pbp_id
        ), pbp_mapped AS (
         SELECT p.season,
            p.game_id,
            p.event_index,
            p.pbp_id,
            p.period,
            p."time",
            p."periodTime",
            p.event_type,
            p.event_team,
            m_for.team_id AS team_id_for
           FROM (pbp_one p
             LEFT JOIN derived.game_team_map_20222023 m_for ON (((m_for.game_id = p.game_id) AND ((m_for.team_code)::text = (p.event_team)::text))))
        )
 SELECT season,
    game_id,
    period,
    "periodTime",
    "time",
        CASE event_type
            WHEN 'SHOT'::text THEN 'Shot'::text
            WHEN 'MISS'::text THEN 'Missed Shot'::text
            WHEN 'BLOCK'::text THEN 'Blocked Shot'::text
            WHEN 'GOAL'::text THEN 'Goal'::text
            ELSE NULL::text
        END AS event,
    team_id_for,
    ( SELECT m2.team_id
           FROM derived.game_team_map_20222023 m2
          WHERE ((m2.game_id = pbp_mapped.game_id) AND (m2.team_id <> pbp_mapped.team_id_for))
         LIMIT 1) AS team_id_against
   FROM pbp_mapped
  WHERE (team_id_for IS NOT NULL);


--
-- Name: game_team_map_20232024; Type: TABLE; Schema: derived; Owner: -
--

CREATE TABLE derived.game_team_map_20232024 (
    game_id bigint,
    team_code character varying(3),
    team_id integer
);


--
-- Name: raw_pbp_20232024; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.raw_pbp_20232024 (
    season integer NOT NULL,
    game_id bigint NOT NULL,
    game_date date NOT NULL,
    session character varying(1) NOT NULL,
    event_index integer,
    game_period smallint,
    game_seconds integer,
    clock_time character varying(5),
    event_type character varying(50),
    event_description text,
    event_detail text,
    event_zone character varying(20),
    event_team character varying(3),
    event_player_1 text,
    event_player_2 text,
    event_player_3 text,
    event_length integer,
    coords_x double precision,
    coords_y double precision,
    num_on double precision,
    num_off double precision,
    players_on text,
    players_off text,
    home_on_1 text,
    home_on_2 text,
    home_on_3 text,
    home_on_4 text,
    home_on_5 text,
    home_on_6 text,
    home_on_7 text,
    away_on_1 text,
    away_on_2 text,
    away_on_3 text,
    away_on_4 text,
    away_on_5 text,
    away_on_6 text,
    away_on_7 text,
    home_goalie text,
    away_goalie text,
    home_team character varying(3),
    away_team character varying(3),
    home_skaters smallint,
    away_skaters smallint,
    home_score smallint,
    away_score smallint,
    game_score_state character varying(20),
    game_strength_state character varying(20),
    home_zone character varying(20),
    pbp_distance double precision,
    event_distance double precision,
    event_angle double precision,
    home_zonestart double precision,
    face_index integer,
    pen_index integer,
    shift_index integer,
    pred_goal double precision,
    id bigint NOT NULL
);


--
-- Name: game_plays_20232024_from_raw_pbp; Type: VIEW; Schema: derived; Owner: -
--

CREATE VIEW derived.game_plays_20232024_from_raw_pbp AS
 WITH pbp AS (
         SELECT r.season,
            r.game_id,
            r.event_index,
            r.id AS pbp_id,
            r.game_period AS period,
            r.game_seconds AS "time",
            (r.game_seconds - ((r.game_period - 1) * 1200)) AS "periodTime",
            r.event_type,
            r.event_team
           FROM raw.raw_pbp_20232024 r
          WHERE ((r.event_type)::text = ANY (ARRAY['SHOT'::text, 'MISS'::text, 'BLOCK'::text, 'GOAL'::text]))
        ), pbp_one AS (
         SELECT DISTINCT ON (p.game_id, p.event_index) p.season,
            p.game_id,
            p.event_index,
            p.pbp_id,
            p.period,
            p."time",
            p."periodTime",
            p.event_type,
            p.event_team
           FROM pbp p
          ORDER BY p.game_id, p.event_index, p.pbp_id
        ), pbp_mapped AS (
         SELECT p.season,
            p.game_id,
            p.event_index,
            p.pbp_id,
            p.period,
            p."time",
            p."periodTime",
            p.event_type,
            p.event_team,
            m_for.team_id AS team_id_for
           FROM (pbp_one p
             LEFT JOIN derived.game_team_map_20232024 m_for ON (((m_for.game_id = p.game_id) AND ((m_for.team_code)::text = (p.event_team)::text))))
        )
 SELECT season,
    game_id,
    period,
    "periodTime",
    "time",
        CASE event_type
            WHEN 'SHOT'::text THEN 'Shot'::text
            WHEN 'MISS'::text THEN 'Missed Shot'::text
            WHEN 'BLOCK'::text THEN 'Blocked Shot'::text
            WHEN 'GOAL'::text THEN 'Goal'::text
            ELSE NULL::text
        END AS event,
    team_id_for,
    ( SELECT m2.team_id
           FROM derived.game_team_map_20232024 m2
          WHERE ((m2.game_id = pbp_mapped.game_id) AND (m2.team_id <> pbp_mapped.team_id_for))
         LIMIT 1) AS team_id_against
   FROM pbp_mapped
  WHERE (team_id_for IS NOT NULL);


--
-- Name: game_team_map_20242025; Type: TABLE; Schema: derived; Owner: -
--

CREATE TABLE derived.game_team_map_20242025 (
    game_id bigint,
    team_code character varying(3),
    team_id integer
);


--
-- Name: raw_pbp_20242025; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.raw_pbp_20242025 (
    season integer NOT NULL,
    game_id bigint NOT NULL,
    game_date date NOT NULL,
    session character varying(1) NOT NULL,
    event_index integer,
    game_period smallint,
    game_seconds integer,
    clock_time character varying(5),
    event_type character varying(50),
    event_description text,
    event_detail text,
    event_zone character varying(20),
    event_team character varying(3),
    event_player_1 text,
    event_player_2 text,
    event_player_3 text,
    event_length integer,
    coords_x double precision,
    coords_y double precision,
    num_on double precision,
    num_off double precision,
    players_on text,
    players_off text,
    home_on_1 text,
    home_on_2 text,
    home_on_3 text,
    home_on_4 text,
    home_on_5 text,
    home_on_6 text,
    home_on_7 text,
    away_on_1 text,
    away_on_2 text,
    away_on_3 text,
    away_on_4 text,
    away_on_5 text,
    away_on_6 text,
    away_on_7 text,
    home_goalie text,
    away_goalie text,
    home_team character varying(3),
    away_team character varying(3),
    home_skaters smallint,
    away_skaters smallint,
    home_score smallint,
    away_score smallint,
    game_score_state character varying(20),
    game_strength_state character varying(20),
    home_zone character varying(20),
    pbp_distance double precision,
    event_distance double precision,
    event_angle double precision,
    home_zonestart double precision,
    face_index integer,
    pen_index integer,
    shift_index integer,
    pred_goal double precision,
    id bigint NOT NULL
);


--
-- Name: game_plays_20242025_from_raw_pbp; Type: VIEW; Schema: derived; Owner: -
--

CREATE VIEW derived.game_plays_20242025_from_raw_pbp AS
 WITH pbp AS (
         SELECT r.season,
            r.game_id,
            r.event_index,
            r.id AS pbp_id,
            r.game_period AS period,
            r.game_seconds AS "time",
            (r.game_seconds - ((r.game_period - 1) * 1200)) AS "periodTime",
            r.event_type,
            r.event_team
           FROM raw.raw_pbp_20242025 r
          WHERE ((r.event_type)::text = ANY (ARRAY['SHOT'::text, 'MISS'::text, 'BLOCK'::text, 'GOAL'::text]))
        ), pbp_one AS (
         SELECT DISTINCT ON (p.game_id, p.event_index) p.season,
            p.game_id,
            p.event_index,
            p.pbp_id,
            p.period,
            p."time",
            p."periodTime",
            p.event_type,
            p.event_team
           FROM pbp p
          ORDER BY p.game_id, p.event_index, p.pbp_id
        ), pbp_mapped AS (
         SELECT p.season,
            p.game_id,
            p.event_index,
            p.pbp_id,
            p.period,
            p."time",
            p."periodTime",
            p.event_type,
            p.event_team,
            m_for.team_id AS team_id_for
           FROM (pbp_one p
             LEFT JOIN derived.game_team_map_20242025 m_for ON (((m_for.game_id = p.game_id) AND ((m_for.team_code)::text = (p.event_team)::text))))
        )
 SELECT season,
    game_id,
    period,
    "periodTime",
    "time",
        CASE event_type
            WHEN 'SHOT'::text THEN 'Shot'::text
            WHEN 'MISS'::text THEN 'Missed Shot'::text
            WHEN 'BLOCK'::text THEN 'Blocked Shot'::text
            WHEN 'GOAL'::text THEN 'Goal'::text
            ELSE NULL::text
        END AS event,
    team_id_for,
    ( SELECT m2.team_id
           FROM derived.game_team_map_20242025 m2
          WHERE ((m2.game_id = pbp_mapped.game_id) AND (m2.team_id <> pbp_mapped.team_id_for))
         LIMIT 1) AS team_id_against
   FROM pbp_mapped
  WHERE (team_id_for IS NOT NULL);


--
-- Name: pbp_corsi_20182019; Type: VIEW; Schema: derived; Owner: -
--

CREATE VIEW derived.pbp_corsi_20182019 AS
 SELECT season,
    game_id,
    game_period,
    game_seconds,
    event_type,
    event_team,
    home_team,
    away_team,
    event_zone,
    coords_x,
    coords_y,
    home_score,
    away_score,
    game_strength_state
   FROM raw.raw_pbp_20182019
  WHERE ((event_type)::text = ANY (ARRAY[('SHOT'::character varying)::text, ('MISS'::character varying)::text, ('BLOCK'::character varying)::text, ('GOAL'::character varying)::text]));


--
-- Name: raw_corsi_20152016; Type: TABLE; Schema: derived; Owner: -
--

CREATE TABLE derived.raw_corsi_20152016 (
    game_id bigint,
    player_id bigint,
    team_id integer,
    corsi_for double precision,
    corsi_against double precision,
    corsi double precision,
    "CF_Percent" double precision
);


--
-- Name: raw_corsi_20162017; Type: TABLE; Schema: derived; Owner: -
--

CREATE TABLE derived.raw_corsi_20162017 (
    game_id bigint,
    player_id bigint,
    team_id integer,
    corsi_for double precision,
    corsi_against double precision,
    corsi double precision,
    "CF_Percent" double precision
);


--
-- Name: raw_corsi_20172018; Type: TABLE; Schema: derived; Owner: -
--

CREATE TABLE derived.raw_corsi_20172018 (
    game_id bigint,
    player_id bigint,
    team_id integer,
    corsi_for double precision,
    corsi_against double precision,
    corsi double precision,
    "CF_Percent" double precision
);


--
-- Name: player_game_corsi_2015_2018; Type: VIEW; Schema: derived; Owner: -
--

CREATE VIEW derived.player_game_corsi_2015_2018 AS
 SELECT raw_corsi_20152016.game_id,
    raw_corsi_20152016.player_id,
    raw_corsi_20152016.team_id,
    raw_corsi_20152016.corsi_for,
    raw_corsi_20152016.corsi_against,
    raw_corsi_20152016.corsi,
    raw_corsi_20152016."CF_Percent" AS cf_percent
   FROM derived.raw_corsi_20152016
UNION ALL
 SELECT raw_corsi_20162017.game_id,
    raw_corsi_20162017.player_id,
    raw_corsi_20162017.team_id,
    raw_corsi_20162017.corsi_for,
    raw_corsi_20162017.corsi_against,
    raw_corsi_20162017.corsi,
    raw_corsi_20162017."CF_Percent" AS cf_percent
   FROM derived.raw_corsi_20162017
UNION ALL
 SELECT raw_corsi_20172018.game_id,
    raw_corsi_20172018.player_id,
    raw_corsi_20172018.team_id,
    raw_corsi_20172018.corsi_for,
    raw_corsi_20172018.corsi_against,
    raw_corsi_20172018.corsi,
    raw_corsi_20172018."CF_Percent" AS cf_percent
   FROM derived.raw_corsi_20172018;


--
-- Name: aggregated_corsi_20152016; Type: TABLE; Schema: mart; Owner: -
--

CREATE TABLE mart.aggregated_corsi_20152016 (
    player_id bigint,
    "firstName" text,
    "lastName" text,
    team_id bigint,
    corsi_for double precision,
    corsi_against double precision,
    corsi double precision,
    "CF_Percent" double precision,
    "timeOnIce" double precision,
    game_count bigint,
    "capHit" double precision
);


--
-- Name: aggregated_corsi_20162017; Type: TABLE; Schema: mart; Owner: -
--

CREATE TABLE mart.aggregated_corsi_20162017 (
    player_id bigint,
    "firstName" text,
    "lastName" text,
    team_id bigint,
    corsi_for double precision,
    corsi_against double precision,
    corsi double precision,
    "CF_Percent" double precision,
    "timeOnIce" double precision,
    game_count bigint,
    "capHit" double precision
);


--
-- Name: aggregated_corsi_20172018; Type: TABLE; Schema: mart; Owner: -
--

CREATE TABLE mart.aggregated_corsi_20172018 (
    player_id bigint,
    "firstName" text,
    "lastName" text,
    team_id bigint,
    corsi_for double precision,
    corsi_against double precision,
    corsi double precision,
    "CF_Percent" double precision,
    "timeOnIce" double precision,
    game_count bigint,
    "capHit" double precision
);


--
-- Name: player_season_corsi_2015_2018; Type: VIEW; Schema: derived; Owner: -
--

CREATE VIEW derived.player_season_corsi_2015_2018 AS
 SELECT aggregated_corsi_20152016.player_id,
    aggregated_corsi_20152016.team_id,
    '20152016'::text AS season,
    aggregated_corsi_20152016.corsi_for,
    aggregated_corsi_20152016.corsi_against,
    aggregated_corsi_20152016.corsi,
    aggregated_corsi_20152016."CF_Percent" AS cf_percent,
    aggregated_corsi_20152016."timeOnIce" AS time_on_ice,
    aggregated_corsi_20152016.game_count,
    aggregated_corsi_20152016."capHit" AS cap_hit
   FROM mart.aggregated_corsi_20152016
UNION ALL
 SELECT aggregated_corsi_20162017.player_id,
    aggregated_corsi_20162017.team_id,
    '20162017'::text AS season,
    aggregated_corsi_20162017.corsi_for,
    aggregated_corsi_20162017.corsi_against,
    aggregated_corsi_20162017.corsi,
    aggregated_corsi_20162017."CF_Percent" AS cf_percent,
    aggregated_corsi_20162017."timeOnIce" AS time_on_ice,
    aggregated_corsi_20162017.game_count,
    aggregated_corsi_20162017."capHit" AS cap_hit
   FROM mart.aggregated_corsi_20162017
UNION ALL
 SELECT aggregated_corsi_20172018.player_id,
    aggregated_corsi_20172018.team_id,
    '20172018'::text AS season,
    aggregated_corsi_20172018.corsi_for,
    aggregated_corsi_20172018.corsi_against,
    aggregated_corsi_20172018.corsi,
    aggregated_corsi_20172018."CF_Percent" AS cf_percent,
    aggregated_corsi_20172018."timeOnIce" AS time_on_ice,
    aggregated_corsi_20172018.game_count,
    aggregated_corsi_20172018."capHit" AS cap_hit
   FROM mart.aggregated_corsi_20172018;


--
-- Name: raw_corsi_20182019; Type: TABLE; Schema: derived; Owner: -
--

CREATE TABLE derived.raw_corsi_20182019 (
    game_id bigint,
    player_id bigint,
    team_id integer,
    corsi_for double precision,
    corsi_against double precision,
    corsi double precision,
    "CF_Percent" double precision
);


--
-- Name: raw_corsi_20192020; Type: TABLE; Schema: derived; Owner: -
--

CREATE TABLE derived.raw_corsi_20192020 (
    game_id bigint,
    player_id bigint,
    team_id integer,
    corsi_for double precision,
    corsi_against double precision,
    corsi double precision,
    "CF_Percent" double precision
);


--
-- Name: raw_corsi_20202021; Type: TABLE; Schema: derived; Owner: -
--

CREATE TABLE derived.raw_corsi_20202021 (
    game_id bigint,
    player_id bigint,
    team_id integer,
    corsi_for double precision,
    corsi_against double precision,
    corsi double precision,
    "CF_Percent" double precision
);


--
-- Name: raw_corsi_20212022; Type: TABLE; Schema: derived; Owner: -
--

CREATE TABLE derived.raw_corsi_20212022 (
    game_id bigint,
    player_id bigint,
    team_id integer,
    corsi_for double precision,
    corsi_against double precision,
    corsi double precision,
    "CF_Percent" double precision
);


--
-- Name: raw_corsi_20222023; Type: TABLE; Schema: derived; Owner: -
--

CREATE TABLE derived.raw_corsi_20222023 (
    game_id bigint,
    player_id bigint,
    team_id integer,
    corsi_for double precision,
    corsi_against double precision,
    corsi double precision,
    "CF_Percent" double precision
);


--
-- Name: raw_corsi_20232024; Type: TABLE; Schema: derived; Owner: -
--

CREATE TABLE derived.raw_corsi_20232024 (
    game_id bigint,
    player_id bigint,
    team_id integer,
    corsi_for double precision,
    corsi_against double precision,
    corsi double precision,
    "CF_Percent" double precision
);


--
-- Name: raw_corsi_20242025; Type: TABLE; Schema: derived; Owner: -
--

CREATE TABLE derived.raw_corsi_20242025 (
    game_id bigint,
    player_id bigint,
    team_id integer,
    corsi_for double precision,
    corsi_against double precision,
    corsi double precision,
    "CF_Percent" double precision
);


--
-- Name: player_info; Type: TABLE; Schema: dim; Owner: -
--

CREATE TABLE dim.player_info (
    player_id bigint NOT NULL,
    "firstName" character varying(50),
    "lastName" character varying(50),
    nationality character varying(50),
    "birthCity" character varying(50),
    "primaryPosition" character varying(50),
    "birthDate" timestamp without time zone,
    "birthStateProvince" character varying(50),
    height double precision,
    height_cm double precision,
    weight double precision,
    "shootCatches" character varying(10)
);


--
-- Name: dim_player_name_unique; Type: VIEW; Schema: dim; Owner: -
--

CREATE VIEW dim.dim_player_name_unique AS
 WITH base AS (
         SELECT player_info.player_id,
            dim.norm_name(
                CASE
                    WHEN ((player_info."lastName")::text ~~ '% %'::text) THEN (player_info."lastName")::text
                    ELSE concat_ws(' '::text, player_info."firstName", player_info."lastName")
                END) AS name_key
           FROM dim.player_info
          WHERE (player_info.player_id IS NOT NULL)
        ), ranked AS (
         SELECT base.player_id,
            base.name_key,
            row_number() OVER (PARTITION BY base.name_key ORDER BY base.player_id DESC) AS rn
           FROM base
        )
 SELECT name_key,
    player_id
   FROM ranked
  WHERE (rn = 1);


--
-- Name: player_name_override; Type: TABLE; Schema: dim; Owner: -
--

CREATE TABLE dim.player_name_override (
    raw_player text NOT NULL,
    team text NOT NULL,
    season integer NOT NULL,
    player_id bigint NOT NULL,
    note text
);


--
-- Name: raw_shifts_20182019; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.raw_shifts_20182019 (
    row_num integer,
    player text NOT NULL,
    team_num character varying(10) NOT NULL,
    "position" character varying(1) NOT NULL,
    game_id bigint NOT NULL,
    game_date date NOT NULL,
    season integer NOT NULL,
    session character varying(1) NOT NULL,
    team character varying(3) NOT NULL,
    opponent character varying(3) NOT NULL,
    is_home boolean NOT NULL,
    game_period smallint NOT NULL,
    shift_num integer NOT NULL,
    seconds_start integer NOT NULL,
    seconds_end integer NOT NULL,
    seconds_duration integer NOT NULL,
    shift_start character varying(5),
    shift_end character varying(5),
    duration character varying(5),
    shift_mod integer NOT NULL,
    id bigint NOT NULL
);


--
-- Name: raw_shifts_20192020; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.raw_shifts_20192020 (
    row_num integer,
    player text NOT NULL,
    team_num character varying(10) NOT NULL,
    "position" character varying(1) NOT NULL,
    game_id bigint NOT NULL,
    game_date date NOT NULL,
    season integer NOT NULL,
    session character varying(1) NOT NULL,
    team character varying(3) NOT NULL,
    opponent character varying(3) NOT NULL,
    is_home boolean NOT NULL,
    game_period smallint NOT NULL,
    shift_num integer NOT NULL,
    seconds_start integer NOT NULL,
    seconds_end integer NOT NULL,
    seconds_duration integer NOT NULL,
    shift_start character varying(5),
    shift_end character varying(5),
    duration character varying(5),
    shift_mod integer NOT NULL,
    id bigint NOT NULL
);


--
-- Name: raw_shifts_20202021; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.raw_shifts_20202021 (
    row_num integer,
    player text NOT NULL,
    team_num character varying(10) NOT NULL,
    "position" character varying(1) NOT NULL,
    game_id bigint NOT NULL,
    game_date date NOT NULL,
    season integer NOT NULL,
    session character varying(1) NOT NULL,
    team character varying(3) NOT NULL,
    opponent character varying(3) NOT NULL,
    is_home boolean NOT NULL,
    game_period smallint NOT NULL,
    shift_num integer NOT NULL,
    seconds_start integer NOT NULL,
    seconds_end integer NOT NULL,
    seconds_duration integer NOT NULL,
    shift_start character varying(5),
    shift_end character varying(5),
    duration character varying(5),
    shift_mod integer NOT NULL,
    id bigint NOT NULL
);


--
-- Name: raw_shifts_20212022; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.raw_shifts_20212022 (
    row_num integer,
    player text NOT NULL,
    team_num character varying(10) NOT NULL,
    "position" character varying(1) NOT NULL,
    game_id bigint NOT NULL,
    game_date date NOT NULL,
    season integer NOT NULL,
    session character varying(1) NOT NULL,
    team character varying(3) NOT NULL,
    opponent character varying(3) NOT NULL,
    is_home boolean NOT NULL,
    game_period smallint NOT NULL,
    shift_num integer NOT NULL,
    seconds_start integer NOT NULL,
    seconds_end integer NOT NULL,
    seconds_duration integer NOT NULL,
    shift_start character varying(5),
    shift_end character varying(5),
    duration character varying(5),
    shift_mod integer NOT NULL,
    id bigint NOT NULL
);


--
-- Name: raw_shifts_20222023; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.raw_shifts_20222023 (
    row_num integer,
    player text NOT NULL,
    team_num character varying(10) NOT NULL,
    "position" character varying(1) NOT NULL,
    game_id bigint NOT NULL,
    game_date date NOT NULL,
    season integer NOT NULL,
    session character varying(1) NOT NULL,
    team character varying(3) NOT NULL,
    opponent character varying(3) NOT NULL,
    is_home boolean NOT NULL,
    game_period smallint NOT NULL,
    shift_num integer NOT NULL,
    seconds_start integer NOT NULL,
    seconds_end integer NOT NULL,
    seconds_duration integer NOT NULL,
    shift_start character varying(5),
    shift_end character varying(5),
    duration character varying(5),
    shift_mod integer NOT NULL,
    id bigint NOT NULL
);


--
-- Name: raw_shifts_20232024; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.raw_shifts_20232024 (
    row_num integer,
    player text NOT NULL,
    team_num character varying(10) NOT NULL,
    "position" character varying(1) NOT NULL,
    game_id bigint NOT NULL,
    game_date date NOT NULL,
    season integer NOT NULL,
    session character varying(1) NOT NULL,
    team character varying(3) NOT NULL,
    opponent character varying(3) NOT NULL,
    is_home boolean NOT NULL,
    game_period smallint NOT NULL,
    shift_num integer NOT NULL,
    seconds_start integer NOT NULL,
    seconds_end integer NOT NULL,
    seconds_duration integer NOT NULL,
    shift_start character varying(5),
    shift_end character varying(5),
    duration character varying(5),
    shift_mod integer NOT NULL,
    id bigint NOT NULL
);


--
-- Name: raw_shifts_20242025; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.raw_shifts_20242025 (
    row_num integer,
    player text NOT NULL,
    team_num character varying(10) NOT NULL,
    "position" character varying(1) NOT NULL,
    game_id bigint NOT NULL,
    game_date date NOT NULL,
    season integer NOT NULL,
    session character varying(1) NOT NULL,
    team character varying(3) NOT NULL,
    opponent character varying(3) NOT NULL,
    is_home boolean NOT NULL,
    game_period smallint NOT NULL,
    shift_num integer NOT NULL,
    seconds_start integer NOT NULL,
    seconds_end integer NOT NULL,
    seconds_duration integer NOT NULL,
    shift_start character varying(5),
    shift_end character varying(5),
    duration character varying(5),
    shift_mod integer NOT NULL,
    id bigint NOT NULL
);


--
-- Name: raw_shifts_all; Type: VIEW; Schema: raw; Owner: -
--

CREATE VIEW raw.raw_shifts_all AS
 SELECT raw_shifts_20182019.row_num,
    raw_shifts_20182019.player,
    raw_shifts_20182019.team_num,
    raw_shifts_20182019."position",
    raw_shifts_20182019.game_id,
    raw_shifts_20182019.game_date,
    raw_shifts_20182019.season,
    raw_shifts_20182019.session,
    raw_shifts_20182019.team,
    raw_shifts_20182019.opponent,
    raw_shifts_20182019.is_home,
    raw_shifts_20182019.game_period,
    raw_shifts_20182019.shift_num,
    raw_shifts_20182019.seconds_start,
    raw_shifts_20182019.seconds_end,
    raw_shifts_20182019.seconds_duration,
    raw_shifts_20182019.shift_start,
    raw_shifts_20182019.shift_end,
    raw_shifts_20182019.duration,
    raw_shifts_20182019.shift_mod,
    raw_shifts_20182019.id
   FROM raw.raw_shifts_20182019
UNION ALL
 SELECT raw_shifts_20192020.row_num,
    raw_shifts_20192020.player,
    raw_shifts_20192020.team_num,
    raw_shifts_20192020."position",
    raw_shifts_20192020.game_id,
    raw_shifts_20192020.game_date,
    raw_shifts_20192020.season,
    raw_shifts_20192020.session,
    raw_shifts_20192020.team,
    raw_shifts_20192020.opponent,
    raw_shifts_20192020.is_home,
    raw_shifts_20192020.game_period,
    raw_shifts_20192020.shift_num,
    raw_shifts_20192020.seconds_start,
    raw_shifts_20192020.seconds_end,
    raw_shifts_20192020.seconds_duration,
    raw_shifts_20192020.shift_start,
    raw_shifts_20192020.shift_end,
    raw_shifts_20192020.duration,
    raw_shifts_20192020.shift_mod,
    raw_shifts_20192020.id
   FROM raw.raw_shifts_20192020
UNION ALL
 SELECT raw_shifts_20202021.row_num,
    raw_shifts_20202021.player,
    raw_shifts_20202021.team_num,
    raw_shifts_20202021."position",
    raw_shifts_20202021.game_id,
    raw_shifts_20202021.game_date,
    raw_shifts_20202021.season,
    raw_shifts_20202021.session,
    raw_shifts_20202021.team,
    raw_shifts_20202021.opponent,
    raw_shifts_20202021.is_home,
    raw_shifts_20202021.game_period,
    raw_shifts_20202021.shift_num,
    raw_shifts_20202021.seconds_start,
    raw_shifts_20202021.seconds_end,
    raw_shifts_20202021.seconds_duration,
    raw_shifts_20202021.shift_start,
    raw_shifts_20202021.shift_end,
    raw_shifts_20202021.duration,
    raw_shifts_20202021.shift_mod,
    raw_shifts_20202021.id
   FROM raw.raw_shifts_20202021
UNION ALL
 SELECT raw_shifts_20212022.row_num,
    raw_shifts_20212022.player,
    raw_shifts_20212022.team_num,
    raw_shifts_20212022."position",
    raw_shifts_20212022.game_id,
    raw_shifts_20212022.game_date,
    raw_shifts_20212022.season,
    raw_shifts_20212022.session,
    raw_shifts_20212022.team,
    raw_shifts_20212022.opponent,
    raw_shifts_20212022.is_home,
    raw_shifts_20212022.game_period,
    raw_shifts_20212022.shift_num,
    raw_shifts_20212022.seconds_start,
    raw_shifts_20212022.seconds_end,
    raw_shifts_20212022.seconds_duration,
    raw_shifts_20212022.shift_start,
    raw_shifts_20212022.shift_end,
    raw_shifts_20212022.duration,
    raw_shifts_20212022.shift_mod,
    raw_shifts_20212022.id
   FROM raw.raw_shifts_20212022
UNION ALL
 SELECT raw_shifts_20222023.row_num,
    raw_shifts_20222023.player,
    raw_shifts_20222023.team_num,
    raw_shifts_20222023."position",
    raw_shifts_20222023.game_id,
    raw_shifts_20222023.game_date,
    raw_shifts_20222023.season,
    raw_shifts_20222023.session,
    raw_shifts_20222023.team,
    raw_shifts_20222023.opponent,
    raw_shifts_20222023.is_home,
    raw_shifts_20222023.game_period,
    raw_shifts_20222023.shift_num,
    raw_shifts_20222023.seconds_start,
    raw_shifts_20222023.seconds_end,
    raw_shifts_20222023.seconds_duration,
    raw_shifts_20222023.shift_start,
    raw_shifts_20222023.shift_end,
    raw_shifts_20222023.duration,
    raw_shifts_20222023.shift_mod,
    raw_shifts_20222023.id
   FROM raw.raw_shifts_20222023
UNION ALL
 SELECT raw_shifts_20232024.row_num,
    raw_shifts_20232024.player,
    raw_shifts_20232024.team_num,
    raw_shifts_20232024."position",
    raw_shifts_20232024.game_id,
    raw_shifts_20232024.game_date,
    raw_shifts_20232024.season,
    raw_shifts_20232024.session,
    raw_shifts_20232024.team,
    raw_shifts_20232024.opponent,
    raw_shifts_20232024.is_home,
    raw_shifts_20232024.game_period,
    raw_shifts_20232024.shift_num,
    raw_shifts_20232024.seconds_start,
    raw_shifts_20232024.seconds_end,
    raw_shifts_20232024.seconds_duration,
    raw_shifts_20232024.shift_start,
    raw_shifts_20232024.shift_end,
    raw_shifts_20232024.duration,
    raw_shifts_20232024.shift_mod,
    raw_shifts_20232024.id
   FROM raw.raw_shifts_20232024
UNION ALL
 SELECT raw_shifts_20242025.row_num,
    raw_shifts_20242025.player,
    raw_shifts_20242025.team_num,
    raw_shifts_20242025."position",
    raw_shifts_20242025.game_id,
    raw_shifts_20242025.game_date,
    raw_shifts_20242025.season,
    raw_shifts_20242025.session,
    raw_shifts_20242025.team,
    raw_shifts_20242025.opponent,
    raw_shifts_20242025.is_home,
    raw_shifts_20242025.game_period,
    raw_shifts_20242025.shift_num,
    raw_shifts_20242025.seconds_start,
    raw_shifts_20242025.seconds_end,
    raw_shifts_20242025.seconds_duration,
    raw_shifts_20242025.shift_start,
    raw_shifts_20242025.shift_end,
    raw_shifts_20242025.duration,
    raw_shifts_20242025.shift_mod,
    raw_shifts_20242025.id
   FROM raw.raw_shifts_20242025;


--
-- Name: raw_shifts_resolved; Type: VIEW; Schema: derived; Owner: -
--

CREATE VIEW derived.raw_shifts_resolved AS
 SELECT rs.row_num,
    rs.player,
    rs.team_num,
    rs."position",
    rs.game_id,
    rs.game_date,
    rs.season,
    rs.session,
    rs.team,
    rs.opponent,
    rs.is_home,
    rs.game_period,
    rs.shift_num,
    rs.seconds_start,
    rs.seconds_end,
    rs.seconds_duration,
    rs.shift_start,
    rs.shift_end,
    rs.duration,
    rs.shift_mod,
    rs.id,
    COALESCE(o.player_id, dpu.player_id) AS player_id_resolved
   FROM ((raw.raw_shifts_all rs
     LEFT JOIN dim.player_name_override o ON (((o.raw_player = rs.player) AND (o.team = (rs.team)::text) AND (o.season = rs.season))))
     LEFT JOIN dim.dim_player_name_unique dpu ON ((dpu.name_key = dim.norm_name(rs.player))));


--
-- Name: team_corsi_games_20182019; Type: TABLE; Schema: derived; Owner: -
--

CREATE TABLE derived.team_corsi_games_20182019 (
    game_id bigint,
    team_id integer,
    cf double precision,
    ca double precision,
    corsi double precision,
    cf_pct double precision
);


--
-- Name: team_corsi_games_20192020; Type: TABLE; Schema: derived; Owner: -
--

CREATE TABLE derived.team_corsi_games_20192020 (
    game_id bigint,
    team_id integer,
    cf double precision,
    ca double precision,
    corsi double precision,
    cf_pct double precision
);


--
-- Name: team_corsi_games_20202021; Type: TABLE; Schema: derived; Owner: -
--

CREATE TABLE derived.team_corsi_games_20202021 (
    game_id bigint,
    team_id integer,
    cf double precision,
    ca double precision,
    corsi double precision,
    cf_pct double precision
);


--
-- Name: team_corsi_games_20212022; Type: TABLE; Schema: derived; Owner: -
--

CREATE TABLE derived.team_corsi_games_20212022 (
    game_id bigint,
    team_id integer,
    cf double precision,
    ca double precision,
    corsi double precision,
    cf_pct double precision
);


--
-- Name: team_corsi_games_20222023; Type: TABLE; Schema: derived; Owner: -
--

CREATE TABLE derived.team_corsi_games_20222023 (
    game_id bigint,
    team_id integer,
    cf double precision,
    ca double precision,
    corsi double precision,
    cf_pct double precision
);


--
-- Name: team_corsi_games_20232024; Type: TABLE; Schema: derived; Owner: -
--

CREATE TABLE derived.team_corsi_games_20232024 (
    game_id bigint,
    team_id integer,
    cf double precision,
    ca double precision,
    corsi double precision,
    cf_pct double precision
);


--
-- Name: team_corsi_games_20242025; Type: TABLE; Schema: derived; Owner: -
--

CREATE TABLE derived.team_corsi_games_20242025 (
    game_id bigint,
    team_id integer,
    cf double precision,
    ca double precision,
    corsi double precision,
    cf_pct double precision
);


--
-- Name: team_event_totals_games_20152016; Type: TABLE; Schema: derived; Owner: -
--

CREATE TABLE derived.team_event_totals_games_20152016 (
    team_id integer,
    total_goals integer,
    total_shots integer,
    total_missed_shots integer,
    total_blocked_shots_for integer,
    total_goals_against integer,
    total_shots_against integer,
    total_missed_shots_against integer,
    total_blocked_shots_against integer,
    game_id bigint
);


--
-- Name: team_event_totals_games_20162017; Type: TABLE; Schema: derived; Owner: -
--

CREATE TABLE derived.team_event_totals_games_20162017 (
    team_id integer,
    total_goals integer,
    total_shots integer,
    total_missed_shots integer,
    total_blocked_shots_for integer,
    total_goals_against integer,
    total_shots_against integer,
    total_missed_shots_against integer,
    total_blocked_shots_against integer,
    game_id bigint
);


--
-- Name: team_event_totals_games_20172018; Type: TABLE; Schema: derived; Owner: -
--

CREATE TABLE derived.team_event_totals_games_20172018 (
    team_id integer,
    total_goals integer,
    total_shots integer,
    total_missed_shots integer,
    total_blocked_shots_for integer,
    total_goals_against integer,
    total_shots_against integer,
    total_missed_shots_against integer,
    total_blocked_shots_against integer,
    game_id bigint
);


--
-- Name: team_event_totals_games_20182019; Type: TABLE; Schema: derived; Owner: -
--

CREATE TABLE derived.team_event_totals_games_20182019 (
    game_id bigint,
    team_id integer,
    goals bigint,
    shots bigint,
    missed_shots bigint,
    blocked_shots_for bigint,
    cf bigint,
    goals_against bigint,
    shots_against bigint,
    missed_shots_against bigint,
    blocked_shots_against bigint,
    ca bigint,
    cf_pct numeric
);


--
-- Name: team_event_totals_games_20192020; Type: TABLE; Schema: derived; Owner: -
--

CREATE TABLE derived.team_event_totals_games_20192020 (
    game_id bigint,
    team_id integer,
    goals bigint,
    shots bigint,
    missed_shots bigint,
    blocked_shots_for bigint,
    cf bigint,
    goals_against bigint,
    shots_against bigint,
    missed_shots_against bigint,
    blocked_shots_against bigint,
    ca bigint,
    cf_pct numeric
);


--
-- Name: team_event_totals_games_20202021; Type: TABLE; Schema: derived; Owner: -
--

CREATE TABLE derived.team_event_totals_games_20202021 (
    game_id bigint,
    team_id integer,
    goals bigint,
    shots bigint,
    missed_shots bigint,
    blocked_shots_for bigint,
    cf bigint,
    goals_against bigint,
    shots_against bigint,
    missed_shots_against bigint,
    blocked_shots_against bigint,
    ca bigint,
    cf_pct numeric
);


--
-- Name: team_event_totals_games_20212022; Type: TABLE; Schema: derived; Owner: -
--

CREATE TABLE derived.team_event_totals_games_20212022 (
    game_id bigint,
    team_id integer,
    goals bigint,
    shots bigint,
    missed_shots bigint,
    blocked_shots_for bigint,
    cf bigint,
    goals_against bigint,
    shots_against bigint,
    missed_shots_against bigint,
    blocked_shots_against bigint,
    ca bigint,
    cf_pct numeric
);


--
-- Name: team_event_totals_games_20222023; Type: TABLE; Schema: derived; Owner: -
--

CREATE TABLE derived.team_event_totals_games_20222023 (
    game_id bigint,
    team_id integer,
    goals bigint,
    shots bigint,
    missed_shots bigint,
    blocked_shots_for bigint,
    cf bigint,
    goals_against bigint,
    shots_against bigint,
    missed_shots_against bigint,
    blocked_shots_against bigint,
    ca bigint,
    cf_pct numeric
);


--
-- Name: team_event_totals_games_20232024; Type: TABLE; Schema: derived; Owner: -
--

CREATE TABLE derived.team_event_totals_games_20232024 (
    game_id bigint,
    team_id integer,
    goals bigint,
    shots bigint,
    missed_shots bigint,
    blocked_shots_for bigint,
    cf bigint,
    goals_against bigint,
    shots_against bigint,
    missed_shots_against bigint,
    blocked_shots_against bigint,
    ca bigint,
    cf_pct numeric
);


--
-- Name: team_event_totals_games_20242025; Type: TABLE; Schema: derived; Owner: -
--

CREATE TABLE derived.team_event_totals_games_20242025 (
    game_id bigint,
    team_id integer,
    goals bigint,
    shots bigint,
    missed_shots bigint,
    blocked_shots_for bigint,
    cf bigint,
    goals_against bigint,
    shots_against bigint,
    missed_shots_against bigint,
    blocked_shots_against bigint,
    ca bigint,
    cf_pct numeric
);


--
-- Name: dim_player_name; Type: TABLE; Schema: dim; Owner: -
--

CREATE TABLE dim.dim_player_name (
    player_id bigint NOT NULL,
    "firstName" character varying(50),
    "lastName" character varying(50),
    full_name text,
    name_key text
);


--
-- Name: dim_player_name_unique_table; Type: TABLE; Schema: dim; Owner: -
--

CREATE TABLE dim.dim_player_name_unique_table (
    player_id bigint,
    "firstName" character varying(50),
    "lastName" character varying(50),
    name_key text
);


--
-- Name: dim_team_code; Type: TABLE; Schema: dim; Owner: -
--

CREATE TABLE dim.dim_team_code (
    team_code character varying(3),
    team_id integer
);


--
-- Name: player_cap_hit_20152016; Type: TABLE; Schema: dim; Owner: -
--

CREATE TABLE dim.player_cap_hit_20152016 (
    player_id double precision,
    "firstName" text,
    "lastName" text,
    "capHit" double precision,
    spotrac_url text
);


--
-- Name: player_cap_hit_20162017; Type: TABLE; Schema: dim; Owner: -
--

CREATE TABLE dim.player_cap_hit_20162017 (
    player_id double precision,
    "firstName" text,
    "lastName" text,
    "capHit" double precision,
    spotrac_url text
);


--
-- Name: player_cap_hit_20172018; Type: TABLE; Schema: dim; Owner: -
--

CREATE TABLE dim.player_cap_hit_20172018 (
    player_id double precision,
    "firstName" text,
    "lastName" text,
    "capHit" double precision,
    spotrac_url text
);


--
-- Name: player_cap_hit_20182019; Type: TABLE; Schema: dim; Owner: -
--

CREATE TABLE dim.player_cap_hit_20182019 (
    player_id double precision,
    "firstName" text,
    "lastName" text,
    "capHit" double precision,
    spotrac_url text
);


--
-- Name: player_cap_hit_20192020; Type: TABLE; Schema: dim; Owner: -
--

CREATE TABLE dim.player_cap_hit_20192020 (
    player_id double precision,
    "firstName" text,
    "lastName" text,
    "capHit" double precision,
    spotrac_url text
);


--
-- Name: player_cap_hit_20202021; Type: TABLE; Schema: dim; Owner: -
--

CREATE TABLE dim.player_cap_hit_20202021 (
    player_id double precision,
    "firstName" text,
    "lastName" text,
    "capHit" double precision,
    spotrac_url text
);


--
-- Name: player_cap_hit_20212022; Type: TABLE; Schema: dim; Owner: -
--

CREATE TABLE dim.player_cap_hit_20212022 (
    player_id double precision,
    "firstName" text,
    "lastName" text,
    "capHit" double precision,
    spotrac_url text
);


--
-- Name: player_cap_hit_20222023; Type: TABLE; Schema: dim; Owner: -
--

CREATE TABLE dim.player_cap_hit_20222023 (
    player_id double precision,
    "firstName" text,
    "lastName" text,
    "capHit" double precision,
    spotrac_url text
);


--
-- Name: player_cap_hit_20232024; Type: TABLE; Schema: dim; Owner: -
--

CREATE TABLE dim.player_cap_hit_20232024 (
    player_id double precision,
    "firstName" text,
    "lastName" text,
    "capHit" double precision,
    spotrac_url text
);


--
-- Name: player_cap_hit_20242025; Type: TABLE; Schema: dim; Owner: -
--

CREATE TABLE dim.player_cap_hit_20242025 (
    player_id double precision,
    "firstName" text,
    "lastName" text,
    "capHit" double precision,
    spotrac_url text
);


--
-- Name: player_cap_hit_spotrac; Type: TABLE; Schema: dim; Owner: -
--

CREATE TABLE dim.player_cap_hit_spotrac (
    season_year integer NOT NULL,
    spotrac_url text NOT NULL,
    "firstName" text,
    "lastName" text,
    "capHit" double precision,
    scraped_at timestamp without time zone DEFAULT now()
);


--
-- Name: player_info_table_test_player_id_seq; Type: SEQUENCE; Schema: dim; Owner: -
--

CREATE SEQUENCE dim.player_info_table_test_player_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: player_info_table_test_player_id_seq; Type: SEQUENCE OWNED BY; Schema: dim; Owner: -
--

ALTER SEQUENCE dim.player_info_table_test_player_id_seq OWNED BY dim.player_info.player_id;


--
-- Name: aggregated_corsi_20182019; Type: TABLE; Schema: mart; Owner: -
--

CREATE TABLE mart.aggregated_corsi_20182019 (
    player_id bigint,
    "firstName" character varying(50),
    "lastName" character varying(50),
    team_id integer,
    cf_total double precision,
    ca_total double precision,
    corsi_total double precision,
    cf_percent double precision,
    toi_sec numeric,
    cf60 double precision,
    ca60 double precision,
    game_count numeric,
    multi_team boolean
);


--
-- Name: aggregated_corsi_20192020; Type: TABLE; Schema: mart; Owner: -
--

CREATE TABLE mart.aggregated_corsi_20192020 (
    player_id bigint,
    "firstName" character varying(50),
    "lastName" character varying(50),
    team_id integer,
    cf_total double precision,
    ca_total double precision,
    corsi_total double precision,
    cf_percent double precision,
    toi_sec numeric,
    cf60 double precision,
    ca60 double precision,
    game_count numeric,
    multi_team boolean
);


--
-- Name: aggregated_corsi_20202021; Type: TABLE; Schema: mart; Owner: -
--

CREATE TABLE mart.aggregated_corsi_20202021 (
    player_id bigint,
    "firstName" character varying(50),
    "lastName" character varying(50),
    team_id integer,
    cf_total double precision,
    ca_total double precision,
    corsi_total double precision,
    cf_percent double precision,
    toi_sec numeric,
    cf60 double precision,
    ca60 double precision,
    game_count numeric,
    multi_team boolean
);


--
-- Name: aggregated_corsi_20212022; Type: TABLE; Schema: mart; Owner: -
--

CREATE TABLE mart.aggregated_corsi_20212022 (
    player_id bigint,
    "firstName" character varying(50),
    "lastName" character varying(50),
    team_id integer,
    cf_total double precision,
    ca_total double precision,
    corsi_total double precision,
    cf_percent double precision,
    toi_sec numeric,
    cf60 double precision,
    ca60 double precision,
    game_count numeric,
    multi_team boolean
);


--
-- Name: aggregated_corsi_20222023; Type: TABLE; Schema: mart; Owner: -
--

CREATE TABLE mart.aggregated_corsi_20222023 (
    player_id bigint,
    "firstName" character varying(50),
    "lastName" character varying(50),
    team_id integer,
    cf_total double precision,
    ca_total double precision,
    corsi_total double precision,
    cf_percent double precision,
    toi_sec numeric,
    cf60 double precision,
    ca60 double precision,
    game_count numeric,
    multi_team boolean
);


--
-- Name: aggregated_corsi_20232024; Type: TABLE; Schema: mart; Owner: -
--

CREATE TABLE mart.aggregated_corsi_20232024 (
    player_id bigint,
    "firstName" character varying(50),
    "lastName" character varying(50),
    team_id integer,
    cf_total double precision,
    ca_total double precision,
    corsi_total double precision,
    cf_percent double precision,
    toi_sec numeric,
    cf60 double precision,
    ca60 double precision,
    game_count numeric,
    multi_team boolean
);


--
-- Name: aggregated_corsi_20242025; Type: TABLE; Schema: mart; Owner: -
--

CREATE TABLE mart.aggregated_corsi_20242025 (
    player_id bigint,
    "firstName" character varying(50),
    "lastName" character varying(50),
    team_id integer,
    cf_total double precision,
    ca_total double precision,
    corsi_total double precision,
    cf_percent double precision,
    toi_sec numeric,
    cf60 double precision,
    ca60 double precision,
    game_count numeric,
    multi_team boolean
);


--
-- Name: player_game_es_20152016; Type: TABLE; Schema: mart; Owner: -
--

CREATE TABLE mart.player_game_es_20152016 (
    game_id bigint,
    player_id bigint,
    team_id integer,
    cf double precision,
    ca double precision,
    toi_sec integer,
    cf60 double precision,
    ca60 double precision,
    cf_percent double precision
);


--
-- Name: player_game_es_20162017; Type: TABLE; Schema: mart; Owner: -
--

CREATE TABLE mart.player_game_es_20162017 (
    game_id bigint,
    player_id bigint,
    team_id integer,
    cf double precision,
    ca double precision,
    toi_sec integer,
    cf60 double precision,
    ca60 double precision,
    cf_percent double precision
);


--
-- Name: player_game_es_20172018; Type: TABLE; Schema: mart; Owner: -
--

CREATE TABLE mart.player_game_es_20172018 (
    game_id bigint,
    player_id bigint,
    team_id integer,
    cf double precision,
    ca double precision,
    toi_sec integer,
    cf60 double precision,
    ca60 double precision,
    cf_percent double precision
);


--
-- Name: player_game_es_20182019; Type: TABLE; Schema: mart; Owner: -
--

CREATE TABLE mart.player_game_es_20182019 (
    game_id bigint,
    player_id bigint,
    team_id integer,
    cf double precision,
    ca double precision,
    toi_sec bigint,
    cf60 double precision,
    ca60 double precision,
    cf_percent double precision
);


--
-- Name: player_game_es_20192020; Type: TABLE; Schema: mart; Owner: -
--

CREATE TABLE mart.player_game_es_20192020 (
    game_id bigint,
    player_id bigint,
    team_id integer,
    cf double precision,
    ca double precision,
    toi_sec bigint,
    cf60 double precision,
    ca60 double precision,
    cf_percent double precision
);


--
-- Name: player_game_es_20202021; Type: TABLE; Schema: mart; Owner: -
--

CREATE TABLE mart.player_game_es_20202021 (
    game_id bigint,
    player_id bigint,
    team_id integer,
    cf double precision,
    ca double precision,
    toi_sec bigint,
    cf60 double precision,
    ca60 double precision,
    cf_percent double precision
);


--
-- Name: player_game_es_20212022; Type: TABLE; Schema: mart; Owner: -
--

CREATE TABLE mart.player_game_es_20212022 (
    game_id bigint,
    player_id bigint,
    team_id integer,
    cf double precision,
    ca double precision,
    toi_sec bigint,
    cf60 double precision,
    ca60 double precision,
    cf_percent double precision
);


--
-- Name: player_game_es_20222023; Type: TABLE; Schema: mart; Owner: -
--

CREATE TABLE mart.player_game_es_20222023 (
    game_id bigint,
    player_id bigint,
    team_id integer,
    cf double precision,
    ca double precision,
    toi_sec bigint,
    cf60 double precision,
    ca60 double precision,
    cf_percent double precision
);


--
-- Name: player_game_es_20232024; Type: TABLE; Schema: mart; Owner: -
--

CREATE TABLE mart.player_game_es_20232024 (
    game_id bigint,
    player_id bigint,
    team_id integer,
    cf double precision,
    ca double precision,
    toi_sec bigint,
    cf60 double precision,
    ca60 double precision,
    cf_percent double precision
);


--
-- Name: player_game_es_20242025; Type: TABLE; Schema: mart; Owner: -
--

CREATE TABLE mart.player_game_es_20242025 (
    game_id bigint,
    player_id bigint,
    team_id integer,
    cf double precision,
    ca double precision,
    toi_sec bigint,
    cf60 double precision,
    ca60 double precision,
    cf_percent double precision
);


--
-- Name: team_corsi_season_20182019; Type: TABLE; Schema: mart; Owner: -
--

CREATE TABLE mart.team_corsi_season_20182019 (
    team_id integer,
    cf double precision,
    ca double precision,
    corsi double precision,
    cf_pct double precision
);


--
-- Name: team_corsi_season_20192020; Type: TABLE; Schema: mart; Owner: -
--

CREATE TABLE mart.team_corsi_season_20192020 (
    team_id integer,
    cf double precision,
    ca double precision,
    corsi double precision,
    cf_pct double precision
);


--
-- Name: team_corsi_season_20202021; Type: TABLE; Schema: mart; Owner: -
--

CREATE TABLE mart.team_corsi_season_20202021 (
    team_id integer,
    cf double precision,
    ca double precision,
    corsi double precision,
    cf_pct double precision
);


--
-- Name: team_corsi_season_20212022; Type: TABLE; Schema: mart; Owner: -
--

CREATE TABLE mart.team_corsi_season_20212022 (
    team_id integer,
    cf double precision,
    ca double precision,
    corsi double precision,
    cf_pct double precision
);


--
-- Name: team_corsi_season_20222023; Type: TABLE; Schema: mart; Owner: -
--

CREATE TABLE mart.team_corsi_season_20222023 (
    team_id integer,
    cf double precision,
    ca double precision,
    corsi double precision,
    cf_pct double precision
);


--
-- Name: team_corsi_season_20232024; Type: TABLE; Schema: mart; Owner: -
--

CREATE TABLE mart.team_corsi_season_20232024 (
    team_id integer,
    cf double precision,
    ca double precision,
    corsi double precision,
    cf_pct double precision
);


--
-- Name: team_corsi_season_20242025; Type: TABLE; Schema: mart; Owner: -
--

CREATE TABLE mart.team_corsi_season_20242025 (
    team_id integer,
    cf double precision,
    ca double precision,
    corsi double precision,
    cf_pct double precision
);


--
-- Name: team_event_totals_season_20152016; Type: TABLE; Schema: mart; Owner: -
--

CREATE TABLE mart.team_event_totals_season_20152016 (
    team_id integer,
    total_goals integer,
    total_shots integer,
    total_missed_shots integer,
    total_blocked_shots_for integer,
    total_goals_against integer,
    total_shots_against integer,
    total_missed_shots_against integer,
    total_blocked_shots_against integer,
    "CF" integer,
    "CA" integer,
    "CF%" double precision
);


--
-- Name: team_event_totals_season_20162017; Type: TABLE; Schema: mart; Owner: -
--

CREATE TABLE mart.team_event_totals_season_20162017 (
    team_id integer,
    total_goals integer,
    total_shots integer,
    total_missed_shots integer,
    total_blocked_shots_for integer,
    total_goals_against integer,
    total_shots_against integer,
    total_missed_shots_against integer,
    total_blocked_shots_against integer,
    "CF" integer,
    "CA" integer,
    "CF%" double precision
);


--
-- Name: team_event_totals_season_20172018; Type: TABLE; Schema: mart; Owner: -
--

CREATE TABLE mart.team_event_totals_season_20172018 (
    team_id integer,
    total_goals integer,
    total_shots integer,
    total_missed_shots integer,
    total_blocked_shots_for integer,
    total_goals_against integer,
    total_shots_against integer,
    total_missed_shots_against integer,
    total_blocked_shots_against integer,
    "CF" integer,
    "CA" integer,
    "CF%" double precision
);


--
-- Name: team_event_totals_season_20182019; Type: TABLE; Schema: mart; Owner: -
--

CREATE TABLE mart.team_event_totals_season_20182019 (
    team_id integer,
    goals numeric,
    shots numeric,
    missed_shots numeric,
    blocked_shots_for numeric,
    cf numeric,
    goals_against numeric,
    shots_against numeric,
    missed_shots_against numeric,
    blocked_shots_against numeric,
    ca numeric,
    cf_pct numeric
);


--
-- Name: team_event_totals_season_20192020; Type: TABLE; Schema: mart; Owner: -
--

CREATE TABLE mart.team_event_totals_season_20192020 (
    team_id integer,
    goals numeric,
    shots numeric,
    missed_shots numeric,
    blocked_shots_for numeric,
    cf numeric,
    goals_against numeric,
    shots_against numeric,
    missed_shots_against numeric,
    blocked_shots_against numeric,
    ca numeric,
    cf_pct numeric
);


--
-- Name: team_event_totals_season_20202021; Type: TABLE; Schema: mart; Owner: -
--

CREATE TABLE mart.team_event_totals_season_20202021 (
    team_id integer,
    goals numeric,
    shots numeric,
    missed_shots numeric,
    blocked_shots_for numeric,
    cf numeric,
    goals_against numeric,
    shots_against numeric,
    missed_shots_against numeric,
    blocked_shots_against numeric,
    ca numeric,
    cf_pct numeric
);


--
-- Name: team_event_totals_season_20212022; Type: TABLE; Schema: mart; Owner: -
--

CREATE TABLE mart.team_event_totals_season_20212022 (
    team_id integer,
    goals numeric,
    shots numeric,
    missed_shots numeric,
    blocked_shots_for numeric,
    cf numeric,
    goals_against numeric,
    shots_against numeric,
    missed_shots_against numeric,
    blocked_shots_against numeric,
    ca numeric,
    cf_pct numeric
);


--
-- Name: team_event_totals_season_20222023; Type: TABLE; Schema: mart; Owner: -
--

CREATE TABLE mart.team_event_totals_season_20222023 (
    team_id integer,
    goals numeric,
    shots numeric,
    missed_shots numeric,
    blocked_shots_for numeric,
    cf numeric,
    goals_against numeric,
    shots_against numeric,
    missed_shots_against numeric,
    blocked_shots_against numeric,
    ca numeric,
    cf_pct numeric
);


--
-- Name: team_event_totals_season_20232024; Type: TABLE; Schema: mart; Owner: -
--

CREATE TABLE mart.team_event_totals_season_20232024 (
    team_id integer,
    goals numeric,
    shots numeric,
    missed_shots numeric,
    blocked_shots_for numeric,
    cf numeric,
    goals_against numeric,
    shots_against numeric,
    missed_shots_against numeric,
    blocked_shots_against numeric,
    ca numeric,
    cf_pct numeric
);


--
-- Name: team_event_totals_season_20242025; Type: TABLE; Schema: mart; Owner: -
--

CREATE TABLE mart.team_event_totals_season_20242025 (
    team_id integer,
    goals numeric,
    shots numeric,
    missed_shots numeric,
    blocked_shots_for numeric,
    cf numeric,
    goals_against numeric,
    shots_against numeric,
    missed_shots_against numeric,
    blocked_shots_against numeric,
    ca numeric,
    cf_pct numeric
);


--
-- Name: team_summary_20152016; Type: TABLE; Schema: mart; Owner: -
--

CREATE TABLE mart.team_summary_20152016 (
    season integer NOT NULL,
    team_name text,
    abbr text,
    team_id integer NOT NULL,
    gp integer,
    w integer,
    l integer,
    otl integer,
    pts integer,
    total_cap_raw text,
    total_cap numeric
);


--
-- Name: team_summary_20162017; Type: TABLE; Schema: mart; Owner: -
--

CREATE TABLE mart.team_summary_20162017 (
    season bigint NOT NULL,
    team_name text,
    abbr text,
    team_id bigint NOT NULL,
    gp integer,
    w integer,
    l integer,
    otl integer,
    pts integer,
    total_cap_raw text,
    total_cap numeric
);


--
-- Name: team_summary_20172018; Type: TABLE; Schema: mart; Owner: -
--

CREATE TABLE mart.team_summary_20172018 (
    season bigint NOT NULL,
    team_name text,
    abbr text,
    team_id bigint NOT NULL,
    gp integer,
    w integer,
    l integer,
    otl integer,
    pts integer,
    total_cap_raw text,
    total_cap numeric
);


--
-- Name: team_summary_20182019; Type: TABLE; Schema: mart; Owner: -
--

CREATE TABLE mart.team_summary_20182019 (
    team_name text,
    abbr text,
    team_id bigint,
    gp bigint,
    w bigint,
    l bigint,
    otl bigint,
    pts bigint,
    total_cap_raw text,
    total_cap double precision
);


--
-- Name: team_summary_20192020; Type: TABLE; Schema: mart; Owner: -
--

CREATE TABLE mart.team_summary_20192020 (
    team_name text,
    abbr text,
    team_id bigint,
    gp bigint,
    w bigint,
    l bigint,
    otl bigint,
    pts bigint,
    total_cap_raw text,
    total_cap double precision
);


--
-- Name: team_summary_20202021; Type: TABLE; Schema: mart; Owner: -
--

CREATE TABLE mart.team_summary_20202021 (
    team_name text,
    abbr text,
    team_id bigint,
    gp bigint,
    w bigint,
    l bigint,
    otl bigint,
    pts bigint,
    total_cap_raw text,
    total_cap double precision
);


--
-- Name: team_summary_20212022; Type: TABLE; Schema: mart; Owner: -
--

CREATE TABLE mart.team_summary_20212022 (
    team_name text,
    abbr text,
    team_id bigint,
    gp bigint,
    w bigint,
    l bigint,
    otl bigint,
    pts bigint,
    total_cap_raw text,
    total_cap double precision
);


--
-- Name: team_summary_20222023; Type: TABLE; Schema: mart; Owner: -
--

CREATE TABLE mart.team_summary_20222023 (
    team_name text,
    abbr text,
    team_id bigint,
    gp bigint,
    w bigint,
    l bigint,
    otl bigint,
    pts bigint,
    total_cap_raw text,
    total_cap double precision
);


--
-- Name: team_summary_20232024; Type: TABLE; Schema: mart; Owner: -
--

CREATE TABLE mart.team_summary_20232024 (
    team_name text,
    abbr text,
    team_id bigint,
    gp bigint,
    w bigint,
    l bigint,
    otl bigint,
    pts bigint,
    total_cap_raw text,
    total_cap double precision
);


--
-- Name: team_summary_20242025; Type: TABLE; Schema: mart; Owner: -
--

CREATE TABLE mart.team_summary_20242025 (
    team_name text,
    abbr text,
    team_id bigint,
    gp bigint,
    w bigint,
    l bigint,
    otl bigint,
    pts bigint,
    total_cap_raw text,
    total_cap double precision
);


--
-- Name: dim.player_cap_hit_20152016; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw."dim.player_cap_hit_20152016" (
    player_id bigint,
    "firstName" character varying(50),
    "lastName" character varying(50),
    "capHit" double precision,
    spotrac_url character varying(255)
);


--
-- Name: dim.player_cap_hit_20162017; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw."dim.player_cap_hit_20162017" (
    player_id bigint,
    "firstName" character varying(50),
    "lastName" character varying(50),
    "capHit" double precision,
    spotrac_url character varying(255)
);


--
-- Name: dim.player_cap_hit_20172018; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw."dim.player_cap_hit_20172018" (
    player_id bigint,
    "firstName" character varying(50),
    "lastName" character varying(50),
    "capHit" double precision,
    spotrac_url character varying(255)
);


--
-- Name: dim.player_cap_hit_20182019; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw."dim.player_cap_hit_20182019" (
    player_id bigint,
    "firstName" character varying(50),
    "lastName" character varying(50),
    "capHit" double precision,
    spotrac_url character varying(255)
);


--
-- Name: dim.player_cap_hit_20192020; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw."dim.player_cap_hit_20192020" (
    player_id bigint,
    "firstName" character varying(50),
    "lastName" character varying(50),
    "capHit" double precision,
    spotrac_url character varying(255)
);


--
-- Name: dim.player_cap_hit_20202021; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw."dim.player_cap_hit_20202021" (
    player_id bigint,
    "firstName" character varying(50),
    "lastName" character varying(50),
    "capHit" double precision,
    spotrac_url character varying(255)
);


--
-- Name: dim.player_cap_hit_20212022; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw."dim.player_cap_hit_20212022" (
    player_id bigint,
    "firstName" character varying(50),
    "lastName" character varying(50),
    "capHit" double precision,
    spotrac_url character varying(255)
);


--
-- Name: dim.player_cap_hit_20222023; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw."dim.player_cap_hit_20222023" (
    player_id bigint,
    "firstName" character varying(50),
    "lastName" character varying(50),
    "capHit" double precision,
    spotrac_url character varying(255)
);


--
-- Name: dim.player_cap_hit_20232024; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw."dim.player_cap_hit_20232024" (
    player_id bigint,
    "firstName" character varying(50),
    "lastName" character varying(50),
    "capHit" double precision,
    spotrac_url character varying(255)
);


--
-- Name: dim.player_cap_hit_20242025; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw."dim.player_cap_hit_20242025" (
    player_id bigint,
    "firstName" character varying(50),
    "lastName" character varying(50),
    "capHit" double precision,
    spotrac_url character varying(255)
);


--
-- Name: game; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.game (
    game_id integer NOT NULL,
    season integer,
    type character varying,
    "date_time_GMT" timestamp without time zone,
    away_team_id integer,
    home_team_id integer,
    away_goals integer,
    home_goals integer,
    outcome character varying,
    home_rink_side_start character varying,
    venue character varying,
    venue_link character varying,
    venue_time_zone_id character varying,
    venue_time_zone_offset integer,
    venue_time_zone_tz character varying
);


--
-- Name: game_plays_players; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.game_plays_players (
    play_id character varying(20),
    game_id bigint NOT NULL,
    player_id bigint NOT NULL,
    "playerType" character varying(20)
);


--
-- Name: game_shifts; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.game_shifts (
    game_id bigint,
    player_id bigint,
    period integer,
    shift_start integer,
    shift_end integer
);


--
-- Name: game_shifts_final; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.game_shifts_final (
    game_id bigint NOT NULL,
    player_id bigint NOT NULL,
    period smallint NOT NULL,
    shift_start integer NOT NULL,
    shift_end integer NOT NULL,
    raw_shift_id bigint
);


--
-- Name: game_skater_stats; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.game_skater_stats (
    game_id bigint,
    player_id bigint,
    team_id integer,
    "timeOnIce" integer,
    assists integer,
    goals integer,
    shots integer,
    hits integer,
    "powerPlayGoals" integer,
    "powerPlayAssists" integer,
    "penaltyMinutes" integer,
    "faceOffWins" integer,
    "faceoffTaken" integer,
    takeaways integer,
    giveaways integer,
    "shortHandedGoals" integer,
    "shortHandedAssists" integer,
    blocked integer,
    "plusMinus" integer,
    "evenTimeOnIce" integer,
    "shortHandedTimeOnIce" integer,
    "powerPlayTimeOnIce" integer
);


--
-- Name: raw_pbp_20182019_id_seq; Type: SEQUENCE; Schema: raw; Owner: -
--

ALTER TABLE raw.raw_pbp_20182019 ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME raw.raw_pbp_20182019_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: raw_pbp_20192020_id_seq; Type: SEQUENCE; Schema: raw; Owner: -
--

ALTER TABLE raw.raw_pbp_20192020 ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME raw.raw_pbp_20192020_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: raw_pbp_20202021_id_seq; Type: SEQUENCE; Schema: raw; Owner: -
--

ALTER TABLE raw.raw_pbp_20202021 ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME raw.raw_pbp_20202021_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: raw_pbp_20212022_id_seq; Type: SEQUENCE; Schema: raw; Owner: -
--

ALTER TABLE raw.raw_pbp_20212022 ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME raw.raw_pbp_20212022_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: raw_pbp_20222023_id_seq; Type: SEQUENCE; Schema: raw; Owner: -
--

ALTER TABLE raw.raw_pbp_20222023 ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME raw.raw_pbp_20222023_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: raw_pbp_20232024_id_seq; Type: SEQUENCE; Schema: raw; Owner: -
--

ALTER TABLE raw.raw_pbp_20232024 ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME raw.raw_pbp_20232024_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: raw_pbp_20242025_id_seq; Type: SEQUENCE; Schema: raw; Owner: -
--

ALTER TABLE raw.raw_pbp_20242025 ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME raw.raw_pbp_20242025_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: raw_pbp_all; Type: VIEW; Schema: raw; Owner: -
--

CREATE VIEW raw.raw_pbp_all AS
 SELECT raw_pbp_20182019.season,
    raw_pbp_20182019.game_id,
    raw_pbp_20182019.game_date,
    raw_pbp_20182019.session,
    raw_pbp_20182019.event_index,
    raw_pbp_20182019.game_period,
    raw_pbp_20182019.game_seconds,
    raw_pbp_20182019.clock_time,
    raw_pbp_20182019.event_type,
    raw_pbp_20182019.event_description,
    raw_pbp_20182019.event_detail,
    raw_pbp_20182019.event_zone,
    raw_pbp_20182019.event_team,
    raw_pbp_20182019.event_player_1,
    raw_pbp_20182019.event_player_2,
    raw_pbp_20182019.event_player_3,
    raw_pbp_20182019.event_length,
    raw_pbp_20182019.coords_x,
    raw_pbp_20182019.coords_y,
    raw_pbp_20182019.num_on,
    raw_pbp_20182019.num_off,
    raw_pbp_20182019.players_on,
    raw_pbp_20182019.players_off,
    raw_pbp_20182019.home_on_1,
    raw_pbp_20182019.home_on_2,
    raw_pbp_20182019.home_on_3,
    raw_pbp_20182019.home_on_4,
    raw_pbp_20182019.home_on_5,
    raw_pbp_20182019.home_on_6,
    raw_pbp_20182019.home_on_7,
    raw_pbp_20182019.away_on_1,
    raw_pbp_20182019.away_on_2,
    raw_pbp_20182019.away_on_3,
    raw_pbp_20182019.away_on_4,
    raw_pbp_20182019.away_on_5,
    raw_pbp_20182019.away_on_6,
    raw_pbp_20182019.away_on_7,
    raw_pbp_20182019.home_goalie,
    raw_pbp_20182019.away_goalie,
    raw_pbp_20182019.home_team,
    raw_pbp_20182019.away_team,
    raw_pbp_20182019.home_skaters,
    raw_pbp_20182019.away_skaters,
    raw_pbp_20182019.home_score,
    raw_pbp_20182019.away_score,
    raw_pbp_20182019.game_score_state,
    raw_pbp_20182019.game_strength_state,
    raw_pbp_20182019.home_zone,
    raw_pbp_20182019.pbp_distance,
    raw_pbp_20182019.event_distance,
    raw_pbp_20182019.event_angle,
    raw_pbp_20182019.home_zonestart,
    raw_pbp_20182019.face_index,
    raw_pbp_20182019.pen_index,
    raw_pbp_20182019.shift_index,
    raw_pbp_20182019.pred_goal,
    raw_pbp_20182019.id
   FROM raw.raw_pbp_20182019
UNION ALL
 SELECT raw_pbp_20192020.season,
    raw_pbp_20192020.game_id,
    raw_pbp_20192020.game_date,
    raw_pbp_20192020.session,
    raw_pbp_20192020.event_index,
    raw_pbp_20192020.game_period,
    raw_pbp_20192020.game_seconds,
    raw_pbp_20192020.clock_time,
    raw_pbp_20192020.event_type,
    raw_pbp_20192020.event_description,
    raw_pbp_20192020.event_detail,
    raw_pbp_20192020.event_zone,
    raw_pbp_20192020.event_team,
    raw_pbp_20192020.event_player_1,
    raw_pbp_20192020.event_player_2,
    raw_pbp_20192020.event_player_3,
    raw_pbp_20192020.event_length,
    raw_pbp_20192020.coords_x,
    raw_pbp_20192020.coords_y,
    raw_pbp_20192020.num_on,
    raw_pbp_20192020.num_off,
    raw_pbp_20192020.players_on,
    raw_pbp_20192020.players_off,
    raw_pbp_20192020.home_on_1,
    raw_pbp_20192020.home_on_2,
    raw_pbp_20192020.home_on_3,
    raw_pbp_20192020.home_on_4,
    raw_pbp_20192020.home_on_5,
    raw_pbp_20192020.home_on_6,
    raw_pbp_20192020.home_on_7,
    raw_pbp_20192020.away_on_1,
    raw_pbp_20192020.away_on_2,
    raw_pbp_20192020.away_on_3,
    raw_pbp_20192020.away_on_4,
    raw_pbp_20192020.away_on_5,
    raw_pbp_20192020.away_on_6,
    raw_pbp_20192020.away_on_7,
    raw_pbp_20192020.home_goalie,
    raw_pbp_20192020.away_goalie,
    raw_pbp_20192020.home_team,
    raw_pbp_20192020.away_team,
    raw_pbp_20192020.home_skaters,
    raw_pbp_20192020.away_skaters,
    raw_pbp_20192020.home_score,
    raw_pbp_20192020.away_score,
    raw_pbp_20192020.game_score_state,
    raw_pbp_20192020.game_strength_state,
    raw_pbp_20192020.home_zone,
    raw_pbp_20192020.pbp_distance,
    raw_pbp_20192020.event_distance,
    raw_pbp_20192020.event_angle,
    raw_pbp_20192020.home_zonestart,
    raw_pbp_20192020.face_index,
    raw_pbp_20192020.pen_index,
    raw_pbp_20192020.shift_index,
    raw_pbp_20192020.pred_goal,
    raw_pbp_20192020.id
   FROM raw.raw_pbp_20192020
UNION ALL
 SELECT raw_pbp_20202021.season,
    raw_pbp_20202021.game_id,
    raw_pbp_20202021.game_date,
    raw_pbp_20202021.session,
    raw_pbp_20202021.event_index,
    raw_pbp_20202021.game_period,
    raw_pbp_20202021.game_seconds,
    raw_pbp_20202021.clock_time,
    raw_pbp_20202021.event_type,
    raw_pbp_20202021.event_description,
    raw_pbp_20202021.event_detail,
    raw_pbp_20202021.event_zone,
    raw_pbp_20202021.event_team,
    raw_pbp_20202021.event_player_1,
    raw_pbp_20202021.event_player_2,
    raw_pbp_20202021.event_player_3,
    raw_pbp_20202021.event_length,
    raw_pbp_20202021.coords_x,
    raw_pbp_20202021.coords_y,
    raw_pbp_20202021.num_on,
    raw_pbp_20202021.num_off,
    raw_pbp_20202021.players_on,
    raw_pbp_20202021.players_off,
    raw_pbp_20202021.home_on_1,
    raw_pbp_20202021.home_on_2,
    raw_pbp_20202021.home_on_3,
    raw_pbp_20202021.home_on_4,
    raw_pbp_20202021.home_on_5,
    raw_pbp_20202021.home_on_6,
    raw_pbp_20202021.home_on_7,
    raw_pbp_20202021.away_on_1,
    raw_pbp_20202021.away_on_2,
    raw_pbp_20202021.away_on_3,
    raw_pbp_20202021.away_on_4,
    raw_pbp_20202021.away_on_5,
    raw_pbp_20202021.away_on_6,
    raw_pbp_20202021.away_on_7,
    raw_pbp_20202021.home_goalie,
    raw_pbp_20202021.away_goalie,
    raw_pbp_20202021.home_team,
    raw_pbp_20202021.away_team,
    raw_pbp_20202021.home_skaters,
    raw_pbp_20202021.away_skaters,
    raw_pbp_20202021.home_score,
    raw_pbp_20202021.away_score,
    raw_pbp_20202021.game_score_state,
    raw_pbp_20202021.game_strength_state,
    raw_pbp_20202021.home_zone,
    raw_pbp_20202021.pbp_distance,
    raw_pbp_20202021.event_distance,
    raw_pbp_20202021.event_angle,
    raw_pbp_20202021.home_zonestart,
    raw_pbp_20202021.face_index,
    raw_pbp_20202021.pen_index,
    raw_pbp_20202021.shift_index,
    raw_pbp_20202021.pred_goal,
    raw_pbp_20202021.id
   FROM raw.raw_pbp_20202021
UNION ALL
 SELECT raw_pbp_20212022.season,
    raw_pbp_20212022.game_id,
    raw_pbp_20212022.game_date,
    raw_pbp_20212022.session,
    raw_pbp_20212022.event_index,
    raw_pbp_20212022.game_period,
    raw_pbp_20212022.game_seconds,
    raw_pbp_20212022.clock_time,
    raw_pbp_20212022.event_type,
    raw_pbp_20212022.event_description,
    raw_pbp_20212022.event_detail,
    raw_pbp_20212022.event_zone,
    raw_pbp_20212022.event_team,
    raw_pbp_20212022.event_player_1,
    raw_pbp_20212022.event_player_2,
    raw_pbp_20212022.event_player_3,
    raw_pbp_20212022.event_length,
    raw_pbp_20212022.coords_x,
    raw_pbp_20212022.coords_y,
    raw_pbp_20212022.num_on,
    raw_pbp_20212022.num_off,
    raw_pbp_20212022.players_on,
    raw_pbp_20212022.players_off,
    raw_pbp_20212022.home_on_1,
    raw_pbp_20212022.home_on_2,
    raw_pbp_20212022.home_on_3,
    raw_pbp_20212022.home_on_4,
    raw_pbp_20212022.home_on_5,
    raw_pbp_20212022.home_on_6,
    raw_pbp_20212022.home_on_7,
    raw_pbp_20212022.away_on_1,
    raw_pbp_20212022.away_on_2,
    raw_pbp_20212022.away_on_3,
    raw_pbp_20212022.away_on_4,
    raw_pbp_20212022.away_on_5,
    raw_pbp_20212022.away_on_6,
    raw_pbp_20212022.away_on_7,
    raw_pbp_20212022.home_goalie,
    raw_pbp_20212022.away_goalie,
    raw_pbp_20212022.home_team,
    raw_pbp_20212022.away_team,
    raw_pbp_20212022.home_skaters,
    raw_pbp_20212022.away_skaters,
    raw_pbp_20212022.home_score,
    raw_pbp_20212022.away_score,
    raw_pbp_20212022.game_score_state,
    raw_pbp_20212022.game_strength_state,
    raw_pbp_20212022.home_zone,
    raw_pbp_20212022.pbp_distance,
    raw_pbp_20212022.event_distance,
    raw_pbp_20212022.event_angle,
    raw_pbp_20212022.home_zonestart,
    raw_pbp_20212022.face_index,
    raw_pbp_20212022.pen_index,
    raw_pbp_20212022.shift_index,
    raw_pbp_20212022.pred_goal,
    raw_pbp_20212022.id
   FROM raw.raw_pbp_20212022
UNION ALL
 SELECT raw_pbp_20222023.season,
    raw_pbp_20222023.game_id,
    raw_pbp_20222023.game_date,
    raw_pbp_20222023.session,
    raw_pbp_20222023.event_index,
    raw_pbp_20222023.game_period,
    raw_pbp_20222023.game_seconds,
    raw_pbp_20222023.clock_time,
    raw_pbp_20222023.event_type,
    raw_pbp_20222023.event_description,
    raw_pbp_20222023.event_detail,
    raw_pbp_20222023.event_zone,
    raw_pbp_20222023.event_team,
    raw_pbp_20222023.event_player_1,
    raw_pbp_20222023.event_player_2,
    raw_pbp_20222023.event_player_3,
    raw_pbp_20222023.event_length,
    raw_pbp_20222023.coords_x,
    raw_pbp_20222023.coords_y,
    raw_pbp_20222023.num_on,
    raw_pbp_20222023.num_off,
    raw_pbp_20222023.players_on,
    raw_pbp_20222023.players_off,
    raw_pbp_20222023.home_on_1,
    raw_pbp_20222023.home_on_2,
    raw_pbp_20222023.home_on_3,
    raw_pbp_20222023.home_on_4,
    raw_pbp_20222023.home_on_5,
    raw_pbp_20222023.home_on_6,
    raw_pbp_20222023.home_on_7,
    raw_pbp_20222023.away_on_1,
    raw_pbp_20222023.away_on_2,
    raw_pbp_20222023.away_on_3,
    raw_pbp_20222023.away_on_4,
    raw_pbp_20222023.away_on_5,
    raw_pbp_20222023.away_on_6,
    raw_pbp_20222023.away_on_7,
    raw_pbp_20222023.home_goalie,
    raw_pbp_20222023.away_goalie,
    raw_pbp_20222023.home_team,
    raw_pbp_20222023.away_team,
    raw_pbp_20222023.home_skaters,
    raw_pbp_20222023.away_skaters,
    raw_pbp_20222023.home_score,
    raw_pbp_20222023.away_score,
    raw_pbp_20222023.game_score_state,
    raw_pbp_20222023.game_strength_state,
    raw_pbp_20222023.home_zone,
    raw_pbp_20222023.pbp_distance,
    raw_pbp_20222023.event_distance,
    raw_pbp_20222023.event_angle,
    raw_pbp_20222023.home_zonestart,
    raw_pbp_20222023.face_index,
    raw_pbp_20222023.pen_index,
    raw_pbp_20222023.shift_index,
    raw_pbp_20222023.pred_goal,
    raw_pbp_20222023.id
   FROM raw.raw_pbp_20222023
UNION ALL
 SELECT raw_pbp_20232024.season,
    raw_pbp_20232024.game_id,
    raw_pbp_20232024.game_date,
    raw_pbp_20232024.session,
    raw_pbp_20232024.event_index,
    raw_pbp_20232024.game_period,
    raw_pbp_20232024.game_seconds,
    raw_pbp_20232024.clock_time,
    raw_pbp_20232024.event_type,
    raw_pbp_20232024.event_description,
    raw_pbp_20232024.event_detail,
    raw_pbp_20232024.event_zone,
    raw_pbp_20232024.event_team,
    raw_pbp_20232024.event_player_1,
    raw_pbp_20232024.event_player_2,
    raw_pbp_20232024.event_player_3,
    raw_pbp_20232024.event_length,
    raw_pbp_20232024.coords_x,
    raw_pbp_20232024.coords_y,
    raw_pbp_20232024.num_on,
    raw_pbp_20232024.num_off,
    raw_pbp_20232024.players_on,
    raw_pbp_20232024.players_off,
    raw_pbp_20232024.home_on_1,
    raw_pbp_20232024.home_on_2,
    raw_pbp_20232024.home_on_3,
    raw_pbp_20232024.home_on_4,
    raw_pbp_20232024.home_on_5,
    raw_pbp_20232024.home_on_6,
    raw_pbp_20232024.home_on_7,
    raw_pbp_20232024.away_on_1,
    raw_pbp_20232024.away_on_2,
    raw_pbp_20232024.away_on_3,
    raw_pbp_20232024.away_on_4,
    raw_pbp_20232024.away_on_5,
    raw_pbp_20232024.away_on_6,
    raw_pbp_20232024.away_on_7,
    raw_pbp_20232024.home_goalie,
    raw_pbp_20232024.away_goalie,
    raw_pbp_20232024.home_team,
    raw_pbp_20232024.away_team,
    raw_pbp_20232024.home_skaters,
    raw_pbp_20232024.away_skaters,
    raw_pbp_20232024.home_score,
    raw_pbp_20232024.away_score,
    raw_pbp_20232024.game_score_state,
    raw_pbp_20232024.game_strength_state,
    raw_pbp_20232024.home_zone,
    raw_pbp_20232024.pbp_distance,
    raw_pbp_20232024.event_distance,
    raw_pbp_20232024.event_angle,
    raw_pbp_20232024.home_zonestart,
    raw_pbp_20232024.face_index,
    raw_pbp_20232024.pen_index,
    raw_pbp_20232024.shift_index,
    raw_pbp_20232024.pred_goal,
    raw_pbp_20232024.id
   FROM raw.raw_pbp_20232024
UNION ALL
 SELECT raw_pbp_20242025.season,
    raw_pbp_20242025.game_id,
    raw_pbp_20242025.game_date,
    raw_pbp_20242025.session,
    raw_pbp_20242025.event_index,
    raw_pbp_20242025.game_period,
    raw_pbp_20242025.game_seconds,
    raw_pbp_20242025.clock_time,
    raw_pbp_20242025.event_type,
    raw_pbp_20242025.event_description,
    raw_pbp_20242025.event_detail,
    raw_pbp_20242025.event_zone,
    raw_pbp_20242025.event_team,
    raw_pbp_20242025.event_player_1,
    raw_pbp_20242025.event_player_2,
    raw_pbp_20242025.event_player_3,
    raw_pbp_20242025.event_length,
    raw_pbp_20242025.coords_x,
    raw_pbp_20242025.coords_y,
    raw_pbp_20242025.num_on,
    raw_pbp_20242025.num_off,
    raw_pbp_20242025.players_on,
    raw_pbp_20242025.players_off,
    raw_pbp_20242025.home_on_1,
    raw_pbp_20242025.home_on_2,
    raw_pbp_20242025.home_on_3,
    raw_pbp_20242025.home_on_4,
    raw_pbp_20242025.home_on_5,
    raw_pbp_20242025.home_on_6,
    raw_pbp_20242025.home_on_7,
    raw_pbp_20242025.away_on_1,
    raw_pbp_20242025.away_on_2,
    raw_pbp_20242025.away_on_3,
    raw_pbp_20242025.away_on_4,
    raw_pbp_20242025.away_on_5,
    raw_pbp_20242025.away_on_6,
    raw_pbp_20242025.away_on_7,
    raw_pbp_20242025.home_goalie,
    raw_pbp_20242025.away_goalie,
    raw_pbp_20242025.home_team,
    raw_pbp_20242025.away_team,
    raw_pbp_20242025.home_skaters,
    raw_pbp_20242025.away_skaters,
    raw_pbp_20242025.home_score,
    raw_pbp_20242025.away_score,
    raw_pbp_20242025.game_score_state,
    raw_pbp_20242025.game_strength_state,
    raw_pbp_20242025.home_zone,
    raw_pbp_20242025.pbp_distance,
    raw_pbp_20242025.event_distance,
    raw_pbp_20242025.event_angle,
    raw_pbp_20242025.home_zonestart,
    raw_pbp_20242025.face_index,
    raw_pbp_20242025.pen_index,
    raw_pbp_20242025.shift_index,
    raw_pbp_20242025.pred_goal,
    raw_pbp_20242025.id
   FROM raw.raw_pbp_20242025;


--
-- Name: raw_shifts_20182019_id_seq; Type: SEQUENCE; Schema: raw; Owner: -
--

ALTER TABLE raw.raw_shifts_20182019 ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME raw.raw_shifts_20182019_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: raw_shifts_20192020_id_seq; Type: SEQUENCE; Schema: raw; Owner: -
--

ALTER TABLE raw.raw_shifts_20192020 ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME raw.raw_shifts_20192020_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: raw_shifts_20202021_id_seq; Type: SEQUENCE; Schema: raw; Owner: -
--

ALTER TABLE raw.raw_shifts_20202021 ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME raw.raw_shifts_20202021_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: raw_shifts_20212022_id_seq; Type: SEQUENCE; Schema: raw; Owner: -
--

ALTER TABLE raw.raw_shifts_20212022 ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME raw.raw_shifts_20212022_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: raw_shifts_20222023_id_seq; Type: SEQUENCE; Schema: raw; Owner: -
--

ALTER TABLE raw.raw_shifts_20222023 ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME raw.raw_shifts_20222023_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: raw_shifts_20232024_id_seq; Type: SEQUENCE; Schema: raw; Owner: -
--

ALTER TABLE raw.raw_shifts_20232024 ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME raw.raw_shifts_20232024_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: raw_shifts_20242025_id_seq; Type: SEQUENCE; Schema: raw; Owner: -
--

ALTER TABLE raw.raw_shifts_20242025 ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME raw.raw_shifts_20242025_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: raw_shifts_resolved; Type: VIEW; Schema: raw; Owner: -
--

CREATE VIEW raw.raw_shifts_resolved AS
 SELECT rs.row_num,
    rs.player,
    rs.team_num,
    rs."position",
    rs.game_id,
    rs.game_date,
    rs.season,
    rs.session,
    rs.team,
    rs.opponent,
    rs.is_home,
    rs.game_period,
    rs.shift_num,
    rs.seconds_start,
    rs.seconds_end,
    rs.seconds_duration,
    rs.shift_start,
    rs.shift_end,
    rs.duration,
    rs.shift_mod,
    rs.id,
    COALESCE(o.player_id, dpu.player_id) AS player_id_resolved
   FROM ((raw.raw_shifts_all rs
     LEFT JOIN dim.player_name_override o ON (((o.raw_player = rs.player) AND (o.team = (rs.team)::text) AND (o.season = rs.season))))
     LEFT JOIN dim.dim_player_name_unique dpu ON ((dpu.name_key = dim.norm_name(rs.player))));


--
-- Name: game_shifts_new; Type: TABLE; Schema: scratch; Owner: -
--

CREATE TABLE scratch.game_shifts_new (
    game_id bigint NOT NULL,
    player_id bigint NOT NULL,
    period smallint NOT NULL,
    shift_start integer NOT NULL,
    shift_end integer NOT NULL,
    raw_shift_id bigint
);


--
-- Name: missing_player_names; Type: TABLE; Schema: scratch; Owner: -
--

CREATE TABLE scratch.missing_player_names (
    name_key text,
    raw_player text,
    n_rows bigint
);


--
-- Name: missing_player_work; Type: TABLE; Schema: scratch; Owner: -
--

CREATE TABLE scratch.missing_player_work (
    name_key text,
    raw_player text,
    team character varying(3),
    season integer,
    sample_game_id bigint,
    n_rows bigint
);


--
-- Name: penalty_exclude_times_20152016; Type: TABLE; Schema: scratch; Owner: -
--

CREATE TABLE scratch.penalty_exclude_times_20152016 (
    "time" integer,
    team_1 integer,
    team_2 integer,
    game_id bigint,
    exclude character varying(5)
);


--
-- Name: penalty_exclude_times_20162017; Type: TABLE; Schema: scratch; Owner: -
--

CREATE TABLE scratch.penalty_exclude_times_20162017 (
    "time" integer,
    team_1 integer,
    team_2 integer,
    game_id bigint,
    exclude character varying(5)
);


--
-- Name: penalty_exclude_times_20172018; Type: TABLE; Schema: scratch; Owner: -
--

CREATE TABLE scratch.penalty_exclude_times_20172018 (
    "time" integer,
    team_1 integer,
    team_2 integer,
    game_id bigint,
    exclude character varying(5)
);


--
-- Name: player_info_stage; Type: TABLE; Schema: scratch; Owner: -
--

CREATE TABLE scratch.player_info_stage (
    player_id bigint,
    player text,
    "position" text,
    shootscatches text,
    birthday date,
    nationality text,
    height integer,
    weight integer,
    draftposition integer,
    draftyear integer
);


--
-- Name: preprocessed_shifts; Type: TABLE; Schema: scratch; Owner: -
--

CREATE TABLE scratch.preprocessed_shifts (
    game_id bigint,
    player_id bigint,
    team_id integer,
    shift_start integer,
    shift_end integer,
    game_time integer,
    team_id_for integer,
    team_id_against integer,
    event character varying(50)
);


--
-- Name: player_info player_id; Type: DEFAULT; Schema: dim; Owner: -
--

ALTER TABLE ONLY dim.player_info ALTER COLUMN player_id SET DEFAULT nextval('dim.player_info_table_test_player_id_seq'::regclass);


--
-- Name: dim_player_name dim_player_name_pkey; Type: CONSTRAINT; Schema: dim; Owner: -
--

ALTER TABLE ONLY dim.dim_player_name
    ADD CONSTRAINT dim_player_name_pkey PRIMARY KEY (player_id);


--
-- Name: player_cap_hit_spotrac player_cap_hit_spotrac_pkey; Type: CONSTRAINT; Schema: dim; Owner: -
--

ALTER TABLE ONLY dim.player_cap_hit_spotrac
    ADD CONSTRAINT player_cap_hit_spotrac_pkey PRIMARY KEY (season_year, spotrac_url);


--
-- Name: player_info player_info_pk; Type: CONSTRAINT; Schema: dim; Owner: -
--

ALTER TABLE ONLY dim.player_info
    ADD CONSTRAINT player_info_pk PRIMARY KEY (player_id);


--
-- Name: player_name_override player_name_override_pkey; Type: CONSTRAINT; Schema: dim; Owner: -
--

ALTER TABLE ONLY dim.player_name_override
    ADD CONSTRAINT player_name_override_pkey PRIMARY KEY (raw_player, team, season);


--
-- Name: team_summary_20152016 team_summary_20152016_pkey; Type: CONSTRAINT; Schema: mart; Owner: -
--

ALTER TABLE ONLY mart.team_summary_20152016
    ADD CONSTRAINT team_summary_20152016_pkey PRIMARY KEY (season, team_id);


--
-- Name: team_summary_20162017 team_summary_20162017_pkey; Type: CONSTRAINT; Schema: mart; Owner: -
--

ALTER TABLE ONLY mart.team_summary_20162017
    ADD CONSTRAINT team_summary_20162017_pkey PRIMARY KEY (season, team_id);


--
-- Name: team_summary_20172018 team_summary_20172018_pkey; Type: CONSTRAINT; Schema: mart; Owner: -
--

ALTER TABLE ONLY mart.team_summary_20172018
    ADD CONSTRAINT team_summary_20172018_pkey PRIMARY KEY (season, team_id);


--
-- Name: game_plays game_plays_processor_test_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.game_plays
    ADD CONSTRAINT game_plays_processor_test_pkey PRIMARY KEY (play_id);


--
-- Name: game_shifts_final game_shifts_final_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.game_shifts_final
    ADD CONSTRAINT game_shifts_final_pkey PRIMARY KEY (game_id, player_id, period, shift_start, shift_end);


--
-- Name: raw_pbp_20182019 raw_pbp_20182019_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.raw_pbp_20182019
    ADD CONSTRAINT raw_pbp_20182019_pkey PRIMARY KEY (id);


--
-- Name: raw_pbp_20192020 raw_pbp_20192020_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.raw_pbp_20192020
    ADD CONSTRAINT raw_pbp_20192020_pkey PRIMARY KEY (id);


--
-- Name: raw_pbp_20202021 raw_pbp_20202021_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.raw_pbp_20202021
    ADD CONSTRAINT raw_pbp_20202021_pkey PRIMARY KEY (id);


--
-- Name: raw_pbp_20212022 raw_pbp_20212022_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.raw_pbp_20212022
    ADD CONSTRAINT raw_pbp_20212022_pkey PRIMARY KEY (id);


--
-- Name: raw_pbp_20222023 raw_pbp_20222023_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.raw_pbp_20222023
    ADD CONSTRAINT raw_pbp_20222023_pkey PRIMARY KEY (id);


--
-- Name: raw_pbp_20232024 raw_pbp_20232024_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.raw_pbp_20232024
    ADD CONSTRAINT raw_pbp_20232024_pkey PRIMARY KEY (id);


--
-- Name: raw_pbp_20242025 raw_pbp_20242025_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.raw_pbp_20242025
    ADD CONSTRAINT raw_pbp_20242025_pkey PRIMARY KEY (id);


--
-- Name: raw_shifts_20182019 raw_shifts_20182019_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.raw_shifts_20182019
    ADD CONSTRAINT raw_shifts_20182019_pkey PRIMARY KEY (id);


--
-- Name: raw_shifts_20192020 raw_shifts_20192020_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.raw_shifts_20192020
    ADD CONSTRAINT raw_shifts_20192020_pkey PRIMARY KEY (id);


--
-- Name: raw_shifts_20202021 raw_shifts_20202021_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.raw_shifts_20202021
    ADD CONSTRAINT raw_shifts_20202021_pkey PRIMARY KEY (id);


--
-- Name: raw_shifts_20212022 raw_shifts_20212022_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.raw_shifts_20212022
    ADD CONSTRAINT raw_shifts_20212022_pkey PRIMARY KEY (id);


--
-- Name: raw_shifts_20222023 raw_shifts_20222023_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.raw_shifts_20222023
    ADD CONSTRAINT raw_shifts_20222023_pkey PRIMARY KEY (id);


--
-- Name: raw_shifts_20232024 raw_shifts_20232024_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.raw_shifts_20232024
    ADD CONSTRAINT raw_shifts_20232024_pkey PRIMARY KEY (id);


--
-- Name: raw_shifts_20242025 raw_shifts_20242025_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.raw_shifts_20242025
    ADD CONSTRAINT raw_shifts_20242025_pkey PRIMARY KEY (id);


--
-- Name: game_shifts_new game_shifts_new_pkey; Type: CONSTRAINT; Schema: scratch; Owner: -
--

ALTER TABLE ONLY scratch.game_shifts_new
    ADD CONSTRAINT game_shifts_new_pkey PRIMARY KEY (game_id, player_id, period, shift_start, shift_end);


--
-- Name: ix_game_team_map_20182019; Type: INDEX; Schema: derived; Owner: -
--

CREATE INDEX ix_game_team_map_20182019 ON derived.game_team_map_20182019 USING btree (game_id, team_code);


--
-- Name: ix_game_team_map_20192020; Type: INDEX; Schema: derived; Owner: -
--

CREATE INDEX ix_game_team_map_20192020 ON derived.game_team_map_20182019 USING btree (game_id, team_code);


--
-- Name: ix_game_team_map_20202021; Type: INDEX; Schema: derived; Owner: -
--

CREATE INDEX ix_game_team_map_20202021 ON derived.game_team_map_20182019 USING btree (game_id, team_code);


--
-- Name: ix_game_team_map_20212022; Type: INDEX; Schema: derived; Owner: -
--

CREATE INDEX ix_game_team_map_20212022 ON derived.game_team_map_20212022 USING btree (game_id, team_code);


--
-- Name: ix_game_team_map_20222023; Type: INDEX; Schema: derived; Owner: -
--

CREATE INDEX ix_game_team_map_20222023 ON derived.game_team_map_20222023 USING btree (game_id, team_code);


--
-- Name: ix_game_team_map_20232024; Type: INDEX; Schema: derived; Owner: -
--

CREATE INDEX ix_game_team_map_20232024 ON derived.game_team_map_20232024 USING btree (game_id, team_code);


--
-- Name: ix_game_team_map_20242025; Type: INDEX; Schema: derived; Owner: -
--

CREATE INDEX ix_game_team_map_20242025 ON derived.game_team_map_20242025 USING btree (game_id, team_code);


--
-- Name: ix_team_corsi_games_20182019; Type: INDEX; Schema: derived; Owner: -
--

CREATE INDEX ix_team_corsi_games_20182019 ON derived.team_corsi_games_20182019 USING btree (game_id, team_id);


--
-- Name: ix_team_corsi_games_20192020; Type: INDEX; Schema: derived; Owner: -
--

CREATE INDEX ix_team_corsi_games_20192020 ON derived.team_corsi_games_20192020 USING btree (game_id, team_id);


--
-- Name: ix_team_corsi_games_20202021; Type: INDEX; Schema: derived; Owner: -
--

CREATE INDEX ix_team_corsi_games_20202021 ON derived.team_corsi_games_20202021 USING btree (game_id, team_id);


--
-- Name: ix_team_corsi_games_20212022; Type: INDEX; Schema: derived; Owner: -
--

CREATE INDEX ix_team_corsi_games_20212022 ON derived.team_corsi_games_20212022 USING btree (game_id, team_id);


--
-- Name: ix_team_corsi_games_20222023; Type: INDEX; Schema: derived; Owner: -
--

CREATE INDEX ix_team_corsi_games_20222023 ON derived.team_corsi_games_20222023 USING btree (game_id, team_id);


--
-- Name: ix_team_corsi_games_20232024; Type: INDEX; Schema: derived; Owner: -
--

CREATE INDEX ix_team_corsi_games_20232024 ON derived.team_corsi_games_20232024 USING btree (game_id, team_id);


--
-- Name: ix_team_corsi_games_20242025; Type: INDEX; Schema: derived; Owner: -
--

CREATE INDEX ix_team_corsi_games_20242025 ON derived.team_corsi_games_20242025 USING btree (game_id, team_id);


--
-- Name: ix_team_event_totals_games_20182019; Type: INDEX; Schema: derived; Owner: -
--

CREATE INDEX ix_team_event_totals_games_20182019 ON derived.team_event_totals_games_20182019 USING btree (game_id, team_id);


--
-- Name: ix_team_event_totals_games_20192020; Type: INDEX; Schema: derived; Owner: -
--

CREATE INDEX ix_team_event_totals_games_20192020 ON derived.team_event_totals_games_20192020 USING btree (game_id, team_id);


--
-- Name: ix_team_event_totals_games_20202021; Type: INDEX; Schema: derived; Owner: -
--

CREATE INDEX ix_team_event_totals_games_20202021 ON derived.team_event_totals_games_20202021 USING btree (game_id, team_id);


--
-- Name: ix_team_event_totals_games_20212022; Type: INDEX; Schema: derived; Owner: -
--

CREATE INDEX ix_team_event_totals_games_20212022 ON derived.team_event_totals_games_20212022 USING btree (game_id, team_id);


--
-- Name: ix_team_event_totals_games_20222023; Type: INDEX; Schema: derived; Owner: -
--

CREATE INDEX ix_team_event_totals_games_20222023 ON derived.team_event_totals_games_20222023 USING btree (game_id, team_id);


--
-- Name: ix_team_event_totals_games_20232024; Type: INDEX; Schema: derived; Owner: -
--

CREATE INDEX ix_team_event_totals_games_20232024 ON derived.team_event_totals_games_20232024 USING btree (game_id, team_id);


--
-- Name: ix_team_event_totals_games_20242025; Type: INDEX; Schema: derived; Owner: -
--

CREATE INDEX ix_team_event_totals_games_20242025 ON derived.team_event_totals_games_20242025 USING btree (game_id, team_id);


--
-- Name: ix_dim_player_name_key; Type: INDEX; Schema: dim; Owner: -
--

CREATE INDEX ix_dim_player_name_key ON dim.dim_player_name USING btree (name_key);


--
-- Name: ix_dim_player_name_unique_key; Type: INDEX; Schema: dim; Owner: -
--

CREATE INDEX ix_dim_player_name_unique_key ON dim.dim_player_name_unique_table USING btree (name_key);


--
-- Name: ux_dim_team_code; Type: INDEX; Schema: dim; Owner: -
--

CREATE UNIQUE INDEX ux_dim_team_code ON dim.dim_team_code USING btree (team_code);


--
-- Name: idx_pges_20152016_game; Type: INDEX; Schema: mart; Owner: -
--

CREATE INDEX idx_pges_20152016_game ON mart.player_game_es_20152016 USING btree (game_id);


--
-- Name: idx_pges_20152016_player; Type: INDEX; Schema: mart; Owner: -
--

CREATE INDEX idx_pges_20152016_player ON mart.player_game_es_20152016 USING btree (player_id);


--
-- Name: idx_pges_20152016_team; Type: INDEX; Schema: mart; Owner: -
--

CREATE INDEX idx_pges_20152016_team ON mart.player_game_es_20152016 USING btree (team_id);


--
-- Name: idx_pges_20162017_game; Type: INDEX; Schema: mart; Owner: -
--

CREATE INDEX idx_pges_20162017_game ON mart.player_game_es_20162017 USING btree (game_id);


--
-- Name: idx_pges_20162017_player; Type: INDEX; Schema: mart; Owner: -
--

CREATE INDEX idx_pges_20162017_player ON mart.player_game_es_20162017 USING btree (player_id);


--
-- Name: idx_pges_20162017_team; Type: INDEX; Schema: mart; Owner: -
--

CREATE INDEX idx_pges_20162017_team ON mart.player_game_es_20162017 USING btree (team_id);


--
-- Name: idx_pges_20172018_game; Type: INDEX; Schema: mart; Owner: -
--

CREATE INDEX idx_pges_20172018_game ON mart.player_game_es_20172018 USING btree (game_id);


--
-- Name: idx_pges_20172018_player; Type: INDEX; Schema: mart; Owner: -
--

CREATE INDEX idx_pges_20172018_player ON mart.player_game_es_20172018 USING btree (player_id);


--
-- Name: idx_pges_20172018_team; Type: INDEX; Schema: mart; Owner: -
--

CREATE INDEX idx_pges_20172018_team ON mart.player_game_es_20172018 USING btree (team_id);


--
-- Name: idx_pges_20182019_game; Type: INDEX; Schema: mart; Owner: -
--

CREATE INDEX idx_pges_20182019_game ON mart.player_game_es_20182019 USING btree (game_id);


--
-- Name: idx_pges_20182019_player; Type: INDEX; Schema: mart; Owner: -
--

CREATE INDEX idx_pges_20182019_player ON mart.player_game_es_20182019 USING btree (player_id);


--
-- Name: idx_pges_20182019_team; Type: INDEX; Schema: mart; Owner: -
--

CREATE INDEX idx_pges_20182019_team ON mart.player_game_es_20182019 USING btree (team_id);


--
-- Name: idx_pges_20192020_game; Type: INDEX; Schema: mart; Owner: -
--

CREATE INDEX idx_pges_20192020_game ON mart.player_game_es_20192020 USING btree (game_id);


--
-- Name: idx_pges_20192020_player; Type: INDEX; Schema: mart; Owner: -
--

CREATE INDEX idx_pges_20192020_player ON mart.player_game_es_20192020 USING btree (player_id);


--
-- Name: idx_pges_20192020_team; Type: INDEX; Schema: mart; Owner: -
--

CREATE INDEX idx_pges_20192020_team ON mart.player_game_es_20192020 USING btree (team_id);


--
-- Name: idx_pges_20202021_game; Type: INDEX; Schema: mart; Owner: -
--

CREATE INDEX idx_pges_20202021_game ON mart.player_game_es_20202021 USING btree (game_id);


--
-- Name: idx_pges_20202021_player; Type: INDEX; Schema: mart; Owner: -
--

CREATE INDEX idx_pges_20202021_player ON mart.player_game_es_20202021 USING btree (player_id);


--
-- Name: idx_pges_20202021_team; Type: INDEX; Schema: mart; Owner: -
--

CREATE INDEX idx_pges_20202021_team ON mart.player_game_es_20202021 USING btree (team_id);


--
-- Name: idx_pges_20212022_game; Type: INDEX; Schema: mart; Owner: -
--

CREATE INDEX idx_pges_20212022_game ON mart.player_game_es_20212022 USING btree (game_id);


--
-- Name: idx_pges_20212022_player; Type: INDEX; Schema: mart; Owner: -
--

CREATE INDEX idx_pges_20212022_player ON mart.player_game_es_20212022 USING btree (player_id);


--
-- Name: idx_pges_20212022_team; Type: INDEX; Schema: mart; Owner: -
--

CREATE INDEX idx_pges_20212022_team ON mart.player_game_es_20212022 USING btree (team_id);


--
-- Name: idx_pges_20222023_game; Type: INDEX; Schema: mart; Owner: -
--

CREATE INDEX idx_pges_20222023_game ON mart.player_game_es_20222023 USING btree (game_id);


--
-- Name: idx_pges_20222023_player; Type: INDEX; Schema: mart; Owner: -
--

CREATE INDEX idx_pges_20222023_player ON mart.player_game_es_20222023 USING btree (player_id);


--
-- Name: idx_pges_20222023_team; Type: INDEX; Schema: mart; Owner: -
--

CREATE INDEX idx_pges_20222023_team ON mart.player_game_es_20222023 USING btree (team_id);


--
-- Name: idx_pges_20232024_game; Type: INDEX; Schema: mart; Owner: -
--

CREATE INDEX idx_pges_20232024_game ON mart.player_game_es_20232024 USING btree (game_id);


--
-- Name: idx_pges_20232024_player; Type: INDEX; Schema: mart; Owner: -
--

CREATE INDEX idx_pges_20232024_player ON mart.player_game_es_20232024 USING btree (player_id);


--
-- Name: idx_pges_20232024_team; Type: INDEX; Schema: mart; Owner: -
--

CREATE INDEX idx_pges_20232024_team ON mart.player_game_es_20232024 USING btree (team_id);


--
-- Name: idx_pges_20242025_game; Type: INDEX; Schema: mart; Owner: -
--

CREATE INDEX idx_pges_20242025_game ON mart.player_game_es_20242025 USING btree (game_id);


--
-- Name: idx_pges_20242025_player; Type: INDEX; Schema: mart; Owner: -
--

CREATE INDEX idx_pges_20242025_player ON mart.player_game_es_20242025 USING btree (player_id);


--
-- Name: idx_pges_20242025_team; Type: INDEX; Schema: mart; Owner: -
--

CREATE INDEX idx_pges_20242025_team ON mart.player_game_es_20242025 USING btree (team_id);


--
-- Name: idx_team_summary_20152016_abbr; Type: INDEX; Schema: mart; Owner: -
--

CREATE INDEX idx_team_summary_20152016_abbr ON mart.team_summary_20152016 USING btree (abbr);


--
-- Name: idx_team_summary_20152016_pts; Type: INDEX; Schema: mart; Owner: -
--

CREATE INDEX idx_team_summary_20152016_pts ON mart.team_summary_20152016 USING btree (pts);


--
-- Name: idx_team_summary_20162017_abbr; Type: INDEX; Schema: mart; Owner: -
--

CREATE INDEX idx_team_summary_20162017_abbr ON mart.team_summary_20162017 USING btree (abbr);


--
-- Name: idx_team_summary_20162017_pts; Type: INDEX; Schema: mart; Owner: -
--

CREATE INDEX idx_team_summary_20162017_pts ON mart.team_summary_20162017 USING btree (pts);


--
-- Name: idx_team_summary_20172018_abbr; Type: INDEX; Schema: mart; Owner: -
--

CREATE INDEX idx_team_summary_20172018_abbr ON mart.team_summary_20172018 USING btree (abbr);


--
-- Name: idx_team_summary_20172018_pts; Type: INDEX; Schema: mart; Owner: -
--

CREATE INDEX idx_team_summary_20172018_pts ON mart.team_summary_20172018 USING btree (pts);


--
-- Name: ix_team_corsi_season_20182019; Type: INDEX; Schema: mart; Owner: -
--

CREATE INDEX ix_team_corsi_season_20182019 ON mart.team_corsi_season_20182019 USING btree (team_id);


--
-- Name: ix_team_corsi_season_20192020; Type: INDEX; Schema: mart; Owner: -
--

CREATE INDEX ix_team_corsi_season_20192020 ON mart.team_corsi_season_20192020 USING btree (team_id);


--
-- Name: ix_team_corsi_season_20202021; Type: INDEX; Schema: mart; Owner: -
--

CREATE INDEX ix_team_corsi_season_20202021 ON mart.team_corsi_season_20202021 USING btree (team_id);


--
-- Name: ix_team_corsi_season_20212022; Type: INDEX; Schema: mart; Owner: -
--

CREATE INDEX ix_team_corsi_season_20212022 ON mart.team_corsi_season_20212022 USING btree (team_id);


--
-- Name: ix_team_corsi_season_20222023; Type: INDEX; Schema: mart; Owner: -
--

CREATE INDEX ix_team_corsi_season_20222023 ON mart.team_corsi_season_20222023 USING btree (team_id);


--
-- Name: ix_team_corsi_season_20232024; Type: INDEX; Schema: mart; Owner: -
--

CREATE INDEX ix_team_corsi_season_20232024 ON mart.team_corsi_season_20232024 USING btree (team_id);


--
-- Name: ix_team_corsi_season_20242025; Type: INDEX; Schema: mart; Owner: -
--

CREATE INDEX ix_team_corsi_season_20242025 ON mart.team_corsi_season_20242025 USING btree (team_id);


--
-- Name: ix_team_event_totals_season_20182019; Type: INDEX; Schema: mart; Owner: -
--

CREATE INDEX ix_team_event_totals_season_20182019 ON mart.team_event_totals_season_20182019 USING btree (team_id);


--
-- Name: ix_team_event_totals_season_20192020; Type: INDEX; Schema: mart; Owner: -
--

CREATE INDEX ix_team_event_totals_season_20192020 ON mart.team_event_totals_season_20192020 USING btree (team_id);


--
-- Name: ix_team_event_totals_season_20202021; Type: INDEX; Schema: mart; Owner: -
--

CREATE INDEX ix_team_event_totals_season_20202021 ON mart.team_event_totals_season_20202021 USING btree (team_id);


--
-- Name: ix_team_event_totals_season_20212022; Type: INDEX; Schema: mart; Owner: -
--

CREATE INDEX ix_team_event_totals_season_20212022 ON mart.team_event_totals_season_20212022 USING btree (team_id);


--
-- Name: ix_team_event_totals_season_20222023; Type: INDEX; Schema: mart; Owner: -
--

CREATE INDEX ix_team_event_totals_season_20222023 ON mart.team_event_totals_season_20222023 USING btree (team_id);


--
-- Name: ix_team_event_totals_season_20232024; Type: INDEX; Schema: mart; Owner: -
--

CREATE INDEX ix_team_event_totals_season_20232024 ON mart.team_event_totals_season_20232024 USING btree (team_id);


--
-- Name: ix_team_event_totals_season_20242025; Type: INDEX; Schema: mart; Owner: -
--

CREATE INDEX ix_team_event_totals_season_20242025 ON mart.team_event_totals_season_20242025 USING btree (team_id);


--
-- Name: ix_dim.player_cap_hit_20152016_player_id; Type: INDEX; Schema: raw; Owner: -
--

CREATE INDEX "ix_dim.player_cap_hit_20152016_player_id" ON raw."dim.player_cap_hit_20152016" USING btree (player_id);


--
-- Name: ix_dim.player_cap_hit_20152016_spotrac_url; Type: INDEX; Schema: raw; Owner: -
--

CREATE INDEX "ix_dim.player_cap_hit_20152016_spotrac_url" ON raw."dim.player_cap_hit_20152016" USING btree (spotrac_url);


--
-- Name: ix_dim.player_cap_hit_20162017_player_id; Type: INDEX; Schema: raw; Owner: -
--

CREATE INDEX "ix_dim.player_cap_hit_20162017_player_id" ON raw."dim.player_cap_hit_20162017" USING btree (player_id);


--
-- Name: ix_dim.player_cap_hit_20162017_spotrac_url; Type: INDEX; Schema: raw; Owner: -
--

CREATE INDEX "ix_dim.player_cap_hit_20162017_spotrac_url" ON raw."dim.player_cap_hit_20162017" USING btree (spotrac_url);


--
-- Name: ix_dim.player_cap_hit_20172018_player_id; Type: INDEX; Schema: raw; Owner: -
--

CREATE INDEX "ix_dim.player_cap_hit_20172018_player_id" ON raw."dim.player_cap_hit_20172018" USING btree (player_id);


--
-- Name: ix_dim.player_cap_hit_20172018_spotrac_url; Type: INDEX; Schema: raw; Owner: -
--

CREATE INDEX "ix_dim.player_cap_hit_20172018_spotrac_url" ON raw."dim.player_cap_hit_20172018" USING btree (spotrac_url);


--
-- Name: ix_dim.player_cap_hit_20182019_player_id; Type: INDEX; Schema: raw; Owner: -
--

CREATE INDEX "ix_dim.player_cap_hit_20182019_player_id" ON raw."dim.player_cap_hit_20182019" USING btree (player_id);


--
-- Name: ix_dim.player_cap_hit_20182019_spotrac_url; Type: INDEX; Schema: raw; Owner: -
--

CREATE INDEX "ix_dim.player_cap_hit_20182019_spotrac_url" ON raw."dim.player_cap_hit_20182019" USING btree (spotrac_url);


--
-- Name: ix_dim.player_cap_hit_20192020_player_id; Type: INDEX; Schema: raw; Owner: -
--

CREATE INDEX "ix_dim.player_cap_hit_20192020_player_id" ON raw."dim.player_cap_hit_20192020" USING btree (player_id);


--
-- Name: ix_dim.player_cap_hit_20192020_spotrac_url; Type: INDEX; Schema: raw; Owner: -
--

CREATE INDEX "ix_dim.player_cap_hit_20192020_spotrac_url" ON raw."dim.player_cap_hit_20192020" USING btree (spotrac_url);


--
-- Name: ix_dim.player_cap_hit_20202021_player_id; Type: INDEX; Schema: raw; Owner: -
--

CREATE INDEX "ix_dim.player_cap_hit_20202021_player_id" ON raw."dim.player_cap_hit_20202021" USING btree (player_id);


--
-- Name: ix_dim.player_cap_hit_20202021_spotrac_url; Type: INDEX; Schema: raw; Owner: -
--

CREATE INDEX "ix_dim.player_cap_hit_20202021_spotrac_url" ON raw."dim.player_cap_hit_20202021" USING btree (spotrac_url);


--
-- Name: ix_dim.player_cap_hit_20212022_player_id; Type: INDEX; Schema: raw; Owner: -
--

CREATE INDEX "ix_dim.player_cap_hit_20212022_player_id" ON raw."dim.player_cap_hit_20212022" USING btree (player_id);


--
-- Name: ix_dim.player_cap_hit_20212022_spotrac_url; Type: INDEX; Schema: raw; Owner: -
--

CREATE INDEX "ix_dim.player_cap_hit_20212022_spotrac_url" ON raw."dim.player_cap_hit_20212022" USING btree (spotrac_url);


--
-- Name: ix_dim.player_cap_hit_20222023_player_id; Type: INDEX; Schema: raw; Owner: -
--

CREATE INDEX "ix_dim.player_cap_hit_20222023_player_id" ON raw."dim.player_cap_hit_20222023" USING btree (player_id);


--
-- Name: ix_dim.player_cap_hit_20222023_spotrac_url; Type: INDEX; Schema: raw; Owner: -
--

CREATE INDEX "ix_dim.player_cap_hit_20222023_spotrac_url" ON raw."dim.player_cap_hit_20222023" USING btree (spotrac_url);


--
-- Name: ix_dim.player_cap_hit_20232024_player_id; Type: INDEX; Schema: raw; Owner: -
--

CREATE INDEX "ix_dim.player_cap_hit_20232024_player_id" ON raw."dim.player_cap_hit_20232024" USING btree (player_id);


--
-- Name: ix_dim.player_cap_hit_20232024_spotrac_url; Type: INDEX; Schema: raw; Owner: -
--

CREATE INDEX "ix_dim.player_cap_hit_20232024_spotrac_url" ON raw."dim.player_cap_hit_20232024" USING btree (spotrac_url);


--
-- Name: ix_dim.player_cap_hit_20242025_player_id; Type: INDEX; Schema: raw; Owner: -
--

CREATE INDEX "ix_dim.player_cap_hit_20242025_player_id" ON raw."dim.player_cap_hit_20242025" USING btree (player_id);


--
-- Name: ix_dim.player_cap_hit_20242025_spotrac_url; Type: INDEX; Schema: raw; Owner: -
--

CREATE INDEX "ix_dim.player_cap_hit_20242025_spotrac_url" ON raw."dim.player_cap_hit_20242025" USING btree (spotrac_url);


--
-- Name: ix_game_shifts_final_game_time; Type: INDEX; Schema: raw; Owner: -
--

CREATE INDEX ix_game_shifts_final_game_time ON raw.game_shifts_final USING btree (game_id, shift_start, shift_end);


--
-- Name: ix_pbp_corsi_20182019_game_time; Type: INDEX; Schema: raw; Owner: -
--

CREATE INDEX ix_pbp_corsi_20182019_game_time ON raw.raw_pbp_20182019 USING btree (game_id, game_seconds);


--
-- Name: ix_raw_pbp_20182019_game_time; Type: INDEX; Schema: raw; Owner: -
--

CREATE INDEX ix_raw_pbp_20182019_game_time ON raw.raw_pbp_20182019 USING btree (game_id, game_seconds);


--
-- Name: ux_game_game_id; Type: INDEX; Schema: raw; Owner: -
--

CREATE UNIQUE INDEX ux_game_game_id ON raw.game USING btree (game_id) WHERE (game_id IS NOT NULL);


--
-- Name: ix_game_shifts_new_game_time; Type: INDEX; Schema: scratch; Owner: -
--

CREATE INDEX ix_game_shifts_new_game_time ON scratch.game_shifts_new USING btree (game_id, shift_start, shift_end);


--
-- PostgreSQL database dump complete
--

\unrestrict kSpt9F3sRkfLe11aQnt0AombGdk7RjasJF1YqxuVxYXmRnUDBCpmGC6IfP6bHm6

