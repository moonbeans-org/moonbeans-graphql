--
-- PostgreSQL database dump
--

-- Dumped from database version 14.1
-- Dumped by pg_dump version 14.1

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: meta; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.meta (
    name text NOT NULL,
    value text,
    "timestamp" numeric
);


ALTER TABLE public.meta OWNER TO postgres;

--
-- Data for Name: meta; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.meta (name, value, "timestamp") FROM stdin;
last_block_zoombies	1359434	1642999641
last_block_cryptodate	1359439	1642999699
last_block_moonbrats	1359439	1642999699
last_block_obsoletepals	1359439	1642999699
last_block_moonshroomiz	1359439	1642999699
last_block_midasdragon	1359439	1642999699
last_block_moonbotsguns	1359439	1642999699
last_block_freeriverdragons	1359439	1642999699
last_block_pupazzipunks	1359439	1642999699
last_block_moonbots	1359439	1642999699
last_block_upsettispaghetti	1359439	1642999699
last_block_moonowlets	1359439	1642999699
last_block_moonsquitos	1359439	1642999699
last_block_movrrole	1359439	1642999699
last_block_moonarines	1359439	1642999700
last_block_damnedpirates	1359439	1642999700
last_block_rivrmaids	1359439	1642999700
last_block_beanies	1359439	1642999700
last_block_moonmonsters	1359439	1642999700
last_block_symtraction	1359439	1642999700
last_block_cryptofemalebutchers	1359439	1642999700
last_block_moonriverpunks	1359439	1642999700
last_block_movrloot	1359439	1642999700
last_block_blvckmarketcyan	1359439	1642999702
last_block_moonsama	1359430	1642999599
last_block_cryptobutchers	1359439	1642999699
last_block_blvcksnakes	1359429	1642999586
last_block_neoncrisis	1359429	1642999587
last_block_scifipunks	1359439	1642999699
last_block_projectinfecticide	1359439	1642999699
last_block_moonchads	1359439	1642999699
last_block_moonkeys	1359439	1642999700
last_block_moonninjas	1359439	1642999700
last_block_rugcollection	1359439	1642999700
last_block_movrpunks	1359439	1642999699
\.


--
-- Name: meta meta_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.meta
    ADD CONSTRAINT meta_pkey PRIMARY KEY (name);


--
-- PostgreSQL database dump complete
--

