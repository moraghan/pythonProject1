
create table if not exists collection
(
collection_id integer primary key,
collection_descr varchar(100),
collection_poster_path  text null,
backdrop_poster_path  text null
);

create table if not exists country
(
country_code char(2) primary key,
country_descr varchar(100)
);

create table if not exists company
(
company_id integer primary key,
company_descr varchar(100),
logo_path  text null,
country_origin  char(2)
);

insert into collection
(
collection_id,
collection_descr,
collection_poster_path,
backdrop_poster_path
)

select distinct (response_json -> 'belongs_to_collection' ->> 'id')::int as collection_id,
                response_json -> 'belongs_to_collection' ->> 'name' as collection_descr,
                response_json -> 'belongs_to_collection' ->> 'poster_path' as collection_poster_path,
                response_json -> 'belongs_to_collection' ->> 'backdrop_path' as collection_backdrop_path
from  tmdb_requests
where request_type = 'movie' and
      response_json -> 'belongs_to_collection' ->> 'id' is not null and
      not exists (select 1 from collection where collection_id = (response_json -> 'belongs_to_collection' ->> 'id')::int);


with country_cte as
         (select distinct
                  jsonb_array_elements(response_json -> 'production_countries') ->> 'iso_3166_1' as country_code,
                  jsonb_array_elements(response_json -> 'production_countries') ->> 'name'       as country_descr
          from tmdb_requests
          where request_type = 'movie')
insert into country
(
country_code,
country_descr
)
select country_code,
       country_descr
from   country_cte
where  country_code not in (select country_code from country);



select distinct jsonb_array_elements(response_json -> 'belongs_to_collection') ->> 'id' as collection_id,
                jsonb_array_elements(response_json -> 'belongs_to_collection') ->> 'name' as collection_descr

from tmdb_requests
where request_type = 'movie'

select distinct jsonb_array_elements(response_json -> 'belongs_to_collection'::text) ->
                'id'::text    as collection_id,
                jsonb_array_elements(response_json -> 'belongs_to_collection'::text) ->
                'name'::text  as collection_descr
from tmdb_requests
where request_type = 'movie'

drop table if exists company

create table company
(
    id             integer,
    title          varchar(100),
    homepage       text,
    logo_path      text,
    description    text,
    headquarters   text,
    origin_country text,
    parent_company text
);

alter table company
    owner to moraghan;


insert into company
select request_key as id,
       response_json->>'name' as title,
       response_json->>'homepage'  as homepage,
       response_json->>'logo_path' as logo_path,
       response_json->>'description' as description,
       response_json->>'headquarters' as headquarters,
       response_json->>'origin_country' as origin_country,
       response_json->>'parent_company' as parent_company

from tmdb_requests
where request_type = 'company' and response_status_code = 200

delete from tmdb_requests where request_type = 'country'
and response_status_code = 200

drop table if exists country

create table country
(
    country_code  char(2),
    country_descr text
)

alter table country
    owner to moraghan;

insert into country
select distinct jsonb_array_elements(response_json -> 'production_countries') ->> 'iso_3166_1' as country_code,
                jsonb_array_elements(response_json -> 'production_countries') ->> 'name' as country_descr

from tmdb_requests
where request_type = 'movie'

select distinct request_key,
                jsonb_array_elements(response_json -> 'production_countries') ->> 'iso_3166_1' as country_code

from tmdb_requests
where request_type = 'movie'

drop table if exists movie_country

create table movie_country
(
    movie_id integer,
    country_code  char(2)
);

alter table movie_country
    owner to moraghan;

insert into movie_country
select distinct request_key,
                jsonb_array_elements(response_json -> 'production_countries') ->> 'iso_3166_1' as country_code

from tmdb_requests
where request_type = 'movie'

select movie_id from movie_country group by movie_id having count(*) > 1

select * from tmdb_requests where request_key = 490024

drop table if exists movie_company

create table movie_company
(
    movie_id integer,
    company_id  integer
);

alter table movie_company
    owner to moraghan


insert into movie_company
select distinct request_key,
                (jsonb_array_elements(response_json -> 'production_companies') ->> 'id')::int as company_id

from tmdb_requests
where request_type = 'movie'

select * from tmdb_requests where request_type = 'genres'

drop table if exists movie_company

create table movie_company
(
    movie_id integer,
    company_id  integer
);

alter table movie_company
    owner to moraghan

drop table if exists genre

create table genre
(
    genre_id integer,
    genre_descr text
);

insert into genre
select distinct (jsonb_array_elements(response_json -> 'genres') ->> 'id')::int as genre_id,
                jsonb_array_elements(response_json -> 'genres') ->> 'name' as genre_descr

from tmdb_requests
where request_type = 'movie'

drop table if exists movie_genre

create table movie_genre
(
    movie_id integer,
    genre_id  integer
);

insert into movie_genre
select distinct request_key as movie_id,
                (jsonb_array_elements(response_json -> 'genres') ->> 'id')::int as company_id
from tmdb_requests
where request_type = 'movie'

select * from movie_genre

drop table if exists language

create table language
(
    language_code char(2),
    language_descr text,
    english_descr text
);

alter table language
    owner to moraghan;

insert into language
select distinct jsonb_array_elements(response_json -> 'spoken_languages') ->> 'iso_639_1' as language_code,
                jsonb_array_elements(response_json -> 'spoken_languages') ->> 'name' as language_descr,
                jsonb_array_elements(response_json -> 'spoken_languages') ->> 'english_name' as english_descr

from tmdb_requests
where request_type = 'movie'

drop table if exists movie_spoken_language

create table movie_spoken_language
(
    movie_id integer,
    language_code  char(2)
);

insert into movie_spoken_language
select distinct request_key as movie_id,
                jsonb_array_elements(response_json -> 'spoken_languages') ->> 'iso_639_1' as language_code
from tmdb_requests
where request_type = 'movie'


drop table if exists movie

create table movie
(
    movie_id              integer primary key ,
    title                 text,
    status                text,
    imdb_id               text,
    homepage              text,
    poster_path           text,
    backdrop_path         text,
    original_title        text,
    original_language     text,
    tagline               text,
    overview              text,
    belongs_to_collection text,
    runtime               text,
    release_date          date,
    budget                bigint,
    revenue               bigint,
    popularity            numeric(7,2),
    vote_count            integer,
    vote_average          numeric(5,2)
);

alter table movie
    owner to moraghan;

insert into movie
select request_key as movie_id,
       response_json->>'title' as title,
       response_json->>'status'  as status,
       response_json->>'imdb_id' as imdb_id,
       response_json->>'homepage'  as homepage,
       response_json->>'poster_path' as poster_path,
       response_json->>'backdrop_path' as backdrop_path,
       response_json->>'original_title' as original_title,
       response_json->>'original_language' as original_language,
       response_json->>'tagline' as tagline,
       response_json->>'overview' as overview,
       response_json->>'belongs_to_collection' as belongs_to_collection,
       response_json->>'runtime' as runtime,
       to_date(response_json->>'release_date','yyyy-mm-dd') as release_date,

       (response_json->>'budget')::bigint as budget,
       (response_json->>'revenue')::bigint as revenue,

       (response_json->>'popularity')::numeric(7,2) as popularity,
       (response_json->>'vote_count')::int as vote_count,
       (response_json->>'vote_average')::numeric(5,2) as vote_average

from tmdb_requests
where request_type = 'movie'

select * from tmdb_requests where request_type = 'credit' and request_key = 744
select * from tmdb_requests where request_type = 'person' and request_key = 500

select * from movie where title = 'Top Gun'
