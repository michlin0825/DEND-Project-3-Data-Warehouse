import configparser

# CONFIG

config = configparser.ConfigParser()
config.read('dwh.cfg')


# DROP TABLES

staging_events_table_drop = "drop table if exists staging_events_table"
staging_songs_table_drop = "drop table if exists staging_songs_table"
songplay_table_drop = "drop table if exists songplay_table"
user_table_drop = "drop table if exists user_table"
song_table_drop = "drop table if exists song_table"
artist_table_drop = "drop table if exists artist_table"
time_table_drop = "drop table if exists time_table"


# CREATE TABLES

staging_events_table_create= ("""create table if not exists staging_events(
                                artist text,
                                auth text,
                                first_name text,
                                gender char(1),
                                item_session integer,
                                last_name text,
                                length numeric,
                                level text,
                                location text,
                                method Text,
                                page text,
                                registration numeric,
                                session_id integer,
                                song text,
                                status integer,
                                ts bigint,
                                user_agent text,
                                user_id integer
                            )
""")


staging_songs_table_create = ("""create table if not exists staging_songs(
                                num_songs integer,
                                artist_id text,
                                artist_latitude numeric,
                                artist_longitude numeric,
                                artist_location text,
                                artist_name text,
                                song_id text,
                                title text,
                                duration numeric,
                                year integer
                            )
""")


songplay_table_create = ("""create table if not exists songplay(
                            songplay_id int identity(1,1) not null primary key,
                            start_time timestamp references time(start_time) not null sortkey,
                            user_id integer references users(user_id) not null,
                            level text,
                            song_id text references songs(song_id) not null distkey,
                            artist_id text references artists(artist_id) not null,
                            session_id integer,
                            location text,
                            user_agent text
                        )
""")


user_table_create = ("""create table if not exists users(
                        user_id integer not null primary key,
                        first_name text not null,
                        last_name text not null,
                        gender char(1),
                        level text
                    )
""")

song_table_create = ("""create table if not exists songs(
                        song_id text not null primary key,
                        title text,
                        artist_id text not null,
                        year integer,
                        duration numeric
                    )
""")

artist_table_create = ("""create table if not exists artists(
                          artist_id text not null primary key, 
                          name text,
                          location text,
                          latitude numeric,
                          longitude numeric
                       )
""")

time_table_create = ("""create table if not exists time(
                        start_time timestamp not null primary key,
                        hour integer,
                        day integer,
                        week integer,
                        month integer,
                        year integer,
                        weekDay integer
                    )
""")


# STAGING TABLES

staging_events_copy = ("""copy staging_events 
                          from {}
                          iam_role {}
                          region 'us-west-2'
                          compupdate off 
                          json {};
                       """).format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])


staging_songs_copy = ("""copy staging_songs 
                          from {} 
                          iam_role {}
                          region 'us-west-2'
                          compupdate off 
                          json 'auto';
                      """).format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])



# FINAL TABLES

songplay_table_insert = ("""insert into songplay(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
                            select distinct timestamp 'epoch' + se.ts/1000 * interval '1 second' as start_time, se.user_id, se.level, 
                            ss.song_id, ss.artist_id, se.session_id, se.location, se.user_agent
                            from staging_events se, staging_songs ss
                            where se.page = 'NextSong' and
                            se.song =ss.title and
                            se.artist = ss.artist_name and
                            se.length = ss.duration
""")

user_table_insert = ("""insert into users(user_id, first_name, last_name, gender, level)
                        select distinct user_id, first_name, last_name, gender, level
                        from staging_events
                        where page = 'NextSong'
""")

song_table_insert = ("""insert into songs(song_id, title, artist_id, year, duration)
                        select distinct song_id, title, artist_id, year, duration
                        from staging_songs
                        where song_id is not null
""")

artist_table_insert = ("""insert into artists(artist_id, name, location, latitude, longitude)
                          select distinct artist_id, artist_name, artist_location , artist_latitude, artist_longitude 
                          from staging_songs
                          where artist_id is not null
""")

time_table_insert = ("""insert into time(start_time, hour, day, week, month, year, weekDay)
                        select distinct start_time, extract(hour from start_time), extract(day from start_time),
                        extract(week from start_time), extract(month from start_time),
                        extract(year from start_time), extract(dayofweek from start_time)
                        from songplay
""")



# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
