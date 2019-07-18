class SqlQueries:
    songplay_table_insert = ("""
    truncate table songplays;
    
    INSERT INTO songplays(start_time,user_id,level,song_id ,artist_id, session_id,location,user_agent)
        SELECT
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    user_table_insert = ("""
    truncate table users;
    
    INSERT INTO users (userid,firstname,lastname ,gender ,level )
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong' and userid is not null
    """)

    song_table_insert = ("""
    truncate table songs;
    
    INSERT INTO songs (songid, title,artistid ,year,duration)
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
    truncate table artists;
    
    INSERT INTO artists (artistid, name,location,lattitude,longitude)
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
    truncate table time;
    
    INSERT INTO time(start_time, hour, day, week, month , year , weekday)
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """)
        
    data_check_qry = ("""
                select count(*), 'users' as tablename from public.users
                union
                select count(*), 'artists' from public.artists
                union
                select count(*), 'songs' from public.songs
                union
                select count(*), 'time' from public.time
                union
                select count(*), 'songplays' from public.songplays
                union
                select count(*), 'staging_events' from public.staging_events
                union
                select count(*), 'staging_songs' from public.staging_songs
    """)