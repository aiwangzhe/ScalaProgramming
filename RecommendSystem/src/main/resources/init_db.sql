
create table movie (mid int, name string, descri string, timelong string, issue string,
                   shoot string, language string, genres string, actors string, directors string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '^';

create table rating (uid int, mid int, score double, `timestamp` int)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

create table tag (uid int, mid int, tag string, `timestamp` int)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';