create database commentOnYtb;
create table commentOnYtb.videoCmt(
videoId char(11),
updateAt datetime,
authorId char(24),
cmt varchar(10000),
label int(1),
PRIMARY KEY (videoId, updateAt, authorId)
);

create table commentOnYtb.video(
videoId char(11),
lastUpdate datetime,
PRIMARY KEY (videoId)
);
drop table commentonytb.tmp;
drop table commentonytb.videoCmt;
select * from commentonytb.videocmt;
select * from commentonytb.video;

