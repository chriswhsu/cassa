use wherewhenwhat;

truncate devices;
                                                                                           
insert into devices (device_id,geohash,name,measures,tags) values (1,'9q9p3yyrn1','Acme1',{'aparPower','actPower','actEnergy'},{'make':'Acme'});
insert into devices (device_id,geohash,name,measures,tags) values (2,'9q9p3yyrn3','Acme2',{'aparPower','actPower','actEnergy'},{'make':'Acme'});
insert into devices (device_id,geohash,name,measures,tags) values (3,'9q9p3yyrn5','Acme3',{'aparPower','actPower','actEnergy'},{'make':'Acme'});

insert into devices (device_id,name,tags) values (4,'WeatherStation',{'make':'Davis','model':'VantagePro 2'});
insert into devices (device_id,geohash,name,parent_device_id,measures) values (5,'9q9p3yyrna','Receiving Console',4,{'humidity','temp'});
insert into devices (device_id,geohash,name,parent_device_id,measures) values (6,'9q9p3yyrnb','Outdoor Unit',4,{'humidity','temp','pressure','solar_rad','wind_dir','wind_speed'});

insert into devices (device_id,geohash,name,parent_device_id,measures) values (7,'9q9p3yyrn1','RFID Reader',4,{'rfid_scan'});