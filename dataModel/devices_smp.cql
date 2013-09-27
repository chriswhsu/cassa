use sense;

truncate devices;
                                                                                           
insert into devices (device_id, geohash,name, external_identifier, measures, tags) values (c37d661d-7e61-49ea-96a5-68c34e83db3a,'9q9p3yyrn1', 'Acme1', '936', {'aparPower','actPower','actEnergy'},{'make':'Acme'});
insert into devices (device_id, geohash,name, external_identifier, measures, tags) values (7a6a3557-a23d-4e34-8b76-80b74490db70,'9q9p3yyrn3', 'Acme2', '94e', {'aparPower','actPower','actEnergy'},{'make':'Acme'});
insert into devices (device_id, geohash,name, external_identifier, measures, tags) values (e0e6c13e-4686-4b75-b62a-e7b406baa36e,'9q9p3yyrn5', 'Acme3', '8b9', {'aparPower','actPower','actEnergy'},{'make':'Acme'});

insert into devices (device_id,name,tags) values (dd1b0837-146e-4926-9a6a-155e2ffb885a,'WeatherStation',{'make':'Davis','model':'VantagePro 2'});
insert into devices (device_id,geohash,name,parent_device_id,measures) values (22c06d59-f4f2-4e77-9e4e-9df043ccf5df,'9q9p3yyrna','Receiving Console',dd1b0837-146e-4926-9a6a-155e2ffb885a,{'humidity','temp'});
insert into devices (device_id,geohash,name,parent_device_id,measures) values (695118d1-b5ca-4410-8790-466bcdfa573b,'9q9p3yyrnb','Outdoor Unit',dd1b0837-146e-4926-9a6a-155e2ffb885a,{'humidity','temp','pressure','solar_rad','wind_dir','wind_speed'});

insert into devices (device_id,geohash,name,parent_device_id,measures) values (84d1a535-bdef-4c72-8972-449287b6b6e8,'9q9p3yyrn1','RFID Reader',null,{'rfid_scan'});
