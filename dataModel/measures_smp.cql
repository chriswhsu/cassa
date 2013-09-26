use wherewhenwhat;

insert into measures (name, description, uom, datatype) values ('aparPower', 'Apparent Power','mVA', 'float');
insert into measures (name, description, uom, datatype) values ('actPower',  'Actual Power',  'mW',  'float');
insert into measures (name, description, uom, datatype) values ('actEnergy', 'Actual Energy', 'mWh', 'float');

insert into measures (name, description, uom, datatype) values ('humidity',  'Relative Humidity', '%', 'float');
insert into measures (name, description, uom, datatype) values ('temp',      'Temperature', 'degreesC', 'float');
insert into measures (name, description, uom, datatype) values ('pressure',  'Pressure', 'inHg', 'float');
insert into measures (name, description, uom, datatype) values ('solar_rad', 'Solar Radiation', 'W/m^2', 'float');
insert into measures (name, description, uom, datatype) values ('wind_dir',  'Wind Direction', 'degrees', 'float');
insert into measures (name, description, uom, datatype) values ('wind_speed','Wind Speed', 'mph', 'float');

insert into measures (name, description, uom, datatype) values ('rfid_scan',  'RFID Scan', 'true', 'boolean');
