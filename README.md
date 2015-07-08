# user-data-consolidator

##Purpose
This is a general purpose utility for detecting duplicate user records in
a JSON dataset. By default, duplicates are identified based on ID and 
email address. Records are consolidated by ID first, and email second.

##Consolidation Methodology:
Once duplicates are indentified, an array of duplicates is created ordered
by date. The oldest record becomes the originating record. A delta of each
duplicate record is used to update the originating record sequentially
based on date. The result is a consolidated user record that contains the
most up-to-date user data.


##Installation
```
npm install
```

##Running
```
node app.js
```
