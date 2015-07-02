/*============================================================================*\
|  Author: Chris Loeper
|  Created:6.30.2015
|
|  Purpose:
|    This is a general purpose utility for detecting duplicate user records in
|    a JSON dataset. By default, duplicates are identified based on ID and 
|    email address. Records are consolidated by ID first, and email second.
|
|  Consolidation Methodology:
|    Once duplicates are indentified, an array of duplicates is created ordered
|    by date. The oldest record becomes the originating record. A delta of each
|    duplicate record is used to update the originating record sequentially
|    based on date. The result is a consolidated user record that contains the
|    most up-to-date user data.
|
\*============================================================================*/

"use strict"
var fs = require('fs');
var util = require('util');
var moment = require('moment'); // Used for change.log timestamping

// Keys used to identify duplicates.
var KEYS_WITH_DUPES = ['_id', 'email'];

// Changelog file
var changelog = fs.createWriteStream(__dirname + '/change.log', { flags: 'a' });

function main() {
    // Load user records, get the array of users in the 'leads' array.
    var users = loadUsers('leads.json')['leads'];

    // This does all of the heavy lifting.
    getResolveDuplicates(users, KEYS_WITH_DUPES);
}

/**
 * loadUsers
 *
 * Loads user records into a JSON object and returns them.
 * @param  {String} filename
 * @return {Object} users
 */
function loadUsers(filename) {
  var users = JSON.parse(fs.readFileSync(filename, 'utf8'));
  return users;
}

/**
 * getResolveDuplicates
 *
 * Takes a list of JSON user objects and keys. Calls transform functions
 * to aggregate the data based on key type and then normalizes record
 * aggregation back to the original format. Checks for duplicates and recurse
 * if any are found.
 *
 * @param  {Array} users
 * @param  {Array} keys
 */
function getResolveDuplicates(users, keys) {
    // Create user hash based on passed in users
    var userHash = (transformUsersToHashByKeys(users, keys));

    // Get a list of users back in the original format
    var consolidatedUsers = getConsolidatedUserList(userHash, keys);

    // Get a list of any duplicate values that may still exist in the dataset.
    // This can happen if record consolidation caused a new duplicate to occur
    // on a different key type when resolving known duplicates.
    var duplicateValues = getListOfDuplicateValues(consolidatedUsers, keys);
    
    if(duplicateValues.length > 0) {
        // If we still have duplicates, recurse.
        writelog("Duplicate values still exist: " + duplicateValues);
        getResolveDuplicates(consolidatedUsers, keys);
    } else {
        // We're good! Write the new user list and log it to the console.
        fs.writeFileSync('./consolidated-leads.json', util.inspect(consolidatedUsers) , 'utf-8'); 
        log(consolidatedUsers);
        log("Consolidated users written to consolidated-leads.json");
    }
}

/**
 * getConsolidatedUserList
 *
 * Transforms a userHash array to the original format. Consolidates any 
 * duplicates contained in the has via consolidateDuplicates.
 *
 * @param  {Array} userHash
 * @param  {Array} keys
 * @return {Array} consolidatedUsers
 */
function getConsolidatedUserList(userHash, keys) {
    var consolidatedUsers = [];
    for(var i in userHash) {
        for(var j in keys) {
            var key = keys[j];

            // This gives us our aggregate data based on key type.
            // (i.e. all data agregated by _id, or email)
            var hashByKey = userHash[key];
            
            var consolidatedUsers = []
            for(var k in hashByKey) {
                // This is where we determine if our bucket contains duplicates
                // or not. If so, we want to consolidate all of the records in
                // the bucket. If not, hurray. We have a probably unique record
                // that can be added back to the list.
                if(hashByKey[k].length > 1) {
                    consolidatedUsers.push(consolidateDuplicates(hashByKey[k], key));
                } else {
                    consolidatedUsers.push(hashByKey[k][0]);
                }
            }
        }
    }
    return consolidatedUsers;
}

/**
 * getListOfDuplicateValues
 *
 * Returns a list of values that are duplicated in the set of user records based
 * on key type (e.g. _id, email).
 *
 * @param  {Array} users
 * @param  {Array} keys
 * @return {Array} dupes
 */
function getListOfDuplicateValues(users, keys) {
    var list = [];
    var dupes = [];
    for(var i in users) {
        var user = users[i];

        for(var j in keys) {
            var key = keys[j];

            // This isn't a list of unique values, just used to check against
            // for duplicate values.
            if(list.indexOf(user[key]) == -1) {
                list.push(user[key]);
            }
            else {
                dupes.push(user[key]);
            }
        }
    }
    return dupes; // ['123','a@b.com']
}

/**
 * consolidateDuplicates
 *
 * Takes an array of records that are duplicates and consolidates the data from
 * oldest to newest. It modifies the oldest record with each subsequent newer
 * record. Checks for any missing keys and adds them.
 *
 * @param  {Array} dupes
 * @param  {Array} keys
 * @return {Object} oldestRecord
 */
function consolidateDuplicates(dupes, key) {
    writelog("Consolidating " + dupes.length + " records with duplicate " + key)
    
    // Grab the first object, it's the oldest in this case.
    var oldestRecord = dupes[0];
    
    writelog("Updating record with " + key + " of " + oldestRecord[key] + " and timestamp of " + oldestRecord.entryDate);

    for(var i in dupes) {
        var nextRecord = dupes[i];
        var keys = Object.keys(nextRecord);

        // We can to iterate over the keys of the next record in the dupe set
        // so that we can check for any new keys that may exist and also
        // as a handle to existing keys.
        for(var j in keys) {
            if(!oldestRecord.hasOwnProperty(keys[j])) {
                // New key check
                writelog("Oldest record does not contain key " + keys[j] + ". Adding.");
                oldestRecord[keys[j]] = nextRecord[keys[j]];
            } else {
                // Otherwise, we check if the value on the newer record is
                // different from the oldest. Update the oldest if so.
                if(nextRecord[keys[j]] !== oldestRecord[keys[j]]) {
                    writelog("Newer data found for " + keys[j] + ". Updating from " + oldestRecord[keys[j]] + " to " + nextRecord[keys[j]]);
                    oldestRecord[keys[j]] = nextRecord[keys[j]];
                }
            }
        }
    }
    return oldestRecord;
}

/**
 * transformUsersToHashByKeys
 *
 * Iterates through all users and keeps track of all unique values found for
 * each key type (e.x. _id, email). It then uses each unique value as a bucket
 * for any records that share the same value for the key types.
 *
 * @param  {Array} users
 * @param  {Array} keys
 * @return {Array} userHash
 */
function transformUsersToHashByKeys(users, keys) {
    // Init and generate our keyList and userHash
    var keyList  = generateKeyList(keys),
        userHash = initUserHash(keys)

    for(var i in users) {
        var user = users[i];

        // Iterate over the keyList.
        for(var j in keyList) {
            // Here we check to see if the current value of our key in the user
            // record already exists in the key list. If it does we ignore it,
            // and if it does we push it. This gives us a unique list of values
            // for the user dataset based on key type.
            if(keyList[j].list.indexOf(user[keyList[j].key]) == -1) {
                keyList[j].list.push(user[keyList[j].key]);
            }

            // Now we check the list we populated above. This is done to prevent
            // iterating over the entire user dataset each time we check for
            // unique values.
            for(var k in keyList[j].list) {
                var activeKey = keyList[j].key;

                // If we find that the value of the key on user record is the
                // same as the list item we're on, we check to see if we already
                // have a bucket for that particular value. If so, we push it to
                // the array, and if not, we create the array first then push.
                if(keyList[j].list[k] == user[activeKey]) {
                    if(userHash[activeKey].hasOwnProperty(user[activeKey])){
                        userHash[activeKey][user[activeKey]].push(user);
                    } else {
                        userHash[activeKey][user[activeKey]] = [];
                        userHash[activeKey][user[activeKey]].push(user);
                    }
                }
            }
        }
    }
    return userHash;
}

/**
 * generateKeyList
 *
 * Creates an object used to hold a list of unique values based on key type
 * as well as the type the list was created for.
 *
 * @param  {Array} keys
 * @return {Object} keyList
 */
function generateKeyList(keys) {
    var keyList = {};
    for(var i in keys) {
        keyList[keys[i]]         = {}
        keyList[keys[i]]['key']  = keys[i]
        keyList[keys[i]]['list'] = [];
    }
    
    /* Example object
    keyList: {
        _id: {
            key: '_id',
            list: [123, 234, 345]
        },
        email: {
            key: 'email',
            list: ['a@b.com', b@c.com]
        }
    }
    */
    return keyList;
}

/**
 * initUserHash
 *
 * Creates an object used to hold a hash of users based on key type.
 *
 * @param  {Array} keys
 * @return {Object} userHash
 */
function initUserHash(keys) {
    var userHash = {};
    for(var i in keys) {
        userHash[keys[i]] = []
    }

    /* Example object
    userHash: {
        _id: [
            '123': [{},{},...],
            '234': {{},{},...}
        ],
        email: [
            'a@b.com': [{},{},...],
            'b@c.com': [{},{},...]
        ]
    }
    */
    return userHash;
}

/**
 * log
 *
 * Useful while debugging. Less verbose, easier to use.
 *
 * @param {String} message
 */
function log(message) {
  console.log(message);
};

/**
 * writelog
 *
 * Write to the change log and also display the log in the console.
 *
 * @param {String} message
 */
function writelog(message) {
    log(message);
    changelog.write(moment().format() + ": " + message + "\n");
}

main();

// Close changelog
changelog.end();
