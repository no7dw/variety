var ObjectId = require('objectid')
var log = function(message) {
    console.log(message);
};

var readConfig = function(configProvider) {
    var config = {};
    var read = function(name, defaultValue) {
        var value = typeof configProvider[name] !== 'undefined' ? configProvider[name] : defaultValue;
        config[name] = value;
        // log('Using ' + name + ' of ' + tojson(value));
    };
    read('collection', null);
    read('query', {});
    // read('limit', db.getCollection(config.collection).find(config.query).count());
    read('maxDepth', 99);
    read('sort', { _id: -1 });
    read('outputFormat', 'ascii');
    read('persistResults', false);
    read('resultsDatabase', 'varietyResults');
    // read('resultsCollection', collection + 'Keys');
    read('resultsUser', null);
    read('resultsPass', null);
    read('logKeysContinuously', false);
    read('excludeSubkeys', []);
    read('arrayEscape', 'XX');
    read('lastValue', true);

    //Translate excludeSubkeys to set like object... using an object for compatibility...
    config.excludeSubkeys = config.excludeSubkeys.reduce(function(result, item) { result[item + '.'] = true; return result; }, {});

    return config;
};

var config = readConfig(this);
var varietyTypeOf = function(thing) {
    if (!arguments.length) { throw 'varietyTypeOf() requires an argument'; }

    if (typeof thing === 'undefined') {
        return 'undefined';
    } else if (typeof thing !== 'object') {
        // the messiness below capitalizes the first letter, so the output matches
        // the other return values below. -JC
        var typeofThing = typeof thing; // edgecase of JSHint's "singleGroups"
        return typeofThing[0].toUpperCase() + typeofThing.slice(1);
    } else {
        if (thing && thing.constructor === Array) {
            return 'Array';
        } else if (thing === null) {
            return 'null';
        } else if (thing instanceof Date) {
            return 'Date';
        } else if (thing instanceof ObjectId) {
            return 'ObjectId';
            // } else if (thing instanceof BinData) {
            //     var binDataTypes = {};
            //     binDataTypes[0x00] = 'generic';
            //     binDataTypes[0x01] = 'function';
            //     binDataTypes[0x02] = 'old';
            //     binDataTypes[0x03] = 'UUID';
            //     binDataTypes[0x04] = 'UUID';
            //     binDataTypes[0x05] = 'MD5';
            //     binDataTypes[0x80] = 'user';
            //     return 'BinData-' + binDataTypes[thing.subtype()];
        } else {
            return 'Object';
        }
    }
};
// convert document to key-value map, where value is always an array with types as plain strings
var analyseDocument = function(document) {
    var result = {};
    var arrayRegex = new RegExp('\\.' + config.arrayEscape + '\\d+' + config.arrayEscape, 'g');
    for (var key in document) {
        var value = document[key];
        key = key.replace(arrayRegex, '.' + config.arrayEscape);
        if (typeof result[key] === 'undefined') {
            result[key] = {};
        }
        var type = varietyTypeOf(value);
        result[key][type] = null;

        if (config.lastValue) {
            if (type in { 'String': true, 'Boolean': true }) {
                result[key][type] = value.toString();
            } else if (type in { 'Number': true, 'NumberLong': true }) {
                result[key][type] = value.valueOf();
            } else if (type == 'ObjectId') {
                result[key][type] = value.str;
            } else if (type == 'Date') {
                result[key][type] = new Date(value).getTime();
            } else if (type.startsWith('BinData')) {
                result[key][type] = value.hex();
            }
        }
        // result[key][type] = value.length
    }

    return result;
};

var mergeDocument = function(docResult, interimResults, doc) {

    for (var key in docResult) {
        if (key in interimResults) {
            var existing = interimResults[key];

            for (var type in docResult[key]) {
                if (type in existing.types) {
                    existing.types[type] = existing.types[type] + 1;

                } else {
                    existing.types[type] = 1;
                    if (config.logKeysContinuously) {
                        log('Found new key type "' + key + '" type "' + type + '"');
                    }
                }
                //docResult[key].length existing.types.length existing.types['type']

                if (doc[key] && doc[key].length > existing.mLength)
                    existing.mLength = doc[key].length
            }
            existing.totalOccurrences = existing.totalOccurrences + 1;
        } else {
            var lastValue = null;
            var mLength = 0
            var types = {};
            for (var newType in docResult[key]) {
                types[newType] = 1;
                lastValue = docResult[key][newType];
                mLength = lastValue ? lastValue.length : 0
                if (config.logKeysContinuously) {
                    log('Found new key type "' + key + '" type "' + newType + '"');
                }
            }
            interimResults[key] = { 'types': types, 'totalOccurrences': 1 };
            if (config.lastValue) {
                interimResults[key]['lastValue'] = lastValue ? lastValue : '[' + newType + ']';
                interimResults[key]['mLength'] = mLength
            }
        }
    }
};

var convertResults = function(interimResults, documentsCount) {
    var getKeys = function(obj) {
        var keys = {};
        for (var key in obj) {
            keys[key] = obj[key];
        }
        return keys;
        //return keys.sort();
    };
    var varietyResults = [];
    //now convert the interimResults into the proper format
    for (var key in interimResults) {
        var entry = interimResults[key];

        var obj = {
            '_id': { 'key': key },
            'value': { 'types': getKeys(entry.types) },
            'totalOccurrences': entry.totalOccurrences,
            'percentContaining': entry.totalOccurrences * 100 / documentsCount
        };

        if (config.lastValue) {
            obj.lastValue = entry.lastValue;
            obj.mLength = entry.mLength
        }

        varietyResults.push(obj);
    }
    return varietyResults;
};
//flattens object keys to 1D. i.e. {'key1':1,{'key2':{'key3':2}}} becomes {'key1':1,'key2.key3':2}
//we assume no '.' characters in the keys, which is an OK assumption for MongoDB
var serializeDoc = function(doc, maxDepth, excludeSubkeys) {
    var result = {};

    //determining if an object is a Hash vs Array vs something else is hard
    //returns true, if object in argument may have nested objects and makes sense to analyse its content
    function isHash(v) {
        var isArray = Array.isArray(v);
        var isObject = typeof v === 'object';
        var specialObject = v instanceof Date ||
            v instanceof ObjectId
            // v instanceof BinData ||
            // v instanceof NumberLong;
        return !specialObject && (isArray || isObject);
    }

    var arrayRegex = new RegExp('\\.' + config.arrayEscape + '\\d+' + config.arrayEscape + '\\.', 'g');

    function serialize(document, parentKey, maxDepth) {
        if (Object.prototype.hasOwnProperty.call(excludeSubkeys, parentKey.replace(arrayRegex, '.')))
            return;
        for (var key in document) {
            //skip over inherited properties such as string, length, etch
            if (!document.hasOwnProperty(key)) {
                continue;
            }
            var value = document[key];
            if (Array.isArray(document))
                key = config.arrayEscape + key + config.arrayEscape; //translate unnamed object key from {_parent_name_}.{_index_} to {_parent_name_}.arrayEscape{_index_}arrayEscape.
            result[parentKey + key] = value;
            //it's an object, recurse...only if we haven't reached max depth
            if (isHash(value) && maxDepth > 1) {
                serialize(value, parentKey + key + '.', maxDepth - 1);
            }
        }
    }
    serialize(doc, '', maxDepth);
    return result;
};
// Merge the keys and types of current object into accumulator object
var reduceDocuments = function(accumulator, object) {
    var docResult = analyseDocument(serializeDoc(object, config.maxDepth, config.excludeSubkeys));
    mergeDocument(docResult, accumulator, object);
    return accumulator;
};


// sort desc by totalOccurrences or by key asc if occurrences equal
var comparator = function(a, b) {
    var countsDiff = b.totalOccurrences - a.totalOccurrences;
    return countsDiff !== 0 ? countsDiff : a._id.key.localeCompare(b._id.key);
};

// extend standard MongoDB cursor of reduce method - call forEach and combine the results


var cursor = [{

        "phone": "15622622622",
        "password": "$2a$10$KmZ3bvtYaNikzY86JKPn8.ZAQY2PHrEzQfDKhmD6SL3p5J4A511XW",
        "isIdentifyAudit": false,
        "risk": true,
        "status": "register",
        "photo": {

        },
        "bankcards": [],
        "__v": 0,
        "contactsInfo": [{
                "deleted": "false",
                "mobile": "4008881388",
                "name": "360手机售后客服",
            },
            {
                "deleted": "false",
                "mobile": "15015514222",
                "name": "黄小勇",
            }
        ]
    },
    {

        "phone": "18297793567",
        "password": "$2a$10$PJgsEZwWBAHUGA12osmay.VXH0RM4fpXU.DnS8tLBTFxvb7Y78Ikq",
        "isIdentifyAudit": false,
        "risk": false,
        "status": "verified",
        "photo": {

        },
        "bankcards": [{
            "bankName": "中国工商银行",
            "bankcode": "ICBC",
            "phone": "18297793567",
            "bankcardNo": "6222021309007262607",
        }],
        "__v": 0,
        "idCard": "340827199312021314",
        "name": "高少东",
        "traderPassword": "$2a$10$XU5WIZwUxoYwZPhVPyKgluE5pgV32K8T2gnWaw.7aiegFaBdLjeKi",
        "tags": [{
                "name": "正常",
                "type": "audit"
            },
            {
                "name": "可疑",
                "_id": ObjectId("5a30dc287258be0019d45719"),
                "type": "audit"
            },
            {
                "name": "损失",
                "_id": ObjectId("5a30dc287258be0019d4571d"),
                "type": "system"
            }
        ],
        "extraInfoValid": false,
        "contactsInfo": [{
                "_id": ObjectId("5a006a8a448fec001781550d")
            },
            {
                "deleted": "false",
                "mobile": "7270976",
                "name": "大妈",
                "_id": ObjectId("5a026d4e3ae6aa0017b5c188")
            },
            {
                "deleted": "false",
                "mobile": "15922368205",
                "name": "五爷",
                "_id": ObjectId("5a026d4e3ae6aa0017b5c187")
            },

        ]
    }
]

// var cursor = db.getCollection(config.collection).find(config.query).sort(config.sort).limit(config.limit);
cursor.reduce = function(callback, initialValue) {
    var result = initialValue;
    this.forEach(function(obj) {
        result = callback(result, obj);
    });
    return result;
};
var interimResults = cursor.reduce(reduceDocuments, {});
var varietyResults = convertResults(interimResults, 1)
    // console.log(varietyResults)
var createAsciiTable = function(results) {
    var headers = ['key', 'types', 'occurrences', 'percents'];
    if (config.lastValue) {
        headers.push('lastValue', 'mLength');
    }

    // return the number of decimal places or 1, if the number is int (1.23=>2, 100=>1, 0.1415=>4)
    var significantDigits = function(value) {
        var res = value.toString().match(/^[0-9]+\.([0-9]+)$/);
        return res !== null ? res[1].length : 1;
    };

    var maxDigits = varietyResults.map(function(value) { return significantDigits(value.percentContaining); }).reduce(function(acc, val) { return acc > val ? acc : val; });

    var rows = results.map(function(row) {
        var types = [];
        var typeKeys = Object.keys(row.value.types);
        if (typeKeys.length > 1) {
            for (var type in row.value.types) {
                var typestring = type + ' (' + row.value.types[type] + ')';
                types.push(typestring);
            }
        } else {
            types = typeKeys;
        }

        var rawArray = [row._id.key, types, row.totalOccurrences, row.percentContaining.toFixed(Math.min(maxDigits, 20))];
        if (config.lastValue && row['lastValue']) {
            rawArray.push(row['lastValue']);
            rawArray.push(row['mLength']);
        }
        return rawArray;
    });
    var table = [headers, headers.map(function() { return ''; })].concat(rows);
    var colMaxWidth = function(arr, index) { return Math.max.apply(null, arr.map(function(row) { return row[index] ? row[index].toString().length : 0; })); };
    var pad = function(width, string, symbol) { return width <= string.length ? string : pad(width, isNaN(string) ? string + symbol : symbol + string, symbol); };
    table = table.map(function(row, ri) {
        return '| ' + row.map(function(cell, i) {
            return pad(colMaxWidth(table, i), cell.toString(), ri === 1 ? '-' : ' ');
        }).join(' | ') + ' |';
    });
    var border = '+' + pad(table[0].length - 2, '', '-') + '+';
    return [border].concat(table).concat(border).join('\n');
};
console.log(createAsciiTable(varietyResults));