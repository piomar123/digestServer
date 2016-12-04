var AWS = require('aws-sdk');
var uuid = require('node-uuid');
var moment = require('moment');
var DBdomain = 'piotr.marcinczyk.logs';
var logItemPrefix = 'lab-log-';
AWS.config.loadFromPath('./config.json');

var simpledb = new AWS.SimpleDB();

simpledb.createDomain({ DomainName: DBdomain }, function(err, data) {
	if(err) {
		console.log(err, err.stack);
		return;
	}
	console.log(data);
});

// simpledb.listDomains({}, function(err, data){
// 	if(err) {
// 		console.log(err);
// 		return;
// 	}
// 	console.log(data);
// });

var afterLogFunc = function(err, logParams){
	if(err) {
		console.log(err);
		console.log(logParams);
		return;
	}
};

var log = function(level, message, details){
	var logParams = {
		DomainName: DBdomain,
		ItemName: logItemPrefix + uuid.v4(),
		Attributes: [
			{
				Name: 'timestamp',
				Value: moment().format('YYYY-MM-DD HH:mm:ss.SSS')
			},
			{
				Name: 'level',
				Value: level
			},
			{
				Name: 'message',
				Value: message
			}
		]
	}
	Object.keys(details).forEach(function(key){
		logParams.Attributes.push({
			Name: key,
			Value: details[key]
		});
	});
	simpledb.putAttributes(logParams,
		function(err,data) {console.log(data); return afterLogFunc(err, logParams)});
};

var getLogs = function(cb){
	simpledb.select({
		SelectExpression: 'SELECT * FROM `'+
			DBdomain +
			'` WHERE timestamp IS NOT NULL ORDER BY timestamp'
	}, function(err, data){
		if(err) {
			return cb(err);
		}
		var out = { raw: data, parsed: [] };
		data.Items.forEach(function(item){
			var entry = { id: item.Name };
			item.Attributes.forEach(function(attr){
				entry[attr.Name] = attr.Value;
			});
			out.parsed.push(entry);
		});
		return cb(null, out);
	});
};

var logInfo  = function(message, details){ return log('info',  message,details); };
var logWarn  = function(message, details){ return log('warn',  message,details); };
var logError = function(message, details){ return log('error', message,details); };

exports.info   	= logInfo;
exports.warning = logWarn;
exports.error   = logError;
exports.getLogs = getLogs;
