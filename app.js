var helpers = require('./helpers');
var AWS = require('aws-sdk');

var AWS_CONFIG_FILE = './config.json';
AWS.config.loadFromPath(AWS_CONFIG_FILE);
var simpleLogger = require('./simpleLogger.js');

var sqsURL = 'https://sqs.us-west-2.amazonaws.com/983680736795/MarcinczykSQS';
var sqs = new AWS.SQS();
var s3labMsgID = 'S3labSQS';
var s3 = new AWS.S3();

var onMessageReceivedFunc = function(receivedCb) {
  return function(err, data){
    if(err) {
      receivedCb(err);
      return;
    }
    if(data.Messages) {
      var msg = data.Messages[0];
      sqs.deleteMessage({
        QueueUrl: sqsURL,
        ReceiptHandle: msg.ReceiptHandle
      }, function(err){
        if(err){
          receivedCb(err);
          return;
        }
        receivedCb(null, msg);
      });
    } else {
      receivedCb();
    }
  }
};

var receiveMessage = function(receivedCb) {
  console.log('Waiting for orders..');
  sqs.receiveMessage({
      QueueUrl: sqsURL,
      MaxNumberOfMessages: 1,
      VisibilityTimeout: 30,
      WaitTimeSeconds: 20,
      MessageAttributeNames: ['All']
    },
    onMessageReceivedFunc(receivedCb)
  );
}

var keepListening = function(){
  receiveMessage(handleMessage);
}

var logDigests = function(logData){
  return function(err, digests){
    digests.forEach(function(hash){
    	var arr = hash.split(':');
    	logData[arr[0]] = arr[1].trim();
    });
    simpleLogger.info('S3 file hash calculated', logData);
    keepListening();
  };
};

var calcDigests = function(logData) {
  return function(err, data){
    if(err) {
      console.log(err);
      return keepListening();
    }

    helpers.calculateMultiDigest(
      data.Body,
      ['md5', 'sha1', 'sha256', 'sha512'],
      logDigests(logData), 1);
  };
};

var handleMessage = function(err, msg) {
  if(err){
    console.log(err);
    setTimeout(keepListening, 10000);
    return;
  }
  if(!msg){
    console.log('No data received.');
    return keepListening();
  }
  console.log(msg);
  var recvMsgID = msg.MessageAttributes.id.StringValue;
  if(recvMsgID != s3labMsgID){
    console.log('Unknown SQS message id: ' + recvMsgID);
    return keepListening();
  }
  // TODO check the log for the same request
  var s3fileInfo = {
    Bucket: msg.MessageAttributes.s3bucket.StringValue,
    Key: msg.MessageAttributes.s3key.StringValue,
  };
  var logData = { s3bucket: s3fileInfo.Bucket, s3key: s3fileInfo.Key };
  simpleLogger.info('Received S3 file hash calculation request from SQS', logData);
  s3.getObject(s3fileInfo, calcDigests(logData));
}


console.log('Digest server started.');
keepListening();
