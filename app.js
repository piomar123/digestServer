var helpers = require('./helpers');
var AWS = require('aws-sdk');

var AWS_CONFIG_FILE = './config.json';
AWS.config.loadFromPath(AWS_CONFIG_FILE);
var simpleLogger = require('./simpleLogger.js');

var sqsURL = 'https://sqs.us-west-2.amazonaws.com/983680736795/MarcinczykSQS';
var sqs = new AWS.SQS();
var s3labMsgID = 'S3labSQS';

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

var calcDigests = function(err, data){
  if(err) return next(err);

  helpers.calculateMultiDigest(
    data.Body,
    ['md5', 'sha1', 'sha256', 'sha512'],
    showDigests, 1);
};

var handleMessage = function(err, msg) {
  if(err){
    console.log(err);
    return keepListening();
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
  simpleLogger.info('Received S3 file hash calculation from SQS', {
    s3bucket: msg.MessageAttributes.s3bucket.StringValue,
    s3key: msg.MessageAttributes.s3key.StringValue,
  });
  keepListening();
}

console.log('Digest server started.');
keepListening();
