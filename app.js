var helpers = require("./helpers");
var AWS = require("aws-sdk");

var AWS_CONFIG_FILE = "./config.json";
AWS.config.loadFromPath(AWS_CONFIG_FILE);

var sqsURL = "https://sqs.us-west-2.amazonaws.com/983680736795/MarcinczykSQS";
var sqs = new AWS.SQS();

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
        receivedCb(null, data);
      });
    } else {
      receivedCb(null, null);
    }
  }
};

var receiveMessage = function(receivedCb) {
  console.log("Waiting for orders..");
  sqs.receiveMessage({
      QueueUrl: sqsURL,
      MaxNumberOfMessages: 1,
      VisibilityTimeout: 30,
      WaitTimeSeconds: 20,
      MessageAttributeNames: ["All"]
    },
    onMessageReceivedFunc(receivedCb)
  );
}

var keepListening = function(){
  receiveMessage(handleMessage);
}

var handleMessage = function(err, data) {
  if(err){
    console.log(err);
    return keepListening();
  }
  if(!data){
    console.log("No data received.");
    return keepListening();
  }
  console.log(data);
  keepListening();
}

console.log("Digest server started.");
keepListening();
