digestServer
============
Simple file hash calculation service, using AWS SQS queue to receive orders with
files from AWS S3 service. Also uses SimpleDB to log actions, errors and results, and
to reduce effects of possible duplicated messages in SQS.

Link to task description: https://github.com/iis-io-team/psoir/blob/master/labs/lab6.pdf
