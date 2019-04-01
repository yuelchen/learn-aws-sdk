const aws = require('aws-sdk');
const s3 = new aws.S3();

var https = require('https');
var response = ''

//lambda function handler info would be filename.handler (i.e. HttpPostMethod.handler)
exports.handler = function(event, context, callback){
  //log json event to console
  console.info("Event: " + JSON.stringify(event));
    
  var s3Object = {
    Bucket: 'myBucketName',
    Key: 'my/object/prefix'
  }
  
  s3.getObject(s3Object, function(err, data){
    if(!err){
      //store s3 object content in variable for parsing
      body = Buffer.from(data.Body).toString('utf8');
      
      //content format should be parsed as known (i.e. Account=user1Password=pass1)
      let credentials = JSON.stringify({
        "username":body.substring(body.indexOf("Account=") + "Account=".length, body.indexOf("Password=")).trim(),
        "password":body.substring(body.indexOf("Password=") + "Password=".length).trim()
      });
      
      //only after getting object should you make api call with username and password
      var request = {
        host: 'example.com',
        rejectUnauthorized: false, //this is related to cert issue, only set false if you want to bypass - not recommended for production (https://stackoverflow.com/questions/31673587/error-unable-to-verify-the-first-certificate-in-nodejs)
        path: '/api/v2/login',
        method: 'POST',
        json: true,
        headers: {
          "content-type": "application/json",
          "accept": "application/json",
          'Content-Length': Buffer.byteLength(credentials)
        },
      }
      
      //make request and handle response   
      var connection = https.request(request, function(response){
        var output = "";
        response.on('data', function(chunk){
          output += chunk;
        });
        response.on('end', function(){
          console.log("HTTP POST Response: " + output);
        });
      });
      
      //close connection
      connection.write(credentials);
      connection.end();
      
    }//end if(!err)
    else{
      console.err("Unable to retrieve object content from S3")
    }
  });
}
