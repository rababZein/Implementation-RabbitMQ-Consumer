const amqp = require('amqplib/callback_api');
const mysql = require('mysql');

amqp.connect('amqps://sefbsgbu:F84ZXipYzfADFygHss5_L-Bass0ZTsfk@jellyfish.rmq.cloudamqp.com/sefbsgbu',
 function(error0, connection) {
  if (error0) {
    throw error0;
  }
  connection.createChannel(function(error1, channel) {
    if (error1) {
      throw error1;
    }
    var queue = 'User-messages';

    channel.assertQueue(queue, {
      durable: true
    });
    channel.prefetch(1);
    
    console.log("Waiting for messages in %s", queue);
    channel.consume(queue, function(msg) {

      var data = JSON.parse(msg.content);
      console.log("Received '%s'", data);

      var con = mysql.createConnection({  
          host: "localhost",  
          user: "root",  
          password: "123",  
          database: "nodejs"  
      });  
      con.connect(function(err) {  
          if (err) throw err;  

          console.log("Connected to Mysql!"); 

          var sql = "INSERT INTO guest ( name, phone, email) VALUES ('"+data.name+"', '"+data.phone+"', '"+data.email+"')";  
          con.query(sql, function (err, result) {  
          if (err) throw err;  
          console.log("1 record inserted");  
          
          // Send acknowledgment
          channel.ack(msg)
          });  
      });
      
    }, { noAck: false } );
  });
});