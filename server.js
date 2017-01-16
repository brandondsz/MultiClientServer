var http = require('http') 
  , bodyParser = require('body-parser')
  , express = require('express')
  , mongodb = require('mongodb');

var app = express();

app.use(function (req, res, next) {
  res.header("Access-Control-Allow-Origin", "*");
  res.header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");
  next();
});

var MongoClient = mongodb.MongoClient;
var url = 'mongodb://localhost:27017/transport_db';
MongoClient.connect(url, function (err, db) {
  if (err) console.log('Unable to connect to the mongoDB server. Error:', err);

  app.use(bodyParser.json()); // support json encoded bodies
  app.use(bodyParser.urlencoded({ extended: true }));


  app.get('/systemHealth', function (req, res) {
    //res.send({ id: req.params.id, name: "The Name", description: "description" });
  });

  app.get('/devices', function (req, res) {
    db.collection('devices', function (err, collection) {
      collection.distinct('device_id', {}, function (err, items) {
        res.send(items);
      });
    });
  });

  app.get('/geoLocation/:deviceID/:fromDate/:toDate', function (req, res) {
    //add conversion if needed
    var fromDate = parseInt(req.params.fromDate);
    var toDate = parseInt(req.params.toDate);
    var deviceID = req.params.deviceID;
    db.collection('devices').find({
      $and:
      [{ "device_id": deviceID },
      {
        $and:
        [{ "timestamp": { $gte: fromDate } },
        { "timestamp": { $lte: toDate } }]
      }]
    },
      {
        latitude: 1,
        longitude: 1,
        timestamp: 1,
        status: 1,
        speed: 1,
        _id: 0
      }).toArray(function (err, items) {
        res.send(items);
      })
  })

  app.get('/geoOverSpeeding/:fromDate/:toDate', function (req, res) {
    //Assuming the required output here is a list of devices that had speed of
    //more than 60 for more than 40 seconds(might not be 40 continuous seconds)

    var fromDate = parseInt(req.params.fromDate);
    var toDate = parseInt(req.params.toDate);
    db.collection('devices')
      .aggregate([
        {
          $match: {
            "speed": { $gt: 60 },
            "timestamp": { $gte: fromDate, $lte: toDate }
          }
        },
        {
          $group: {
            _id: "$device_id",
            count: { $sum: 1 }
          }
        },
        {
          //assuming each entry has a 10 second interval
          $match: { count: { $gt: 4 } }
        }
      ])
      .toArray(function (err, items) {
        res.send(items);
      });
  })

  app.get('/geoDwell/:latitude/:longitude/:fromDate/:toDate', function (req, res) {
    //TODO: complete
    var fromDate = parseInt(req.params.fromDate);
    var toDate = parseInt(req.params.toDate);
    var latitude = req.params.latitude;
    var longitude = req.params.longitude;
    var list = db.collection('devices').find({
      $and:
      [{ "timestamp": { $gte: fromDate } },
      { "timestamp": { $lte: toDate } }]
    })
      .toArray(function (err, items) {
        res.send(items);
      });
    // res.send(list);
    // var result = [];
    //   angular.forEach(list, function(value, key) {
    //     console.log(key + ': ' + value);
    //     this.push(key + ': ' + value);
    //   }, result);
    // geolib.isPointInCircle()
  })

  app.post('/addData', function (req, res) {
    var data = req.body;

    //convert datetime to unix timestamp
    var date = new Date((data.date + data.time).replace(
      /^(\d{4})(\d\d)(\d\d)(\d\d)(\d\d)(\d\d)$/,
      '$4:$5:$6 $2/$3/$1'
    ));
    delete data.date;
    delete data.time;

    data.timestamp = convertDateTimeToTimeStamp(date)
    data.speed = parseInt(data.speed)
    db.collection('devices', function (err, collection) {
      collection.insert(data, { safe: true }, function (err, result) {
        if (err) {
          res.send({ 'error': 'An error has occurred' });
        } else {
          // console.log('Success: ' + JSON.stringify(result));
          res.send(result);
        }
      });
    });
  });

  app.listen(3000);
  console.log('Listening on port 3000...');


  function convertDateTimeToTimeStamp(dateTime) {
    return Math.floor(dateTime / 1000);
  }

});