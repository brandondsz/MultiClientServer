var https = require('https')
  , bodyParser = require('body-parser')
  , express = require('express')
  , fs = require('fs')
  , mongodb = require('mongodb')
  , geolib = require('geolib')
  , os = require('os-utils');

var app = express();
var options = {
  key: fs.readFileSync('server.key'),
  cert: fs.readFileSync('server.crt')
};
app.use(bodyParser.urlencoded({ extended: true }));
app.use(bodyParser.json());

app.use(function (req, res, next) {
  res.header("Access-Control-Allow-Origin", "*");
  res.header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");
  next();
});



https.createServer(options, app).listen(3000, function () {

  console.log('Listening on port 3000...');

});



var MongoClient = mongodb.MongoClient;
var url = 'mongodb://127.0.0.1:27017/transport_db';
var db;
MongoClient.connect(url, function (err, database) {
  if (err) console.log('Unable to connect to the mongoDB server. Error:', err);

  db = database;

  //Log CPU and Memory usage every 5 seconds
  var c = 0;
  var timeout = setInterval(function () {
    os.cpuUsage(function (v) {
      logCpuAndMemoryUsage(v, 1 - os.freememPercentage())
    });
    c++;
    //Don't run out of space 
    if (c > 1000000000) {
      clearInterval(timeout);
    }
  }, 5000);
});

function logCpuAndMemoryUsage(cpu, memory) {
  var sysHealth = {
    "timestamp": convertDateTimeToTimeStamp(new Date()),
    "cpu": cpu,
    "memory": memory,
  }

  db.collection('systemHealth', function (err, collection) {
    collection.insert(sysHealth, { safe: true }, function (err, result) {
      if (err) {
        console.log(err);
      }
    });
  });
}

//Report 6
app.get('/systemHealth/:fromDate/:toDate', function (req, res) {
  var fromDate = parseInt(req.params.fromDate);
  var toDate = parseInt(req.params.toDate);
  db.collection('systemHealth', function (err, collection) {
    collection.find({
      $and:
      [{ "timestamp": { $gte: fromDate } },
      { "timestamp": { $lte: toDate } }]
    },
      {
        _id: 0
      })
      .toArray(function (err, items) {
        items = items.map(function (value) {
          return {
            "DateTime": convertTimeStampToDateTime(value.timestamp),
            "CPU Usage": value.cpu * 100,
            "Memory Usage": value.memory * 100
          }
        });
        res.send(items);
      });
  });
});

//Report 1
app.get('/devices', function (req, res) {
  db.collection('devices', function (err, collection) {
    collection.distinct('device_id', {}, function (err, items) {
      res.send(items);
    });
  });
});

//Report 2
app.get('/geoLocation/:deviceID/:fromDate/:toDate', function (req, res) {
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

//Report 3
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
      res.send(items.map(function (value) { return value._id }));
    });
})

//Report 4
app.get('/geoDwell/:latitude/:longitude/:fromDate/:toDate', function (req, res) {
  var fromDate = parseInt(req.params.fromDate);
  var toDate = parseInt(req.params.toDate);
  var latitude = req.params.latitude;
  var longitude = req.params.longitude;

  getDevicesWithinTimeRange(db.collection('devices'), fromDate, toDate, function (result) {

    result = result.filter(function (value) {
      return geolib.isPointInCircle(
        { latitude: value.latitude, longitude: value.longitude },
        { latitude: latitude, longitude: longitude },
        10000
      );
    })
    //get only unique device_id's
    result = result.map(function (value) { return value.device_id }).filter(function (item, i, ar) { return ar.indexOf(item) === i; });
    res.send(result);
  })
})

//Report 5
app.get('/geoStationary/:fromDate/:toDate', function (req, res) {
  var fromDate = parseInt(req.params.fromDate);
  var toDate = parseInt(req.params.toDate);
  db.collection('devices').aggregate([
    {
      $match: {
        "speed": { $lte: 0 },
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
      $match: { count: { $gt: 12 } }
    }
  ])
    .toArray(function (err, items) {
      res.send(items);
    });
})


app.post('/addData', function (req, res) {
  var data = req.body;

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
        console.log(result);
        res.send(result);
      }
    });
  });
});

function getDevicesWithinTimeRange(collection, fromDate, toDate, callback) {
  collection.find({
    $and:
    [{ "timestamp": { $gte: fromDate } },
    { "timestamp": { $lte: toDate } }]
  })
    .toArray(function (err, items) {
      callback(items);
    });
}

function convertDateTimeToTimeStamp(dateTime) {
  return Math.floor(dateTime / 1000);
}
function convertTimeStampToDateTime(timeStamp) {
  return new Date(timeStamp * 1000);
}

