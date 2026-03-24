console.log("loaded app.js");

var app = angular.module('thermostatApp', ['ngWebSocket']);

/*
app.factory('thermostatEvents', function($websocket) {
      // Open a WebSocket connection
      var dataStream = $websocket("ws://192.168.1.37:8080/api/ws");

      var collection = [];

      dataStream.onMessage(function(message) {
        console.log("got message!");
	console.log(message);
        collection.push(JSON.parse(message.data));
      });

      var methods = {
        collection: collection,
        get: function() {
          dataStream.send(JSON.stringify({ action: 'get' }));
        }
      };

      return methods;
});
*/

app.factory('thermostatEvents', function ($websocket) {
        return {
            start: function (url, callback) {
                var s = $websocket(url, null, {reconnectIfNotNormalClose: true});
                s.onMessage(function(message) {
                  console.log("got message!");
          	  console.log(message);
                  callback(JSON.parse(message.data));
                });
            }
        }
    }
);

// Define the `PhoneListController` controller on the `phonecatApp` module
app.controller('thermostatController', function($scope, $http, $interval, $location, thermostatEvents) {
  $scope.tstat = {};
  $scope.blower = {};
  $scope.heatpump = {};
  $scope.vacation = {};

  var $wsUrl = "ws://" + $location.host() + ":" + $location.port() + "/api/ws";

  thermostatEvents.start($wsUrl, function (msg) {
    if (msg.source == "tstat") {
       $scope.tstat = msg.data;
    } else if (msg.source == "blower") {
       $scope.blower = msg.data;
    } else if (msg.source == "heatpump") {
       $scope.heatpump = msg.data;
    } else if (msg.source == "vacation") {
       $scope.vacation = msg.data;
    }
  });

  // $scope.events = thermostatEvents;

  $scope.refreshState = function () {
     $http.get("/api/zone/0/config").then(function(response) {
      $scope.tstat = response.data;
    });
  };

  $scope.setFanSpeed = function(zone,speed) {
    $http.put("/api/zone/" + zone + "/config", { "fanMode": speed }).then(function(response) {
      console.log("set fan speed zone " + zone + " to " + speed) ;
    });
  }

  $scope.setMode = function(mode) {
    $http.put("/api/zone/1/config", { "mode": mode }).then(function(response) {
      console.log("set mode to " + mode) ;
    });
  }

  $scope.setHold = function(zone,hold) {
    $http.put("/api/zone/" + zone + "/config", { "hold": hold }).then(function(response) {
      console.log("set hold zone " + zone + " to " + hold) ;
    });
  }

  $scope.incCoolSetpoint = function(zone,val) {
    var temp = $scope.tstat.zones[zone-1].coolSetpoint + val;
    $http.put("/api/zone/" + zone + "/config", { "coolSetpoint": temp }).then(function(response) {
      console.log("set cool setpoint zone " + zone + " to " + temp) ;
    });
  }

  $scope.incHeatSetpoint = function(zone,val) {
    var temp = $scope.tstat.zones[zone-1].heatSetpoint + val;
    $http.put("/api/zone/" + zone + "/config", { "heatSetpoint": temp }).then(function(response) {
      console.log("set heat setpoint zone " + zone + " to " + temp) ;
    });
  }

  $scope.incVacationMinHumidity = function(val) {
    var hum = ($scope.vacation.minHumidity || 0) + val;
    $http.put("/api/zone/1/vacation", { "minHumidity": hum }).then(function(response) {
      console.log("set vacation min humidity to " + hum) ;
    });
  }

  $scope.incVacationMaxHumidity = function(val) {
    var hum = ($scope.vacation.maxHumidity || 0) + val;
    $http.put("/api/zone/1/vacation", { "maxHumidity": hum }).then(function(response) {
      console.log("set vacation max humidity to " + hum) ;
    });
  }

/*
  $interval(function () {
     $scope.refreshState();
  }, 1000);
*/
});
