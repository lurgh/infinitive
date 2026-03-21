console.log("loaded app.js");

var app = angular.module('thermostatApp', ['ngWebSocket']);
var maxOverrideDurationMins = 2184;

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

  var $wsUrl = "ws://" + $location.host() + ":" + $location.port() + "/api/ws";

  thermostatEvents.start($wsUrl, function (msg) {
    if (msg.source == "tstat") {
       $scope.tstat = msg.data;
    } else if (msg.source == "blower") {
       $scope.blower = msg.data;
    } else if (msg.source == "heatpump") {
       $scope.heatpump = msg.data;
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

  $scope.setZoneMode = function(zone, mode) {
    $http.put("/api/zone/" + zone + "/config", { "mode": mode }).then(function(response) {
      console.log("set zone " + zone + " mode to " + mode) ;
    });
  }

  $scope.setZoneOff = function(zone, zoneOff) {
    $http.put("/api/zone/" + zone + "/config", { "zoneOff": zoneOff }).then(function(response) {
      console.log("set zone " + zone + " zoneOff to " + zoneOff) ;
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

  function adjustOverrideDuration(current, delta) {
    var step = 15;

    // The +/- controls are only enabled while timed override is active.
    // Once duration reaches 0, the backend resumes schedule and the controls
    // should become disabled, so this function should never be entered with 0.
    if (current === 0) {
      console.error("adjustOverrideDuration called with current=0; the override controls should be disabled in this state");
    }

    if (delta > 0) {
      if (current === 0) {
        return 120;
      }
      var roundedUp = Math.ceil(current / step) * step;
      if (roundedUp === current) {
        return Math.min(current + step, maxOverrideDurationMins);
      }
      return Math.min(roundedUp, maxOverrideDurationMins);
    }

    if (delta < 0) {
      var roundedDown = Math.floor(current / step) * step;
      if (roundedDown === current) {
        return current >= step ? current - step : 0;
      }
      return roundedDown;
    }

    return current;
  }

  $scope.incOverrideDuration = function(zone,val) {
    var current = $scope.tstat.zones[zone-1].overrideDurationMins || 0;
    var target = adjustOverrideDuration(current, val);

    if (target === 0) {
      $http.put("/api/zone/" + zone + "/config", { "overrideDurationMins": 0 }).then(function(response) {
        console.log("cleared override duration zone " + zone) ;
      });
      return;
    }

    $http.put("/api/zone/" + zone + "/config", { "overrideDurationMins": target }).then(function(response) {
      console.log("set override duration zone " + zone + " to " + target) ;
    });
  }

/*
  $interval(function () {
     $scope.refreshState();
  }, 1000);
*/
});
