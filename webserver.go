package main

import (
	"encoding/json"
	"encoding/hex"
	"errors"
	"net/http"
	"os"
	"regexp"
	"strconv"

	"golang.org/x/net/websocket"

	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"
)

func handleErrors(c *gin.Context) {
	c.Next()

	if len(c.Errors) > 0 {
		c.JSON(-1, c.Errors) // -1 == not override the current error code
	}
}

func webserver(port int) {
	r := gin.Default()
	r.Use(handleErrors) // attach error handling middleware

	api := r.Group("/api")

	api.GET("/tstat/settings", func(c *gin.Context) {
		tss, ok := getTstatSettings()
		if ok {
			c.JSON(200, tss)
		}
	})

	api.GET("/zones/config", func(c *gin.Context) {
		cfgZ0, ok := getZonesConfig()
		if ok {
			c.JSON(200, cfgZ0)
		}
	})

	api.GET("/zone/:zn/config", func(c *gin.Context) {
		zn, err := strconv.Atoi(c.Param("zn"))

		if  err != nil {
		} else if zn == 0 {
			cfgZ0, ok := getZonesConfig()
			if ok {
				c.JSON(200, cfgZ0)
			}
		} else if zn > 0 && zn <= 8 {
			cfgZN, ok := getZNConfig(zn - 1)
			if ok {
				c.JSON(200, cfgZN)
			}
		}
	})

	api.GET("/airhandler", func(c *gin.Context) {
		ah, ok := getAirHandler()
		if ok {
			c.JSON(200, ah)
		}
	})

	api.GET("/zone/1/airhandler", func(c *gin.Context) {
		ah, ok := getAirHandler()
		if ok {
			c.JSON(200, ah)
		}
	})

	api.GET("/heatpump", func(c *gin.Context) {
		hp, ok := getHeatPump()
		if ok {
			c.JSON(200, hp)
		}
	})

	api.GET("/zone/1/heatpump", func(c *gin.Context) {
		hp, ok := getHeatPump()
		if ok {
			c.JSON(200, hp)
		}
	})

	api.GET("/zone/1/vacation", func(c *gin.Context) {
		vac := TStatVacationParams{}
		ok := infinity.ReadTable(devTSTAT, &vac)
		if ok {
			c.JSON(200, vac.toAPI())
		}
	})

	api.PUT("/zone/1/vacation", func(c *gin.Context) {
		var args APIVacationConfig

		if c.Bind(&args) != nil {
			log.Printf("bind failed")
			return
		}

		params := TStatVacationParams{}
		flags := params.fromAPI(&args)

		infinity.WriteTable(devTSTAT, params, flags)

	})

	api.PUT("/zone/:zn/config", func(c *gin.Context) {
		var args TStatZoneConfig
		reqArgs := map[string]json.RawMessage{}
		zn, err := strconv.Atoi(c.Param("zn"));

		if reqBody, berr := c.GetRawData(); berr != nil || json.Unmarshal(reqBody, &args) != nil || json.Unmarshal(reqBody, &reqArgs) != nil {
			log.Printf("bind failed")
		} else if err != nil || zn < 0 || zn > 8 {
			log.Printf("invalid zone numner")
		} else if _, bad := reqArgs["overrideActive"]; bad {
			c.AbortWithError(400, errors.New("overrideActive is read-only"))
			return
		} else if zn == 0 {
			if len(args.Mode) > 0 {
				_ = putConfig("0", "mode", args.Mode)
			}
		} else {
			params := TStatZoneParams{}
			flags := uint16(0)
			zi := zn - 1
			_, overrideReq := reqArgs["overrideDurationMins"]
			_, zoneOffReq := reqArgs["zoneOff"]
			zoneOff := false

			if zoneOffReq {
				if err := json.Unmarshal(reqArgs["zoneOff"], &zoneOff); err != nil {
					log.Printf("invalid zoneOff value")
					return
				}
			}

			if len(args.FanMode) > 0 {
				mode, ok := stringFanModeToRaw(args.FanMode)

				if !ok {
					log.Printf("invalid fan mode name")
					return
				}

				params.ZFanMode[zi] = mode
				flags |= 0x01
			}

			if args.Hold != nil {
				if *args.Hold {
					params.ZoneHold = 0x01 << zi
				}
				flags |= 0x02
			}

			zoneCfg, haveZoneCfg := getZNConfig(zi)

			if args.HeatSetpoint > 0 && !overrideReq {
				if haveZoneCfg && zn > 1 && zoneCfg.ZoneOff {
					_ = putConfig(strconv.Itoa(zn), "heatSetpoint", strconv.Itoa(int(args.HeatSetpoint)))
				} else {
					params.ZHeatSetpoint[zi] = args.HeatSetpoint
					flags |= 0x04
				}
			}

			if args.CoolSetpoint > 0 && !overrideReq {
				if haveZoneCfg && zn > 1 && zoneCfg.ZoneOff {
					_ = putConfig(strconv.Itoa(zn), "coolSetpoint", strconv.Itoa(int(args.CoolSetpoint)))
				} else {
					params.ZCoolSetpoint[zi] = args.CoolSetpoint
					flags |= 0x08
				}
			}

			if overrideReq {
				cur, ok := getZNConfig(zi)
				if !ok {
					log.Printf("unable to read zone config for override duration write")
					return
				}
				heat := cur.HeatSetpoint
				cool := cur.CoolSetpoint
				if args.HeatSetpoint > 0 {
					heat = args.HeatSetpoint
				}
				if args.CoolSetpoint > 0 {
					cool = args.CoolSetpoint
				}
				if !writeZoneOverrideDuration(zn, args.OvrdDurationMins, heat, cool) {
					log.Printf("failed to write zone override duration")
					return
				}
			}

			if flags != 0 {
				log.Printf("submitting direct zone write zone=%d flags=0x%x", zn, flags)
				infinity.WriteTableZ(devTSTAT, params, uint8(zi), flags)
			}

			if zoneOffReq {
				_ = putConfig(strconv.Itoa(zn), "zoneOff", strconv.FormatBool(zoneOff))
			} else if len(args.Mode) > 0 {
				_ = putConfig(strconv.Itoa(zn), "mode", args.Mode)
			}
		}
	})

	api.GET("/raw/:device/:table", func(c *gin.Context) {
		matched, _ := regexp.MatchString("^[a-f0-9]{4}$", c.Param("device"))
		if !matched {
			c.AbortWithError(400, errors.New("name must be a 4 character hex string"))
			return
		}
		matched, _ = regexp.MatchString("^[a-f0-9]{6}$", c.Param("table"))
		if !matched {
			c.AbortWithError(400, errors.New("table must be a 6 character hex string"))
			return
		}

		d, _ := strconv.ParseUint(c.Param("device"), 16, 16)
		a, _ := hex.DecodeString(c.Param("table"))
		var addr InfinityTableAddr
		copy(addr[:], a[0:3])
		raw := InfinityProtocolRawRequest{&[]byte{}}

		success := infinity.Read(uint16(d), addr, raw)

		if success {
			c.JSON(200, gin.H{"response": hex.EncodeToString(*raw.data)})
		} else {
			c.AbortWithError(504, errors.New("timed out waiting for response"))
		}
	})

	api.GET("/ws", func(c *gin.Context) {
		h := websocket.Handler(attachListener)
		h.ServeHTTP(c.Writer, c.Request)
	})

	if fi, err := os.Stat("assets"); err == nil && fi.IsDir() {
		r.Static("/ui", "./assets")
	} else {
		r.StaticFS("/ui", assetFS())
	}

	r.GET("/", func(c *gin.Context) {
		c.Redirect(http.StatusMovedPermanently, "ui")
	})

	r.Run(":" + strconv.Itoa(port)) // listen and server on 0.0.0.0:8080
}

func attachListener(ws *websocket.Conn) {
	listener := &EventListener{make(chan []byte, 32)}

	defer func() {
		Dispatcher.deregister <- listener
		log.Printf("closing websocket")
		err := ws.Close()
		if err != nil {
			log.Println("error on ws close:", err.Error())
		}
	}()

	Dispatcher.register <- listener

	// log.Printf("dumping cached data")
	for source, data := range wsCache.cacheMap {
		ws.Write(serializeEvent(source, data))
	}

	// wait for events
	for {
		select {
		case message, ok := <-listener.ch:
			if !ok {
				log.Printf("read from listener.ch was not okay")
				return
			} else {
				_, err := ws.Write(message)
				if err != nil {
					log.Printf("error on websocket write: %s", err.Error())
					return
				}
			}
		}
	}
}
