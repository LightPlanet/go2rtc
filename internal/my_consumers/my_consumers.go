package my_consumers

import (
	"sync"

	"github.com/AlexxIT/go2rtc/internal/api"
	"github.com/AlexxIT/go2rtc/internal/app"
	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/rs/zerolog"
)

var (
	consumersMutex sync.Mutex
	consumers      map[string]core.Consumer

	// TODO: Extract fron .yaml:
	stunAddress   string
	remoteAddress string
	whipMutex     sync.Mutex
	whipConsumers map[string]*whipConsumer

	log zerolog.Logger
)

func Init() {
	log = app.GetLogger("remote")

	consumers = make(map[string]core.Consumer)
	whipConsumers = make(map[string]*whipConsumer)

	api.HandleFunc("api/start-save", startSaveHandler)
	api.HandleFunc("api/stop-save", stopSaveHandler)
	api.HandleFunc("api/start-whip", startWHIPStreamHandler)
	api.HandleFunc("api/stop-whip", stopWHIPStreamHandler)
}
