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

	log zerolog.Logger
)

func Init() {
	log = app.GetLogger("remote")

	consumers = make(map[string]core.Consumer)

	api.HandleFunc("api/start-save", startSaveHandler)
	api.HandleFunc("api/stop-save", stopSaveHandler)
}
