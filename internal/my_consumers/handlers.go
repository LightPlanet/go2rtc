package my_consumers

import (
	"net/http"
	"time"
)

func startSaveHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	deviceName := r.URL.Query().Get("name")
	outputDir := r.URL.Query().Get("output-dir")
	durStr := r.URL.Query().Get("duration")

	if deviceName == "" || outputDir == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if durStr == "" {
		durStr = "30s"
	}
	dur, err := time.ParseDuration(durStr)
	if err != nil {
		log.Error().Err(err).Caller().Send()
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if err = startSave(deviceName, outputDir, dur); err != nil {
		log.Error().Err(err).Caller().Send()
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func stopSaveHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	deviceName := r.URL.Query().Get("name")
	if deviceName == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if err := stopSave(deviceName); err != nil {
		log.Error().Err(err).Caller().Send()
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

func startWHIPStreamHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	deviceName := r.URL.Query().Get("name")
	if deviceName == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if err := startWHIP(deviceName, stunAddress, remoteAddress); err != nil {
		log.Error().Err(err).Caller().Send()
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

func stopWHIPStreamHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	devicePath := r.URL.Query().Get("video")
	if devicePath == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if err := stopWHIP(devicePath); err != nil {
		log.Error().Err(err).Caller().Send()
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}
