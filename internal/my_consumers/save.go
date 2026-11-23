package my_consumers

import (
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/AlexxIT/go2rtc/internal/streams"
	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/AlexxIT/go2rtc/pkg/h264"
	"github.com/AlexxIT/go2rtc/pkg/mpegts"
)

// SaveConsumer -------------------------------------------------------------------------

type saveConsumer struct {
	core.Connection

	OutputDir       string
	DeviceName      string
	SegmentDuration time.Duration

	Mutex        sync.Mutex
	WriteStarted time.Time
	File         *os.File
	Muxer        *mpegts.Muxer
	PID          uint16
}

func (self *saveConsumer) AddTrack(
	media *core.Media, codec *core.Codec, track *core.Receiver,
) error {
	sender := core.NewSender(media, codec)
	sender.Handler = func(packet *core.Packet) {
		self.WriteFrameToFile(packet)
	}
	sender.HandleRTP(track)
	self.Senders = append(self.Senders, sender)
	return nil
}

func (self *saveConsumer) Stop() error {
	self.Mutex.Lock()
	self.closeFileLocked()
	self.Mutex.Unlock()
	return self.Connection.Stop()
}

func (self *saveConsumer) WriteFrameToFile(packet *core.Packet) {
	var err error
	isKeyFrame := h264.IsKeyframe(packet.Payload)

	self.Mutex.Lock()
	defer self.Mutex.Unlock()

	// Try finish this and start new with KeyFrame
	if self.File != nil && time.Since(self.WriteStarted) >= self.SegmentDuration {
		if isKeyFrame {
			self.closeFileLocked()
			self.File = nil
		}
	}

	// Start new
	if self.File == nil {
		if !isKeyFrame {
			return // Start with KeyFrame only
		}

		filename := fmt.Sprintf("%s_%s.ts",
			self.DeviceName, time.Now().Format("2006-01-02_15-04-05"),
		)
		fullpath := filepath.Join(self.OutputDir, filename)
		var file *os.File
		file, err = os.Create(fullpath)
		if err != nil {
			log.Error().Err(err).Caller().Send()
			return
		}

		self.Muxer = mpegts.NewMuxer()
		self.PID = self.Muxer.AddTrack(mpegts.StreamTypeH264)

		if _, err = file.Write(self.Muxer.GetHeader()); err != nil {
			_ = file.Close()
			log.Error().Err(err).Caller().Send()
			return
		}

		self.File = file
		self.WriteStarted = time.Now()
	}

	// Write
	buffer := self.Muxer.GetPayload(self.PID, packet.Timestamp, packet.Payload)
	if len(buffer) > 0 {
		if _, err = self.File.Write(buffer); err != nil {
			log.Error().Err(err).Caller().Send()
		}
	}
}

func (self *saveConsumer) closeFileLocked() {
	if self.File != nil {
		if err := self.File.Close(); err != nil {
			log.Error().Err(err).Caller().Send()
		}
		go func() {
			if err := convertTsToMP4(self.File.Name()); err != nil {
				log.Error().Err(err).Caller().Send()
			}
		}()
		self.File = nil
	}
}

func convertTsToMP4(tsFile string) error {
	ext := filepath.Ext(tsFile)
	mp4File := strings.TrimSuffix(tsFile, ext) + ".mp4"

	cmd := exec.Command(
		"ffmpeg", "-hide_banner",
		"-y",
		"-fflags", "+genpts",
		"-i", tsFile,
		"-c:v", "copy",
		"-movflags", "+faststart",
		mp4File,
	)
	if _, err := cmd.CombinedOutput(); err != nil {
		_ = os.Remove(mp4File)
		return fmt.Errorf("Failed to convert: %s to %s: %s", tsFile, mp4File, err)
	}

	info, err := os.Stat(mp4File)
	if err != nil || info.Size() == 0 {
		return fmt.Errorf("No such mp4: %s. Keeping: %s: err: %s", mp4File, tsFile, err)
	}
	if err := os.Remove(tsFile); err != nil {
		return fmt.Errorf("Failed to remove: %s: %s", tsFile, err)
	}
	return nil
}

// HTTP handlers ------------------------------------------------------------------------

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

func startSave(deviceName, outputDir string, dur time.Duration) error {
	stream := streams.Get(deviceName)
	if stream == nil {
		return fmt.Errorf("No such stream: %s", deviceName)
	}

	consumersMutex.Lock()
	defer consumersMutex.Unlock()

	if _, ok := consumers[deviceName]; ok {
		return fmt.Errorf("Already saving: %s", deviceName)
	}
	cons := &saveConsumer{
		Connection: core.Connection{
			ID:         core.NewID(),
			FormatName: "save",
			Medias: []*core.Media{{
				Kind:      core.KindVideo,
				Direction: core.DirectionSendonly,
				Codecs:    []*core.Codec{{Name: core.CodecH264}},
			}},
		},
		OutputDir:       outputDir,
		DeviceName:      deviceName,
		SegmentDuration: dur,
	}

	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return err
	}
	if err := stream.AddConsumer(cons); err != nil {
		return err
	}
	consumers[deviceName] = cons
	return nil
}

func stopSave(deviceName string) error {
	stream := streams.Get(deviceName)
	if stream == nil {
		return fmt.Errorf("No such stream: %s", deviceName)
	}

	consumersMutex.Lock()
	cons, ok := consumers[deviceName]
	if ok {
		delete(consumers, deviceName)
	}
	consumersMutex.Unlock()

	if !ok {
		return fmt.Errorf("Not saving this video: %s", deviceName)
	}

	stream.RemoveConsumer(cons)
	return cons.Stop()
}
