package my_consumers

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/AlexxIT/go2rtc/internal/streams"
	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
)

type whipConsumer struct {
	core.Connection

	pc          *webrtc.PeerConnection
	videoTrack  *webrtc.TrackLocalStaticRTP
	resourceURL string
}

func (self *whipConsumer) AddTrack(media *core.Media, codec *core.Codec, track *core.Receiver) error {
	sender := core.NewSender(media, codec)
	sender.Handler = func(packet *rtp.Packet) {
		if self.videoTrack != nil {
			_ = self.videoTrack.WriteRTP(packet)
		}
	}
	sender.HandleRTP(track)
	self.Senders = append(self.Senders, sender)
	return nil
}

func (self *whipConsumer) Stop() error {
	if self.resourceURL != "" {
		req, err := http.NewRequest(http.MethodDelete, self.resourceURL, nil)
		if err == nil {
			client := &http.Client{Timeout: 5 * time.Second}
			if resp, err := client.Do(req); err == nil {
				_ = resp.Body.Close()
			}
		}
	}
	if self.pc != nil {
		_ = self.pc.Close()
	}
	return self.Connection.Stop()
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

func startWHIP(deviceName, stunAddr, remoteURL string) error {
	stream := streams.Get(deviceName)
	if stream == nil {
		return fmt.Errorf("no such stream: %s", deviceName)
	}

	whipMutex.Lock()
	defer whipMutex.Unlock()

	if _, ok := whipConsumers[deviceName]; ok {
		return fmt.Errorf("whip already active for: %s", deviceName)
	}

	config := webrtc.Configuration{}
	if stunAddr != "" {
		config.ICEServers = []webrtc.ICEServer{
			{URLs: []string{"stun:" + stunAddr}},
		}
	}

	pc, err := webrtc.NewPeerConnection(config)
	if err != nil {
		return fmt.Errorf("ошибка создания PeerConnection: %w", err)
	}

	pc.OnICEConnectionStateChange(func(state webrtc.ICEConnectionState) {
		log.Info().Str("device", deviceName).Str("state", state.String()).Msg("[WebRTC] ICE состояние изменилось")
	})

	// go2rtc has RTP already
	videoTrack, err := webrtc.NewTrackLocalStaticRTP(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeH264},
		"video",
		"pion-whip",
	)
	if err != nil {
		_ = pc.Close()
		return fmt.Errorf("ошибка создания видеотрека: %w", err)
	}

	if _, err = pc.AddTrack(videoTrack); err != nil {
		_ = pc.Close()
		return fmt.Errorf("ошибка добавления трека: %w", err)
	}

	offer, err := pc.CreateOffer(nil)
	if err != nil {
		_ = pc.Close()
		return fmt.Errorf("ошибка CreateOffer: %w", err)
	}

	gatherComplete := webrtc.GatheringCompletePromise(pc)
	if err = pc.SetLocalDescription(offer); err != nil {
		_ = pc.Close()
		return fmt.Errorf("ошибка SetLocalDescription: %w", err)
	}
	<-gatherComplete

	req, err := http.NewRequest(http.MethodPost, remoteURL, bytes.NewBufferString(pc.LocalDescription().SDP))
	if err != nil {
		_ = pc.Close()
		return err
	}
	req.Header.Set("Content-Type", "application/sdp")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		_ = pc.Close()
		return fmt.Errorf("ошибка отправки WHIP POST: %w", err)
	}
	defer resp.Body.Close()

	answerBytes, _ := io.ReadAll(resp.Body)
	log.Debug().Int("status", resp.StatusCode).Str("sdp", string(answerBytes)).Msg("[WHIP] Ответ сервера")

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		_ = pc.Close()
		return fmt.Errorf("удаленный сервер отклонил WHIP (%d): %s", resp.StatusCode, string(answerBytes))
	}

	if err = pc.SetRemoteDescription(webrtc.SessionDescription{
		Type: webrtc.SDPTypeAnswer,
		SDP:  string(answerBytes),
	}); err != nil {
		_ = pc.Close()
		return fmt.Errorf("ошибка SetRemoteDescription: %w", err)
	}

	var resourceURL string
	if loc := resp.Header.Get("Location"); loc != "" {
		if base, err := url.Parse(remoteURL); err == nil {
			if ref, err := url.Parse(loc); err == nil {
				resourceURL = base.ResolveReference(ref).String()
			}
		}
	}

	cons := &whipConsumer{
		Connection: core.Connection{
			ID:         core.NewID(),
			FormatName: "whip",
			Medias: []*core.Media{
				{
					Kind:      core.KindVideo,
					Direction: core.DirectionSendonly,
					Codecs:    []*core.Codec{{Name: core.CodecH264}},
				},
			},
		},
		pc:          pc,
		videoTrack:  videoTrack,
		resourceURL: resourceURL,
	}

	if err = stream.AddConsumer(cons); err != nil {
		_ = cons.Stop()
		return err
	}

	whipConsumers[deviceName] = cons
	return nil
}

func stopWHIP(deviceName string) error {
	stream := streams.Get(deviceName)
	if stream == nil {
		return fmt.Errorf("no such stream: %s", deviceName)
	}

	whipMutex.Lock()
	cons, ok := whipConsumers[deviceName]
	if ok {
		delete(whipConsumers, deviceName)
	}
	whipMutex.Unlock()

	if !ok {
		return fmt.Errorf("whip is not active for: %s", deviceName)
	}

	stream.RemoveConsumer(cons)
	return cons.Stop()
}
