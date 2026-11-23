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

// WHIP Consumer ------------------------------------------------------------------------

type whipConsumer struct {
	core.Connection

	PeerConn    *webrtc.PeerConnection
	VideoTrack  *webrtc.TrackLocalStaticRTP
	ResourceURL string
}

func (self *whipConsumer) AddTrack(media *core.Media, codec *core.Codec, track *core.Receiver) error {
	sender := core.NewSender(media, codec)
	sender.Handler = func(packet *rtp.Packet) {
		if self.VideoTrack != nil {
			_ = self.VideoTrack.WriteRTP(packet)
		}
	}
	sender.HandleRTP(track)
	self.Senders = append(self.Senders, sender)
	return nil
}

func (self *whipConsumer) Stop() error {
	if self.ResourceURL != "" {
		req, err := http.NewRequest(http.MethodDelete, self.ResourceURL, nil)
		if err == nil {
			client := &http.Client{Timeout: 5 * time.Second}
			if resp, err := client.Do(req); err == nil {
				_ = resp.Body.Close()
			}
		}
	}
	if self.PeerConn != nil {
		_ = self.PeerConn.Close()
	}
	return self.Connection.Stop()
}

// Handlers -----------------------------------------------------------------------------

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

	PeerConn, err := webrtc.NewPeerConnection(config)
	if err != nil {
		return fmt.Errorf("ошибка создания PeerConnection: %w", err)
	}

	PeerConn.OnICEConnectionStateChange(func(state webrtc.ICEConnectionState) {
		log.Info().Str("device", deviceName).Str("state", state.String()).Msg("[WebRTC] ICE состояние изменилось")
	})

	// go2rtc has RTP already
	VideoTrack, err := webrtc.NewTrackLocalStaticRTP(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeH264},
		"video",
		"pion-whip",
	)
	if err != nil {
		_ = PeerConn.Close()
		return fmt.Errorf("ошибка создания видеотрека: %w", err)
	}

	if _, err = PeerConn.AddTrack(VideoTrack); err != nil {
		_ = PeerConn.Close()
		return fmt.Errorf("ошибка добавления трека: %w", err)
	}

	offer, err := PeerConn.CreateOffer(nil)
	if err != nil {
		_ = PeerConn.Close()
		return fmt.Errorf("ошибка CreateOffer: %w", err)
	}

	gatherComplete := webrtc.GatheringCompletePromise(PeerConn)
	if err = PeerConn.SetLocalDescription(offer); err != nil {
		_ = PeerConn.Close()
		return fmt.Errorf("ошибка SetLocalDescription: %w", err)
	}
	<-gatherComplete

	req, err := http.NewRequest(http.MethodPost, remoteURL, bytes.NewBufferString(PeerConn.LocalDescription().SDP))
	if err != nil {
		_ = PeerConn.Close()
		return err
	}
	req.Header.Set("Content-Type", "application/sdp")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		_ = PeerConn.Close()
		return fmt.Errorf("ошибка отправки WHIP POST: %w", err)
	}
	defer resp.Body.Close()

	answerBytes, _ := io.ReadAll(resp.Body)
	log.Debug().Int("status", resp.StatusCode).Str("sdp", string(answerBytes)).Msg("[WHIP] Ответ сервера")

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		_ = PeerConn.Close()
		return fmt.Errorf("удаленный сервер отклонил WHIP (%d): %s", resp.StatusCode, string(answerBytes))
	}

	if err = PeerConn.SetRemoteDescription(webrtc.SessionDescription{
		Type: webrtc.SDPTypeAnswer,
		SDP:  string(answerBytes),
	}); err != nil {
		_ = PeerConn.Close()
		return fmt.Errorf("ошибка SetRemoteDescription: %w", err)
	}

	var ResourceURL string
	if loc := resp.Header.Get("Location"); loc != "" {
		if base, err := url.Parse(remoteURL); err == nil {
			if ref, err := url.Parse(loc); err == nil {
				ResourceURL = base.ResolveReference(ref).String()
			}
		}
	}

	cons := &whipConsumer{
		Connection: core.Connection{
			ID:         core.NewID(),
			FormatName: "whip",
			Medias: []*core.Media{{
				Kind:      core.KindVideo,
				Direction: core.DirectionSendonly,
				Codecs:    []*core.Codec{{Name: core.CodecH264}},
			}},
		},
		PeerConn:    PeerConn,
		VideoTrack:  VideoTrack,
		ResourceURL: ResourceURL,
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
