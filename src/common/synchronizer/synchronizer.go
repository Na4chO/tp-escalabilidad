package synchronizer

import (
	"fmt"
	"log/slog"
	"sync"

	clientid "github.com/Na4chO/tp-escalabilidad/common/client_id"
	"github.com/Na4chO/tp-escalabilidad/common/external"
	"github.com/Na4chO/tp-escalabilidad/common/inner"
	"github.com/Na4chO/tp-escalabilidad/common/middleware"
)

const syncKey = "sync"

type SyncedCallback func(clientID clientid.ID)

type FinishedCallback func(clientID clientid.ID, total uint32)

type Synchronizer struct {
	instanceID string
	exchange   middleware.Router
	onSynced   SyncedCallback
	onFinished FinishedCallback

	mu          sync.Mutex
	processed   map[clientid.ID]uint32            // own processed count, sent in count messages
	sent        map[clientid.ID]uint32            // own results sent, sent in finished messages
	closing     map[clientid.ID]bool              // EOF known, counts are being exchanged
	totals      map[clientid.ID]uint32            // expected total from the EOF
	coordinator map[clientid.ID]bool              // sets instance to be coordinator for final EOF
	peerCounts  map[clientid.ID]map[string]uint32 // last count of each instance, compared to total
	peerSent    map[clientid.ID]map[string]uint32 // results sent by each instance, summed by coordinator
}

func NewSynchronizer(instanceID string, exchangeName string, connSettings middleware.ConnSettings, onSynced SyncedCallback, onFinished FinishedCallback) (*Synchronizer, error) {
	exchange, err := middleware.CreateExchangeMiddleware(exchangeName, []string{syncKey}, connSettings)
	if err != nil {
		return nil, fmt.Errorf("creating sync exchange: %w", err)
	}

	return &Synchronizer{
		instanceID:  instanceID,
		exchange:    exchange,
		onSynced:    onSynced,
		onFinished:  onFinished,
		processed:   map[clientid.ID]uint32{},
		sent:        map[clientid.ID]uint32{},
		closing:     map[clientid.ID]bool{},
		totals:      map[clientid.ID]uint32{},
		coordinator: map[clientid.ID]bool{},
		peerCounts:  map[clientid.ID]map[string]uint32{},
		peerSent:    map[clientid.ID]map[string]uint32{},
	}, nil
}

func (s *Synchronizer) StartConsuming() error {
	return s.exchange.StartConsuming(s.handleSync)
}

func (s *Synchronizer) StopConsuming() error {
	return s.exchange.StopConsuming()
}

func (s *Synchronizer) Close() error {
	return s.exchange.Close()
}

func (s *Synchronizer) Processed(clientID clientid.ID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.processed[clientID]++
	if !s.closing[clientID] {
		return nil
	}

	return s.sendCount(clientID)
}

func (s *Synchronizer) EndOfData(clientID clientid.ID, total uint32) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.closing[clientID] = true
	s.totals[clientID] = total
	s.coordinator[clientID] = true

	return s.sendCount(clientID)
}

func (s *Synchronizer) Sent(clientID clientid.ID) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.sent[clientID]++
}

func (s *Synchronizer) Finished(clientID clientid.ID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.send(clientID, inner.PeerCount{Instance: s.instanceID, Count: s.sent[clientID], Finished: true})
	if !s.coordinator[clientID] {
		s.cleanup(clientID)
	}
	return err
}

func (s *Synchronizer) handleSync(msg middleware.Message, ack func(), nack func()) {
	packet, err := external.DecodePacket[inner.PeerCount]([]byte(msg.Body))
	if err != nil {
		slog.Error("Discarding sync message", "err", err)
		nack()
		return
	}

	s.mu.Lock()
	if packet.Payload.Finished {
		s.handleFinished(packet.ClientID, packet.Payload)
	} else {
		s.handleCount(packet.ClientID, packet.Total, packet.Payload)
	}
	s.mu.Unlock()
	ack()
}

func (s *Synchronizer) handleCount(clientID clientid.ID, total uint32, peer inner.PeerCount) {
	if !s.closing[clientID] {
		s.closing[clientID] = true
		s.totals[clientID] = total
		if s.processed[clientID] > 0 {
			if err := s.sendCount(clientID); err != nil {
				slog.Error("While sending count", "client", clientID, "err", err)
			}
		}
	}
	peerCount(s.peerCounts, clientID)[peer.Instance] = peer.Count

	if !s.isConsumed(clientID) {
		return
	}

	go s.onSynced(clientID)
}

func (s *Synchronizer) handleFinished(clientID clientid.ID, peer inner.PeerCount) {
	if !s.coordinator[clientID] {
		return
	}
	peerCount(s.peerSent, clientID)[peer.Instance] = peer.Count
	s.checkFinished(clientID)
}

func (s *Synchronizer) checkFinished(clientID clientid.ID) {
	if !s.coordinator[clientID] {
		return
	}

	var total uint32
	for instance := range s.peerCounts[clientID] {
		sent, ok := s.peerSent[clientID][instance]
		if !ok {
			return
		}
		total += sent
	}

	s.cleanup(clientID)
	go s.onFinished(clientID, total)
}

func (s *Synchronizer) isConsumed(clientID clientid.ID) bool {
	var consumed uint32
	for _, count := range s.peerCounts[clientID] {
		consumed += count
	}
	return consumed == s.totals[clientID]
}

func (s *Synchronizer) sendCount(clientID clientid.ID) error {
	return s.send(clientID, inner.PeerCount{Instance: s.instanceID, Count: s.processed[clientID]})
}

func (s *Synchronizer) send(clientID clientid.ID, payload inner.PeerCount) error {
	data, err := external.Packet[inner.PeerCount]{
		ClientID: clientID,
		Total:    s.totals[clientID],
		Payload:  payload,
	}.Encode()
	if err != nil {
		return err
	}
	return s.exchange.Send(middleware.Message{Body: string(data)})
}

func (s *Synchronizer) cleanup(clientID clientid.ID) {
	delete(s.processed, clientID)
	delete(s.sent, clientID)
	delete(s.closing, clientID)
	delete(s.totals, clientID)
	delete(s.coordinator, clientID)
	delete(s.peerCounts, clientID)
	delete(s.peerSent, clientID)
}

func peerCount(counts map[clientid.ID]map[string]uint32, clientID clientid.ID) map[string]uint32 {
	if counts[clientID] == nil {
		counts[clientID] = map[string]uint32{}
	}
	return counts[clientID]
}
