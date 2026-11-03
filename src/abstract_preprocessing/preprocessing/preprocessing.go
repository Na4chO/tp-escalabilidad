package preprocessing

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Na4chO/tp-escalabilidad/abstract_preprocessing/config"
	"github.com/Na4chO/tp-escalabilidad/common"
	clientid "github.com/Na4chO/tp-escalabilidad/common/client_id"
	"github.com/Na4chO/tp-escalabilidad/common/external"
	"github.com/Na4chO/tp-escalabilidad/common/inner"
	"github.com/Na4chO/tp-escalabilidad/common/middleware"
)

type clientState struct {
	receivedCount uint32
	expectedTotal uint32
	sentUntil2020 uint32
	hasEOF        bool
}

type Preprocessing struct {
	inputQueue *common.PacketQueue[inner.Abstract]
	wordsQueue *common.PacketQueue[inner.AbstractWords]
	until2020  middleware.Router
	from2021   *common.PacketQueue[inner.AbstractWordCount]
	clients    map[clientid.ID]*clientState
	running    atomic.Bool
}

func NewPreprocessing(cfg *config.Config) (*Preprocessing, error) {
	connSettings := middleware.ConnSettings{Hostname: cfg.MomHost, Port: cfg.MomPort}

	inputQueue, err := common.NewPacketQueue[inner.Abstract](common.QueueAbstract, connSettings)
	if err != nil {
		return nil, err
	}

	wordsQueue, err := common.NewPacketQueue[inner.AbstractWords](common.QueueAbstractWords, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	until2020, err := middleware.CreateExchangeMiddleware(common.ExchangeAbstractsUntil2020, nil, connSettings)
	if err != nil {
		inputQueue.Close()
		wordsQueue.Close()
		return nil, err
	}

	from2021, err := common.NewPacketQueue[inner.AbstractWordCount](common.QueueAbstractsFrom2021, connSettings)
	if err != nil {
		inputQueue.Close()
		wordsQueue.Close()
		until2020.Close()
		return nil, err
	}

	p := &Preprocessing{
		inputQueue: inputQueue,
		wordsQueue: wordsQueue,
		until2020:  until2020,
		from2021:   from2021,
		clients:    make(map[clientid.ID]*clientState),
	}
	p.running.Store(true)
	return p, nil
}

func (p *Preprocessing) Run() error {
	go p.handleSignals()

	err := errors.Join(
		p.inputQueue.StartConsuming(func(packet external.Packet[inner.Abstract], ack, nack func()) {
			p.handlePacket(packet, ack, nack)
		}),
		p.closeMiddlewares(),
	)

	if p.running.Load() {
		return err
	}

	return nil
}

func (p *Preprocessing) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")
	p.running.Store(false)
	_ = p.inputQueue.StopConsuming()
}

func (p *Preprocessing) closeMiddlewares() error {
	return errors.Join(
		p.inputQueue.StopConsuming(),
		p.inputQueue.Close(),
		p.wordsQueue.Close(),
		p.until2020.Close(),
		p.from2021.Close(),
	)
}

func (p *Preprocessing) handlePacket(packet external.Packet[inner.Abstract], ack func(), nack func()) {
	var err error
	if packet.IsEof {
		err = p.handleEOF(packet)
	} else {
		err = p.handleAbstract(packet)
	}

	if err != nil {
		slog.Error("While handling abstract packet", "client", packet.ClientID, "isEOF", packet.IsEof, "err", err)
		nack()
		return
	}
	ack()
}

func (p *Preprocessing) handleAbstract(packet external.Packet[inner.Abstract]) error {
	state := p.clientStateFor(packet.ClientID)
	words := extractWords(packet.Payload.Abstract)

	if err := p.wordsQueue.SendData(packet.ClientID, inner.AbstractWords{
		Words:      words,
		InLanguage: packet.Payload.InLanguage,
	}); err != nil {
		return fmt.Errorf("sending abstract words: %w", err)
	}

	if err := p.handleWordCount(packet, state, uint32(len(words))); err != nil {
		return err
	}

	state.receivedCount++
	return p.checkAndFinalize(packet.ClientID, state)
}

func (p *Preprocessing) handleWordCount(packet external.Packet[inner.Abstract], state *clientState, count uint32) error {
	date, ok := parseDateModified(packet.Payload.DateModified)
	if !ok {
		slog.Warn("Invalid date modified", "client", packet.ClientID, "date", packet.Payload.DateModified)
		return nil
	}

	wordCount := inner.AbstractWordCount{
		Url:   packet.Payload.Url,
		Count: count,
	}

	if date.Year() <= 2020 {
		if err := p.sendUntil2020(external.Packet[inner.AbstractWordCount]{
			ClientID: packet.ClientID,
			Payload:  wordCount,
		}); err != nil {
			return fmt.Errorf("sending word count to abstracts until 2020: %w", err)
		}
		state.sentUntil2020++
		return nil
	}

	if err := p.from2021.SendData(packet.ClientID, wordCount); err != nil {
		return fmt.Errorf("sending word count to abstracts from 2021: %w", err)
	}
	return nil
}

func (p *Preprocessing) handleEOF(packet external.Packet[inner.Abstract]) error {
	state := p.clientStateFor(packet.ClientID)
	state.hasEOF = true
	state.expectedTotal = packet.Total
	return p.checkAndFinalize(packet.ClientID, state)
}

func (p *Preprocessing) checkAndFinalize(clientID clientid.ID, state *clientState) error {
	if !shouldFinalize(state) {
		return nil
	}

	if err := p.wordsQueue.SendEOF(clientID); err != nil {
		return fmt.Errorf("sending abstract words EOF: %w", err)
	}

	if err := p.from2021.SendEOF(clientID); err != nil {
		return fmt.Errorf("sending abstracts from 2021 EOF: %w", err)
	}

	if err := p.sendUntil2020(external.Packet[inner.AbstractWordCount]{
		ClientID: clientID,
		IsEof:    true,
		Total:    state.sentUntil2020,
	}); err != nil {
		return fmt.Errorf("sending abstracts until 2020 EOF: %w", err)
	}

	delete(p.clients, clientID)
	return nil
}

func (p *Preprocessing) sendUntil2020(packet external.Packet[inner.AbstractWordCount]) error {
	data, err := packet.Encode()
	if err != nil {
		return err
	}
	return p.until2020.SendTo(
		fmt.Sprintf("%s-%d", packet.ClientID.Server, packet.ClientID.Seq),
		middleware.Message{Body: string(data)},
	)
}

func shouldFinalize(state *clientState) bool {
	return state.hasEOF && state.receivedCount >= state.expectedTotal
}

func (p *Preprocessing) clientStateFor(clientID clientid.ID) *clientState {
	state, ok := p.clients[clientID]
	if !ok {
		state = &clientState{}
		p.clients[clientID] = state
	}
	return state
}

func parseDateModified(value string) (time.Time, bool) {
	ms, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return time.Time{}, false
	}
	return time.UnixMilli(ms).UTC(), true
}
