package common

import (
	"slices"
	"sync"
	"testing"

	clientid "github.com/Na4chO/tp-escalabilidad/common/client_id"
	"github.com/Na4chO/tp-escalabilidad/common/external"
)

func TestBatcherKeepsOrderAndSizes(t *testing.T) {
	for _, size := range []int{1, 3, 7, 100} {
		sent := map[clientid.ID]map[string][]external.Packet[int]{}
		batcher := NewBatcher(size, func(key string, packet external.Packet[int]) error {
			data, err := packet.Encode()
			if err != nil {
				return err
			}
			decoded, err := external.DecodePacket[int](data)
			if err != nil {
				return err
			}
			if sent[decoded.ClientID] == nil {
				sent[decoded.ClientID] = map[string][]external.Packet[int]{}
			}
			sent[decoded.ClientID][key] = append(sent[decoded.ClientID][key], decoded)
			return nil
		})

		clients := []clientid.ID{{Server: "1", Seq: 1}, {Server: "1", Seq: 2}}
		keys := []string{"a", "b"}
		const items = 50
		for i := range items {
			for _, client := range clients {
				for _, key := range keys {
					if err := batcher.Add(client, key, i); err != nil {
						t.Fatal(err)
					}
				}
			}
		}
		for _, client := range clients {
			if err := batcher.Flush(client); err != nil {
				t.Fatal(err)
			}
		}

		for _, client := range clients {
			for _, key := range keys {
				var got []int
				packets := sent[client][key]
				for i, packet := range packets {
					if packet.ClientID != client || int(packet.Count) != len(packet.Items) {
						t.Fatalf("size %d: bad packet %+v", size, packet)
					}
					if i < len(packets)-1 && len(packet.Items) != size {
						t.Fatalf("size %d: batch %d has %d items", size, i, len(packet.Items))
					}
					got = append(got, packet.Items...)
				}
				want := make([]int, items)
				for i := range want {
					want[i] = i
				}
				if !slices.Equal(got, want) {
					t.Fatalf("size %d client %v key %s: got %v", size, client, key, got)
				}
			}
		}
	}
}

func TestBatcherFlushWithNothingPending(t *testing.T) {
	batcher := NewBatcher(5, func(string, external.Packet[int]) error {
		t.Fatal("nothing should be sent")
		return nil
	})
	if err := batcher.Flush(clientid.ID{Server: "1", Seq: 1}); err != nil {
		t.Fatal(err)
	}
}

func TestDecodeRejectsWrongCount(t *testing.T) {
	data, _ := external.Packet[int]{Count: 3, Items: []int{1, 2}}.Encode()
	if _, err := external.DecodePacket[int](data); err == nil {
		t.Fatal("expected an error when the header count doesn't match the items")
	}
}

func TestBatcherConcurrentClientsKeepTheirOrder(t *testing.T) {
	var mu sync.Mutex
	got := map[clientid.ID][]int{}
	batcher := NewBatcher(7, func(_ string, packet external.Packet[int]) error {
		mu.Lock()
		defer mu.Unlock()
		got[packet.ClientID] = append(got[packet.ClientID], packet.Items...)
		return nil
	})

	const clients, items = 8, 1000
	var wg sync.WaitGroup
	for c := range clients {
		wg.Add(1)
		go func(client clientid.ID) {
			defer wg.Done()
			for i := range items {
				if err := batcher.Add(client, "", i); err != nil {
					t.Error(err)
				}
			}
			if err := batcher.Flush(client); err != nil {
				t.Error(err)
			}
		}(clientid.ID{Server: "1", Seq: uint32(c)})
	}
	wg.Wait()

	for c := range clients {
		received := got[clientid.ID{Server: "1", Seq: uint32(c)}]
		if len(received) != items {
			t.Fatalf("client %d got %d items", c, len(received))
		}
		for i, item := range received {
			if item != i {
				t.Fatalf("client %d: item %d is %d", c, i, item)
			}
		}
	}
}
