package session

import "testing"

func TestOsc1337File(t *testing.T) {
	s := newOsc52()
	s.feed([]byte("\x1b]1337;File=name=dGVzdC50eHQ=;size=16:RklMRS1DT05URU5ULUFCQw==\x07"))
	select {
	case fo := <-s.files:
		if fo.Name != "test.txt" || string(fo.Data) != "FILE-CONTENT-ABC" {
			t.Fatalf("bad offer: %+v", fo)
		}
		t.Log("offer ok:", fo.Name, string(fo.Data))
	default:
		t.Fatal("no file offer emitted")
	}
}
