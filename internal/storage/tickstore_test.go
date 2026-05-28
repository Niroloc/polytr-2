package storage

import (
	"bytes"
	"testing"

	"github.com/cicn/polytr/internal/types"
)

func TestTickRoundTrip(t *testing.T) {
	in := types.TickData{
		Timestamp: 1716489600123456789,
		Source:    types.SourceBinance,
		Type:      types.TickTrade,
		Price:     69123.45,
		Amount:    0.0123,
		Side:      types.SideSell,
	}
	var buf bytes.Buffer
	if err := WriteTick(&buf, in); err != nil {
		t.Fatal(err)
	}
	if buf.Len() != types.TickSize {
		t.Fatalf("encoded size = %d want %d", buf.Len(), types.TickSize)
	}
	var out types.TickData
	if err := ReadTick(&buf, &out); err != nil {
		t.Fatal(err)
	}
	if out != in {
		t.Fatalf("round-trip mismatch: %+v vs %+v", out, in)
	}
}

func TestTickEncodingSize(t *testing.T) {
	if types.TickSize != 27 {
		t.Fatalf("TickSize = %d want 27", types.TickSize)
	}
}
