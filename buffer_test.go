package esdb

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestLimitAmountRead(t *testing.T) {
	r := bytes.NewReader([]byte{
		1, 0, 0, 0,
		2, 0, 0, 0,
		3, 0, 0, 0,
		4, 0, 0, 0,
	})

	buf := newBuffer(r, 0, 8, 16)

	for _, expected := range []uint32{1, 2, 0, 0, 0} {
		if found := buf.PullUint32(); found != expected {
			t.Errorf("Wanted: %v, found: %v", expected, found)
		}
	}

	for _, expected := range []uint32{2, 1, 0, 0, 0} {
		if found := buf.PopUint32(); found != expected {
			t.Errorf("Wanted: %v, found: %v", expected, found)
		}
	}
}

func TestPageCaching(t *testing.T) {
	r := bytes.NewReader([]byte{
		1, 0, 0, 0,
		2, 0, 0, 0,
		3, 0, 0, 0,
		4, 0, 0, 0,
	})

	buf := newBuffer(r, 0, 16, 2)

	for _, expected := range []uint32{1, 2, 3, 4, 0} {
		if found := buf.PullUint32(); found != expected {
			t.Errorf("Wanted: %v, found: %v", expected, found)
		}
	}

	for _, expected := range []uint32{4, 3, 2, 1, 0} {
		if found := buf.PopUint32(); found != expected {
			t.Errorf("Wanted: %v, found: %v", expected, found)
		}
	}
}

func TestReset(t *testing.T) {
	r := bytes.NewReader([]byte{
		1, 0, 0, 0,
		2, 0, 0, 0,
		3, 0, 0, 0,
		4, 0, 0, 0,
	})

	buf := newBuffer(r, 0, 16, 2)

	for _, expected := range []uint32{1, 1, 1, 1, 1} {
		if found := buf.PullUint32(); found != expected {
			t.Errorf("Wanted: %v, found: %v", expected, found)
		}

		buf.Reset()
	}

	for _, expected := range []uint32{4, 4, 4, 4, 4} {
		if found := buf.PopUint32(); found != expected {
			t.Errorf("Wanted: %v, found: %v", expected, found)
		}

		buf.Reset()
	}
}

func TestMoveAndResetSmallBuffer(t *testing.T) {
	r := bytes.NewReader([]byte{
		1, 0, 0, 0,
		2, 0, 0, 0,
		3, 0, 0, 0,
		4, 0, 0, 0,
	})

	buf := newBuffer(r, 0, 16, 2)

	for _, expected := range []uint32{1, 2, 3, 4, 0} {
		if found := buf.PullUint32(); found != expected {
			t.Errorf("Wanted: %v, found: %v", expected, found)
		}
	}

	for _, expected := range []uint32{4, 3, 2, 1, 0} {
		if found := buf.PopUint32(); found != expected {
			t.Errorf("Wanted: %v, found: %v", expected, found)
		}
	}

	buf.Move(8, 2)

	for _, expected := range []uint32{3, 4, 0} {
		if found := buf.PullUint32(); found != expected {
			t.Errorf("Wanted: %v, found: %v", expected, found)
		}
	}

	for _, expected := range []uint32{4, 3, 0} {
		if found := buf.PopUint32(); found != expected {
			t.Errorf("Wanted: %v, found: %v", expected, found)
		}
	}

	buf.Move(4, 2)

	for _, expected := range []uint32{2, 3, 4, 0} {
		if found := buf.PullUint32(); found != expected {
			t.Errorf("Wanted: %v, found: %v", expected, found)
		}
	}

	for _, expected := range []uint32{4, 3, 2, 0} {
		if found := buf.PopUint32(); found != expected {
			t.Errorf("Wanted: %v, found: %v", expected, found)
		}
	}

	buf.Reset()

	for _, expected := range []uint32{1, 2, 3, 4, 0} {
		if found := buf.PullUint32(); found != expected {
			t.Errorf("Wanted: %v, found: %v", expected, found)
		}
	}

	for _, expected := range []uint32{4, 3, 2, 1, 0} {
		if found := buf.PopUint32(); found != expected {
			t.Errorf("Wanted: %v, found: %v", expected, found)
		}
	}
}

func TestMoveAndResetBigBuffer(t *testing.T) {
	r := bytes.NewReader([]byte{
		1, 0, 0, 0,
		2, 0, 0, 0,
		3, 0, 0, 0,
		4, 0, 0, 0,
	})

	buf := newBuffer(r, 0, 16, 32)

	for _, expected := range []uint32{1, 2, 3, 4, 0} {
		if found := buf.PullUint32(); found != expected {
			t.Errorf("Wanted: %v, found: %v", expected, found)
		}
	}

	for _, expected := range []uint32{4, 3, 2, 1, 0} {
		if found := buf.PopUint32(); found != expected {
			t.Errorf("Wanted: %v, found: %v", expected, found)
		}
	}

	buf.Move(8, 32)

	for _, expected := range []uint32{3, 4, 0} {
		if found := buf.PullUint32(); found != expected {
			t.Errorf("Wanted: %v, found: %v", expected, found)
		}
	}

	for _, expected := range []uint32{4, 3, 0} {
		if found := buf.PopUint32(); found != expected {
			t.Errorf("Wanted: %v, found: %v", expected, found)
		}
	}

	buf.Move(4, 32)

	for _, expected := range []uint32{2, 3, 4, 0} {
		if found := buf.PullUint32(); found != expected {
			t.Errorf("Wanted: %v, found: %v", expected, found)
		}
	}

	for _, expected := range []uint32{4, 3, 2, 0} {
		if found := buf.PopUint32(); found != expected {
			t.Errorf("Wanted: %v, found: %v", expected, found)
		}
	}

	buf.Reset()

	for _, expected := range []uint32{1, 2, 3, 4, 0} {
		if found := buf.PullUint32(); found != expected {
			t.Errorf("Wanted: %v, found: %v", expected, found)
		}
	}

	for _, expected := range []uint32{4, 3, 2, 1, 0} {
		if found := buf.PopUint32(); found != expected {
			t.Errorf("Wanted: %v, found: %v", expected, found)
		}
	}

	buf = newBuffer(r, 0, 12, 12)

	buf.Move(4, 12)

	for _, expected := range []uint32{2, 3, 0, 0} {
		if found := buf.PullUint32(); found != expected {
			t.Errorf("Wanted: %v, found: %v", expected, found)
		}
	}

	for _, expected := range []uint32{3, 2, 0, 0} {
		if found := buf.PopUint32(); found != expected {
			t.Errorf("Wanted: %v, found: %v", expected, found)
		}
	}
}

func TestReadInt32s(t *testing.T) {
	r := bytes.NewReader([]byte{
		1, 0, 0, 0,
		2, 0, 0, 0,
		3, 0, 0, 0,
		4, 0, 0, 0,
	})

	buf := newBuffer(r, 0, uint64(r.Len()), 16)

	for _, expected := range []uint32{1, 2, 3, 4, 0} {
		if found := buf.PullUint32(); found != expected {
			t.Errorf("Wanted: %v, found: %v", expected, found)
		}
	}

	for _, expected := range []uint32{4, 3, 2, 1, 0} {
		if found := buf.PopUint32(); found != expected {
			t.Errorf("Wanted: %v, found: %v", expected, found)
		}
	}
}

func TestReadInt64s(t *testing.T) {
	r := bytes.NewReader([]byte{
		1, 0, 0, 0, 0, 0, 0, 0,
		2, 0, 0, 0, 0, 0, 0, 0,
		3, 0, 0, 0, 0, 0, 0, 0,
		4, 0, 0, 0, 0, 0, 0, 0,
	})

	buf := newBuffer(r, 0, uint64(r.Len()), 16)

	for _, expected := range []uint64{1, 2, 3, 4, 0} {
		if found := buf.PullUint64(); found != expected {
			t.Errorf("Wanted: %v, found: %v", expected, found)
		}
	}

	for _, expected := range []uint64{4, 3, 2, 1, 0} {
		if found := buf.PopUint64(); found != expected {
			t.Errorf("Wanted: %v, found: %v", expected, found)
		}
	}
}

func TestReadUvarints(t *testing.T) {
	data := make([]byte, 100)
	num := 0
	num += binary.PutUvarint(data[num:], 1234567890)
	num += binary.PutUvarint(data[num:], 867)
	num += binary.PutUvarint(data[num:], 5)
	num += binary.PutUvarint(data[num:], 9001)

	buf := newBuffer(bytes.NewReader(data), 0, 100, 16)

	for _, expected := range []uint64{1234567890, 867, 5, 9001, 0} {
		if found := buf.PullUvarint(); found != expected {
			t.Errorf("Wanted: %v, found: %v", expected, found)
		}
	}
}
