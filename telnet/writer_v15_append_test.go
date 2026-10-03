package telnet

import (
	"bytes"
	"math/rand"
	"strings"
	"testing"
)

func TestWriterV15AppendNormalizedBytes(t *testing.T) {
	for _, tc := range []struct{ name, input, want string }{
		{"empty", "", ""}, {"plain", "abc", "abc"},
		{"lf", "\n", "\r\n"}, {"crlf", "\r\n", "\r\n"},
		{"lone-cr", "\r", "\r"}, {"double-cr", "\r\r\n", "\r\r\n"},
		{"two-lf", "\n\n", "\r\n\r\n"}, {"mixed", "\r\n\n\r", "\r\n\r\n\r"},
		{"binary", "a\x00\xff\n\xfe\r", "a\x00\xff\r\n\xfe\r"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := writerV15Oracle(tc.input); got != tc.want {
				t.Fatalf("frozen oracle=%q, literal=%q", got, tc.want)
			}
			for _, spare := range []int{0, max(0, len(tc.want)-1), len(tc.want), len(tc.want) + 1} {
				batch := make([]byte, 2, 2+spare)
				copy(batch, "P\r")
				got := appendWriterNormalized(batch, tc.input)
				if string(got) != "P\r"+tc.want {
					t.Fatalf("spare=%d output=%q, want=%q", spare, got, "P\r"+tc.want)
				}
			}
		})
	}
}

func TestWriterV15AppendNormalizedOracle(t *testing.T) {
	random := rand.New(rand.NewSource(0x563135))
	for trial := range 4096 {
		input := make([]byte, random.Intn(257))
		for i := range input {
			switch random.Intn(4) {
			case 0:
				input[i] = '\r'
			case 1:
				input[i] = '\n'
			default:
				input[i] = byte(random.Intn(256))
			}
		}
		prefix := []byte{'p', '\r'}
		want := append(append([]byte(nil), prefix...), writerV15Oracle(string(input))...)
		spare := random.Intn(len(want) + 2)
		batch := make([]byte, len(prefix), len(prefix)+spare)
		copy(batch, prefix)
		got := appendWriterNormalized(batch, string(input))
		if !bytes.Equal(got, want) {
			t.Fatalf("trial=%d input=%q output=%q want=%q", trial, input, got, want)
		}
	}
}

func BenchmarkWriterV15AppendNormalized(b *testing.B) {
	for _, tc := range []struct{ name, line string }{
		{"ordinary-lf", strings.Repeat("x", 76) + "\n"},
		{"ordinary-crlf", strings.Repeat("x", 76) + "\r\n"},
		{"multiline-control", "one\r\n\n\rtwo\n\x00\xff\r\r\n"},
	} {
		for _, grow := range []bool{false, true} {
			name := tc.name + "/retained-batch"
			if grow {
				name = tc.name + "/growth"
			}
			b.Run(name, func(b *testing.B) {
				want := []byte(writerV15Oracle(tc.line))
				batch := make([]byte, 0, 16384)
				b.ReportAllocs()
				b.SetBytes(int64(len(want)))
				b.ResetTimer()
				for range b.N {
					if grow {
						batch = nil
					}
					batch = appendWriterNormalized(batch[:0], tc.line)
					if len(batch) != len(want) {
						b.Fatalf("record length=%d, want %d", len(batch), len(want))
					}
				}
				b.StopTimer()
				if !bytes.Equal(batch, want) {
					b.Fatalf("output=%q, want=%q", batch, want)
				}
				b.ReportMetric(float64(b.N), "appended-records")
			})
		}
	}
}
