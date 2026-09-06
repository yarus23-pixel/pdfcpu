/*
Copyright 2026 The pdfcpu Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package filter_test

import (
	"bytes"
	"compress/zlib"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/pdfcpu/pdfcpu/pkg/filter"
)

var flateEncodeFixture = []byte("a fixed small PDF stream fixture repeated for compressor reuse")

func newFlateFilter(t testing.TB) filter.Filter {
	t.Helper()

	f, err := filter.NewFilter(filter.Flate, nil)
	if err != nil {
		t.Fatalf("create flate filter: %v", err)
	}
	return f
}

func decodeFlate(t testing.TB, f filter.Filter, encoded io.Reader, want []byte) {
	t.Helper()

	compressed, err := io.ReadAll(encoded)
	if err != nil {
		t.Fatalf("read encoded stream: %v", err)
	}
	decoded, err := f.Decode(bytes.NewReader(compressed))
	if err != nil {
		t.Fatalf("create decoder: %v", err)
	}
	got, err := io.ReadAll(decoded)
	if err != nil {
		t.Fatalf("read decoded stream: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("decoded stream differs: got %q, want %q", got, want)
	}
}

func TestFlateEncodePreservesEarlierStreams(t *testing.T) {
	f := newFlateFilter(t)
	inputs := make([][]byte, 32)
	encoded := make([]io.Reader, len(inputs))
	for i := range inputs {
		inputs[i] = []byte(strings.Repeat(string(rune('A'+i)), 128))
		var err error
		encoded[i], err = f.Encode(bytes.NewReader(inputs[i]))
		if err != nil {
			t.Fatalf("encode stream %d: %v", i, err)
		}
	}

	for i := range encoded {
		decodeFlate(t, f, encoded[i], inputs[i])
	}
}

func TestFlateEncodeConcurrentDistinctStreams(t *testing.T) {
	f := newFlateFilter(t)
	inputs := make([][]byte, 16)
	for i := range inputs {
		inputs[i] = []byte(strings.Repeat(string(rune('a'+i)), 256))
	}

	encoded := make([]io.Reader, len(inputs))
	errs := make(chan error, len(inputs))
	var wg sync.WaitGroup
	for i := range inputs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			var err error
			encoded[i], err = f.Encode(bytes.NewReader(inputs[i]))
			if err != nil {
				errs <- err
			}
		}(i)
	}
	wg.Wait()
	for i := range encoded {
		if encoded[i] == nil {
			continue
		}
		compressed, err := io.ReadAll(encoded[i])
		if err != nil {
			errs <- err
			continue
		}
		decoded, err := f.Decode(bytes.NewReader(compressed))
		if err != nil {
			errs <- err
			continue
		}
		got, err := io.ReadAll(decoded)
		if err != nil {
			errs <- err
			continue
		}
		if !bytes.Equal(got, inputs[i]) {
			errs <- errors.New("decoded concurrent stream differs from its input")
		}
	}
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
}

type errorAfterReader struct {
	reader *bytes.Reader
	err    error
}

func (r *errorAfterReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if err == io.EOF {
		return 0, r.err
	}
	return n, err
}

func TestFlateEncodeReaderErrorDoesNotPoisonNextCall(t *testing.T) {
	f := newFlateFilter(t)
	wantErr := errors.New("synthetic source failure")
	encoded, err := f.Encode(&errorAfterReader{
		reader: bytes.NewReader([]byte("partial stream")),
		err:    wantErr,
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("encode error = %v, want %v", err, wantErr)
	}
	if encoded != nil {
		t.Fatal("reader-error encode returned a partial stream")
	}

	want := []byte("good stream after the source failure")
	encoded, err = f.Encode(bytes.NewReader(want))
	if err != nil {
		t.Fatalf("encode after source failure: %v", err)
	}
	decodeFlate(t, f, encoded, want)
}

func freshZlibEncode(r io.Reader) (io.Reader, error) {
	var b bytes.Buffer
	w := zlib.NewWriter(&b)
	if _, err := io.Copy(w, r); err != nil {
		_ = w.Close()
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return &b, nil
}

func TestFlateEncodeReusesWriterAllocations(t *testing.T) {
	f := newFlateFilter(t)
	actual := testing.AllocsPerRun(100, func() {
		encoded, err := f.Encode(bytes.NewReader(flateEncodeFixture))
		if err != nil {
			t.Fatalf("encode fixture: %v", err)
		}
		flateAllocationSink = encoded
	})
	fresh := testing.AllocsPerRun(100, func() {
		encoded, err := freshZlibEncode(bytes.NewReader(flateEncodeFixture))
		if err != nil {
			t.Fatalf("fresh encode fixture: %v", err)
		}
		flateAllocationSink = encoded
	})
	if actual >= fresh {
		t.Fatalf("actual encoder allocations = %.2f/op, fresh zlib = %.2f/op; writer reuse is not measurable", actual, fresh)
	}
}

var flateAllocationSink io.Reader

func BenchmarkFlateEncode(b *testing.B) {
	f := newFlateFilter(b)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		encoded, err := f.Encode(bytes.NewReader(flateEncodeFixture))
		if err != nil {
			b.Fatal(err)
		}
		flateAllocationSink = encoded
	}
}

func BenchmarkFreshZlibEncode(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		encoded, err := freshZlibEncode(bytes.NewReader(flateEncodeFixture))
		if err != nil {
			b.Fatal(err)
		}
		flateAllocationSink = encoded
	}
}
