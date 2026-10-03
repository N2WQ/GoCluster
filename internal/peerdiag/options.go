package peerdiag

import (
	"encoding/binary"
	"errors"
	"io"
	"time"
)

const (
	optionsHeaderBytes = 32
	optionsMagic       = 0x31444750 // PGD1, fixed options IPC version
)

func sendOptions(connection io.ReadWriter, options Options) error {
	if !validOptions(options) {
		return errors.New("diagnostic options reservation exhausted")
	}
	var header [optionsHeaderBytes]byte
	binary.LittleEndian.PutUint32(header[0:4], optionsMagic)
	binary.LittleEndian.PutUint32(header[4:8], uint32(len(options.Directory)))
	binary.LittleEndian.PutUint32(header[8:12], uint32(len(options.OverlongPath)))
	if options.Enabled {
		binary.LittleEndian.PutUint32(header[12:16], 1)
	}
	binary.LittleEndian.PutUint64(header[16:24], uint64(options.RetentionDays))
	binary.LittleEndian.PutUint64(header[24:32], uint64(options.DedupeWindow))
	if err := writeFull(connection, header[:]); err != nil {
		return err
	}
	// Fixed chunks avoid copying a whole configuration path into an IPC buffer.
	var chunk [RecordBytes]byte
	for _, path := range []string{options.Directory, options.OverlongPath} {
		for len(path) > 0 {
			n := copy(chunk[:], path)
			if err := writeFull(connection, chunk[:n]); err != nil {
				return err
			}
			path = path[n:]
		}
	}
	var ack [4]byte
	if _, err := io.ReadFull(connection, ack[:]); err != nil {
		return err
	}
	if binary.LittleEndian.Uint32(ack[:]) != optionsMagic {
		return errors.New("diagnostic options rejected")
	}
	return nil
}

func receiveOptions(connection io.ReadWriter) (Options, error) {
	var header [optionsHeaderBytes]byte
	if _, err := io.ReadFull(connection, header[:]); err != nil {
		return Options{}, err
	}
	directory := uint64(binary.LittleEndian.Uint32(header[4:8]))
	overlong := uint64(binary.LittleEndian.Uint32(header[8:12]))
	flags := binary.LittleEndian.Uint32(header[12:16])
	retention := binary.LittleEndian.Uint64(header[16:24])
	dedupe := binary.LittleEndian.Uint64(header[24:32])
	if binary.LittleEndian.Uint32(header[:4]) != optionsMagic || flags > 1 || retention > uint64(^uint(0)>>1) || dedupe > uint64(1<<63-1) || !helperOptionsFit(directory, overlong, 0) {
		return Options{}, errors.New("invalid diagnostic options header or reservation")
	}
	// Admission precedes native output allocation on Windows. The helper
	// inherits no PWD and never changes its working directory.
	cwdBytes, err := helperCurrentDirectoryBytes(directory, overlong)
	if err != nil {
		return Options{}, err
	}
	if !helperOptionsFit(directory, overlong, cwdBytes) {
		return Options{}, errors.New("diagnostic path reservation exhausted")
	}
	payload := make([]byte, int(directory+overlong))
	if _, err = io.ReadFull(connection, payload); err != nil {
		return Options{}, err
	}
	options := Options{Enabled: flags == 1, Directory: string(payload[:directory]), OverlongPath: string(payload[directory:]), RetentionDays: int(retention), DedupeWindow: time.Duration(dedupe)}
	var ack [4]byte
	binary.LittleEndian.PutUint32(ack[:], optionsMagic)
	if err = writeFull(connection, ack[:]); err != nil {
		return Options{}, err
	}
	return options, nil
}
