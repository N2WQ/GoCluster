package main

import (
	"fmt"
	"strings"

	"dxcluster/peer"
)

func sendInit(send func(string) error, tsGen *peer.TimestampGenerator, cfg probeConfig, pc9x bool) error {
	if pc9x {
		line, err := sendPC92Add(tsGen, cfg)
		if err != nil {
			return err
		}
		if err := send(line); err != nil {
			return err
		}
		line, err = buildPC92Keepalive(tsGen, cfg)
		if err != nil {
			return err
		}
		if err := send(line); err != nil {
			return err
		}
		return send("PC20^")
	}
	line := fmt.Sprintf("PC19^1^%s^0^%s^H%d^", cfg.call, cfg.legacyVersion, cfg.hop)
	if err := send(line); err != nil {
		return err
	}
	return send("PC20^")
}

func sendPC92Add(tsGen *peer.TimestampGenerator, cfg probeConfig) (string, error) {
	ts, err := tsGen.Next()
	if err != nil {
		return "", err
	}
	entry := fmt.Sprintf("%d%s:%s", cfg.pc92Bitmap, cfg.call, cfg.nodeVersion)
	if strings.TrimSpace(cfg.build) != "" {
		entry += ":" + strings.TrimSpace(cfg.build)
	}
	return fmt.Sprintf("PC92^%s^%s^A^^%s^H%d^", cfg.call, ts, entry, cfg.hop), nil
}

func buildPC92Keepalive(tsGen *peer.TimestampGenerator, cfg probeConfig) (string, error) {
	ts, err := tsGen.Next()
	if err != nil {
		return "", err
	}
	entry := fmt.Sprintf("%d%s:%s", cfg.pc92Bitmap, cfg.call, cfg.nodeVersion)
	if strings.TrimSpace(cfg.build) != "" {
		entry += ":" + strings.TrimSpace(cfg.build)
	}
	return fmt.Sprintf("PC92^%s^%s^K^%s^%d^%d^H%d^", cfg.call, ts, entry, cfg.nodeCount, cfg.userCount, cfg.hop), nil
}

func modeLabel(pc9x bool) string {
	if pc9x {
		return "pc9x"
	}
	return "legacy"
}
