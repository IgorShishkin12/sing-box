// Package knownhosts stores a name→service-hash mapping in known_hosts.json.
package knownhosts

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
)

func hostsFile(configDir string) string {
	return filepath.Join(configDir, "known_hosts.json")
}

// Load reads the known_hosts.json from configDir.
// Returns an empty map (not an error) if the file does not exist.
func Load(configDir string) (map[string]string, error) {
	data, err := os.ReadFile(hostsFile(configDir))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return map[string]string{}, nil
		}
		return nil, err
	}
	var m map[string]string
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return m, nil
}

// Save atomically updates the known_hosts.json entry for name.
func Save(configDir, name, hash string) error {
	if err := os.MkdirAll(configDir, 0o700); err != nil {
		return err
	}
	m, err := Load(configDir)
	if err != nil {
		return err
	}
	m[name] = hash
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(configDir, ".known_hosts_tmp")
	if err != nil {
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmp.Name())
		return err
	}
	return os.Rename(tmp.Name(), hostsFile(configDir))
}
