package cluster

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

const (
	haOperationLockFilename = ".ha-operation.lock"
	haOperationLockPoll     = 25 * time.Millisecond
)

// haOperationLock serializes destructive HA operations across independent apc
// CLI processes. Public operations acquire it once and call only their locked
// internal helpers, avoiding recursive flock acquisition during recovery. The
// lock inode intentionally remains after release (and cluster deletion) so a
// waiter can never lock a replacement inode while an older operation owns it.
type haOperationLock struct {
	file *os.File
}

func acquireHAOperationLock(ctx context.Context, name string) (*haOperationLock, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("acquire HA operation lock: %w", err)
	}
	configPath, err := HAConfigPath(name)
	if err != nil {
		return nil, err
	}
	directory := filepath.Dir(configPath)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return nil, fmt.Errorf("create HA operation lock directory: %w", err)
	}
	directoryInfo, err := os.Lstat(directory)
	if err != nil {
		return nil, fmt.Errorf("inspect HA operation lock directory: %w", err)
	}
	if directoryInfo.Mode()&os.ModeSymlink != 0 || !directoryInfo.IsDir() || !haPathOwnedByEffectiveUser(directoryInfo) {
		return nil, fmt.Errorf("HA operation lock directory must be a real directory owned by the current user")
	}
	if directoryInfo.Mode().Perm()&0o077 != 0 {
		if err := os.Chmod(directory, 0o700); err != nil {
			return nil, fmt.Errorf("secure owned HA operation lock directory: %w", err)
		}
	}

	path := filepath.Join(directory, haOperationLockFilename)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open HA operation lock: %w", err)
	}
	closeWithError := func(operationErr error) (*haOperationLock, error) {
		return nil, errors.Join(operationErr, file.Close())
	}
	openedInfo, err := file.Stat()
	if err != nil {
		return closeWithError(fmt.Errorf("inspect opened HA operation lock: %w", err))
	}
	pathInfo, err := os.Lstat(path)
	if err != nil {
		return closeWithError(fmt.Errorf("inspect HA operation lock path: %w", err))
	}
	if pathInfo.Mode()&os.ModeSymlink != 0 || !openedInfo.Mode().IsRegular() || !os.SameFile(openedInfo, pathInfo) || !haPathOwnedByEffectiveUser(openedInfo) || openedInfo.Mode().Perm()&0o077 != 0 {
		return closeWithError(fmt.Errorf("HA operation lock must be one private regular file with mode 0600 or stricter"))
	}

	ticker := time.NewTicker(haOperationLockPoll)
	defer ticker.Stop()
	for {
		err = syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			return &haOperationLock{file: file}, nil
		}
		if !errors.Is(err, syscall.EWOULDBLOCK) && !errors.Is(err, syscall.EAGAIN) {
			return closeWithError(fmt.Errorf("acquire HA operation lock: %w", err))
		}
		select {
		case <-ctx.Done():
			return closeWithError(fmt.Errorf("wait for HA cluster %q operation lock: %w", name, ctx.Err()))
		case <-ticker.C:
		}
	}
}

func haPathOwnedByEffectiveUser(info os.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return !ok || int(stat.Uid) == os.Geteuid()
}

func (lock *haOperationLock) release() error {
	if lock == nil || lock.file == nil {
		return nil
	}
	unlockErr := syscall.Flock(int(lock.file.Fd()), syscall.LOCK_UN)
	closeErr := lock.file.Close()
	lock.file = nil
	if unlockErr != nil {
		unlockErr = fmt.Errorf("release HA operation lock: %w", unlockErr)
	}
	if closeErr != nil {
		closeErr = fmt.Errorf("close HA operation lock: %w", closeErr)
	}
	return errors.Join(unlockErr, closeErr)
}
