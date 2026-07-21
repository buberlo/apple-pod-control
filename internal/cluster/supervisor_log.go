package cluster

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"golang.org/x/sys/unix"
)

const (
	SupervisorLogMaxBytes      int64 = 1 << 20
	supervisorLogDirectoryMode       = 0o700
	supervisorLogFileMode            = 0o600
	supervisorChmodPath              = "/bin/chmod"
	supervisorLSPath                 = "/bin/ls"
	supervisorDescriptorPath         = "/dev/fd/3"
)

type supervisorLogOwnership func(string, os.FileInfo) (int, int, error)

type supervisorLogRuntime struct {
	home      string
	uid       int
	gid       int
	maximum   int64
	ownership supervisorLogOwnership
}

// BoundedSupervisorLog is a descriptor-anchored sink. At the size cap it
// truncates the same verified file before appending the next complete write;
// callers can therefore never bypass the bound with an oversized log line.
type BoundedSupervisorLog struct {
	mu      sync.Mutex
	file    *os.File
	maximum int64
	size    int64
	closed  bool
}

// OpenSupervisorLog opens the exact unattended log for the current effective
// user. launchd invokes this code only after launchctl asuser + sudo -u has
// dropped privileges; no root process opens a target-home path.
func OpenSupervisorLog(path, role, name string) (*BoundedSupervisorLog, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, fmt.Errorf("resolve supervisor home: %w", err)
	}
	return openSupervisorLog(path, role, name, supervisorLogRuntime{
		home: home, uid: os.Geteuid(), gid: os.Getegid(), maximum: SupervisorLogMaxBytes,
		ownership: supervisorLogFileOwnership,
	})
}

func openSupervisorLog(path, role, name string, runtime supervisorLogRuntime) (*BoundedSupervisorLog, error) {
	if role != "server" && role != "agent" && role != "ha" {
		return nil, fmt.Errorf("supervisor log role must be server, agent, or ha")
	}
	if !dnsLabel.MatchString(name) {
		return nil, fmt.Errorf("supervisor log cluster name must be a lowercase DNS label")
	}
	home := filepath.Clean(runtime.home)
	if !filepath.IsAbs(home) || home == string(filepath.Separator) || home != runtime.home || strings.ContainsAny(home, "\r\n") {
		return nil, fmt.Errorf("supervisor home must be an exact absolute non-root path")
	}
	expectedDirectory := filepath.Join(home, "Library", "Logs", "APC")
	expectedPath := filepath.Join(expectedDirectory, role+"-"+name+"-unattended.log")
	if filepath.Clean(path) != path || path != expectedPath {
		return nil, fmt.Errorf("supervisor log path must be exactly %s", expectedPath)
	}
	if runtime.uid < 0 || runtime.gid < 0 || runtime.maximum <= 0 || runtime.ownership == nil {
		return nil, fmt.Errorf("supervisor log security context is invalid")
	}

	homeDirectory, err := openSupervisorDirectoryNoFollow(home)
	if err != nil {
		return nil, fmt.Errorf("open supervisor home: %w", err)
	}
	if err := validateSupervisorDirectory(homeDirectory, home, runtime, false); err != nil {
		_ = homeDirectory.Close()
		return nil, err
	}

	current := homeDirectory
	currentPath := home
	components := []string{"Library", "Logs", "APC"}
	for index, component := range components {
		nextPath := filepath.Join(currentPath, component)
		next, created, err := openOrCreateSupervisorDirectoryAt(current, nextPath, component)
		if err != nil {
			_ = current.Close()
			return nil, err
		}
		if created {
			if err := removeSupervisorExtendedACL(next); err != nil {
				_ = next.Close()
				_ = current.Close()
				return nil, fmt.Errorf("remove inherited supervisor log directory ACL: %w", err)
			}
			if err := next.Chmod(supervisorLogDirectoryMode); err != nil {
				_ = next.Close()
				_ = current.Close()
				return nil, fmt.Errorf("protect supervisor log directory: %w", err)
			}
			if err := current.Sync(); err != nil {
				_ = next.Close()
				_ = current.Close()
				return nil, fmt.Errorf("sync supervisor log parent: %w", err)
			}
		}
		if err := validateSupervisorDirectory(next, nextPath, runtime, index == len(components)-1); err != nil {
			_ = next.Close()
			_ = current.Close()
			return nil, err
		}
		if err := current.Close(); err != nil {
			_ = next.Close()
			return nil, fmt.Errorf("close supervisor log ancestor: %w", err)
		}
		current = next
		currentPath = nextPath
	}
	logDirectory := current

	logFile, created, err := openOrCreateSupervisorFileAt(logDirectory, expectedPath, filepath.Base(expectedPath), runtime)
	if err != nil {
		_ = logDirectory.Close()
		return nil, err
	}
	closeOnError := func(openErr error) (*BoundedSupervisorLog, error) {
		return nil, errors.Join(openErr, logFile.Close(), logDirectory.Close())
	}
	if created {
		if err := removeSupervisorExtendedACL(logFile); err != nil {
			return closeOnError(fmt.Errorf("remove inherited supervisor log file ACL: %w", err))
		}
		if err := logFile.Chmod(supervisorLogFileMode); err != nil {
			return closeOnError(fmt.Errorf("protect supervisor log file: %w", err))
		}
		if err := logFile.Sync(); err != nil {
			return closeOnError(fmt.Errorf("sync new supervisor log file: %w", err))
		}
		if err := logDirectory.Sync(); err != nil {
			return closeOnError(fmt.Errorf("sync supervisor log directory: %w", err))
		}
	}
	info, err := validateSupervisorFile(logFile, expectedPath, runtime)
	if err != nil {
		return closeOnError(err)
	}
	if err := verifySupervisorLogPathStable(home, components, expectedPath, logDirectory, logFile); err != nil {
		return closeOnError(err)
	}
	if err := validateSupervisorNoACLGrants(logDirectory, expectedDirectory); err != nil {
		return closeOnError(err)
	}
	if err := validateSupervisorNoACLGrants(logFile, expectedPath); err != nil {
		return closeOnError(err)
	}
	if err := logDirectory.Close(); err != nil {
		_ = logFile.Close()
		return nil, fmt.Errorf("close verified supervisor log directory: %w", err)
	}

	sink := &BoundedSupervisorLog{file: logFile, maximum: runtime.maximum, size: info.Size()}
	if sink.size > sink.maximum {
		if err := sink.resetLocked(); err != nil {
			_ = logFile.Close()
			return nil, err
		}
	}
	return sink, nil
}

func (log *BoundedSupervisorLog) Write(input []byte) (int, error) {
	log.mu.Lock()
	defer log.mu.Unlock()
	if log.closed || log.file == nil {
		return 0, os.ErrClosed
	}
	if len(input) == 0 {
		return 0, nil
	}
	info, err := log.file.Stat()
	if err != nil {
		return 0, fmt.Errorf("inspect bounded supervisor log: %w", err)
	}
	log.size = info.Size()
	data := input
	if int64(len(data)) > log.maximum {
		data = data[len(data)-int(log.maximum):]
		if err := log.resetLocked(); err != nil {
			return 0, err
		}
	} else if log.size+int64(len(data)) > log.maximum {
		if err := log.resetLocked(); err != nil {
			return 0, err
		}
	}
	for written := 0; written < len(data); {
		count, err := log.file.Write(data[written:])
		written += count
		log.size += int64(count)
		if err != nil {
			return 0, fmt.Errorf("write bounded supervisor log: %w", err)
		}
		if count == 0 {
			return 0, io.ErrShortWrite
		}
	}
	return len(input), nil
}

func (log *BoundedSupervisorLog) resetLocked() error {
	if err := log.file.Truncate(0); err != nil {
		return fmt.Errorf("truncate bounded supervisor log: %w", err)
	}
	if _, err := log.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("rewind bounded supervisor log: %w", err)
	}
	log.size = 0
	return nil
}

func (log *BoundedSupervisorLog) Close() error {
	log.mu.Lock()
	defer log.mu.Unlock()
	if log.closed {
		return nil
	}
	log.closed = true
	if log.file == nil {
		return nil
	}
	err := errors.Join(log.file.Sync(), log.file.Close())
	log.file = nil
	if err != nil {
		return fmt.Errorf("close bounded supervisor log: %w", err)
	}
	return nil
}

func openSupervisorDirectoryNoFollow(path string) (*os.File, error) {
	descriptor, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_DIRECTORY, 0)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(descriptor), path)
	if file == nil {
		_ = unix.Close(descriptor)
		return nil, fmt.Errorf("open directory")
	}
	return file, nil
}

func openSupervisorDirectoryAt(parent *os.File, path, component string) (*os.File, error) {
	descriptor, err := unix.Openat(int(parent.Fd()), component, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_DIRECTORY, 0)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(descriptor), path)
	if file == nil {
		_ = unix.Close(descriptor)
		return nil, fmt.Errorf("open directory")
	}
	return file, nil
}

func openOrCreateSupervisorDirectoryAt(parent *os.File, path, component string) (*os.File, bool, error) {
	directory, err := openSupervisorDirectoryAt(parent, path, component)
	if err == nil {
		return directory, false, nil
	}
	if !errors.Is(err, syscall.ENOENT) {
		return nil, false, fmt.Errorf("open supervisor log directory: %w", err)
	}
	created := false
	if mkdirErr := unix.Mkdirat(int(parent.Fd()), component, uint32(supervisorLogDirectoryMode)); mkdirErr == nil {
		created = true
	} else if !errors.Is(mkdirErr, syscall.EEXIST) {
		return nil, false, fmt.Errorf("create supervisor log directory: %w", mkdirErr)
	}
	directory, err = openSupervisorDirectoryAt(parent, path, component)
	if err != nil {
		return nil, false, fmt.Errorf("open created supervisor log directory: %w", err)
	}
	return directory, created, nil
}

func openOrCreateSupervisorFileAt(parent *os.File, path, component string, runtime supervisorLogRuntime) (*os.File, bool, error) {
	flags := unix.O_WRONLY | unix.O_APPEND | unix.O_NONBLOCK | unix.O_CLOEXEC | unix.O_NOFOLLOW
	descriptor, err := unix.Openat(int(parent.Fd()), component, flags|unix.O_CREAT|unix.O_EXCL, uint32(supervisorLogFileMode))
	created := err == nil
	if errors.Is(err, syscall.EEXIST) {
		var stat unix.Stat_t
		if statErr := unix.Fstatat(int(parent.Fd()), component, &stat, unix.AT_SYMLINK_NOFOLLOW); statErr != nil {
			return nil, false, fmt.Errorf("inspect existing supervisor log file: %w", statErr)
		}
		if stat.Mode&unix.S_IFMT != unix.S_IFREG || os.FileMode(stat.Mode).Perm() != supervisorLogFileMode ||
			int(stat.Uid) != runtime.uid || int(stat.Gid) != runtime.gid {
			return nil, false, fmt.Errorf("existing supervisor log must be a mode-0600 regular file owned by the effective account")
		}
		descriptor, err = unix.Openat(int(parent.Fd()), component, flags, 0)
	}
	if err != nil {
		return nil, false, fmt.Errorf("open supervisor log file: %w", err)
	}
	file := os.NewFile(uintptr(descriptor), path)
	if file == nil {
		_ = unix.Close(descriptor)
		return nil, false, fmt.Errorf("open supervisor log file")
	}
	return file, created, nil
}

func validateSupervisorDirectory(file *os.File, path string, runtime supervisorLogRuntime, exact bool) error {
	info, err := file.Stat()
	if err != nil {
		return fmt.Errorf("inspect supervisor log directory: %w", err)
	}
	if !info.IsDir() || info.Mode().Perm()&0o022 != 0 || (exact && info.Mode().Perm() != supervisorLogDirectoryMode) {
		return fmt.Errorf("supervisor log directories must be protected real directories")
	}
	uid, gid, err := runtime.ownership(path, info)
	if err != nil {
		return fmt.Errorf("inspect supervisor log directory ownership: %w", err)
	}
	if uid != runtime.uid || gid != runtime.gid {
		return fmt.Errorf("supervisor log directories must be owned by the effective account")
	}
	if err := validateSupervisorNoACLGrants(file, path); err != nil {
		return err
	}
	return nil
}

func validateSupervisorFile(file *os.File, path string, runtime supervisorLogRuntime) (os.FileInfo, error) {
	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("inspect supervisor log file: %w", err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != supervisorLogFileMode {
		return nil, fmt.Errorf("supervisor log must be a regular file with mode 0600")
	}
	uid, gid, err := runtime.ownership(path, info)
	if err != nil {
		return nil, fmt.Errorf("inspect supervisor log ownership: %w", err)
	}
	if uid != runtime.uid || gid != runtime.gid {
		return nil, fmt.Errorf("supervisor log must be owned by the effective account")
	}
	if err := validateSupervisorNoACLGrants(file, path); err != nil {
		return nil, err
	}
	return info, nil
}

// removeSupervisorExtendedACL targets the already verified descriptor through
// the child's inherited fd 3. This cannot be redirected by swapping a pathname
// while chmod is running.
func removeSupervisorExtendedACL(file *os.File) error {
	if runtime.GOOS != "darwin" {
		return nil
	}
	if file == nil {
		return fmt.Errorf("supervisor ACL descriptor is unavailable")
	}
	command := exec.Command(supervisorChmodPath, "-N", supervisorDescriptorPath)
	command.ExtraFiles = []*os.File{file}
	output, err := command.CombinedOutput()
	if err != nil {
		return fmt.Errorf("remove supervisor extended ACL: %w: %s", err, bytes.TrimSpace(output))
	}
	return nil
}

// validateSupervisorNoACLGrants tolerates macOS's usual deny-only HOME ACLs,
// but rejects every allow ACE. The descriptor identity check makes pathname
// replacement fail closed around the absolute ls inspection.
func validateSupervisorNoACLGrants(file *os.File, path string) error {
	if runtime.GOOS != "darwin" {
		return nil
	}
	before, err := file.Stat()
	if err != nil {
		return fmt.Errorf("inspect supervisor ACL target: %w", err)
	}
	output, err := exec.Command(supervisorLSPath, "-lde", path).CombinedOutput()
	if err != nil {
		return fmt.Errorf("inspect supervisor extended ACL: %w: %s", err, bytes.TrimSpace(output))
	}
	after, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("reinspect supervisor ACL target: %w", err)
	}
	if after.Mode()&os.ModeSymlink != 0 || !os.SameFile(before, after) {
		return fmt.Errorf("supervisor ACL target changed during inspection")
	}
	if err := rejectSupervisorACLGrants(output); err != nil {
		return fmt.Errorf("supervisor log path has an unsafe extended ACL: %w", err)
	}
	return nil
}

func rejectSupervisorACLGrants(output []byte) error {
	lines := bytes.Split(output, []byte{'\n'})
	if len(lines) == 0 || len(bytes.Fields(lines[0])) == 0 {
		return fmt.Errorf("ls returned malformed output")
	}
	for _, rawLine := range lines[1:] {
		line := strings.TrimSpace(string(rawLine))
		if line == "" {
			continue
		}
		entry, _, found := strings.Cut(line, ":")
		if !found {
			return fmt.Errorf("ls returned a malformed ACL entry")
		}
		if _, err := strconv.Atoi(entry); err != nil {
			return fmt.Errorf("ls returned a malformed ACL index")
		}
		switch {
		case strings.Contains(line, " allow "):
			return fmt.Errorf("allow grant is forbidden")
		case strings.Contains(line, " deny "):
			// Deny-only entries are the normal macOS HOME protection.
		default:
			return fmt.Errorf("ls returned an unknown ACL entry")
		}
	}
	return nil
}

func verifySupervisorLogPathStable(home string, components []string, logPath string, expectedDirectory, expectedFile *os.File) error {
	current, err := openSupervisorDirectoryNoFollow(home)
	if err != nil {
		return fmt.Errorf("reopen supervisor home: %w", err)
	}
	currentPath := home
	for _, component := range components {
		nextPath := filepath.Join(currentPath, component)
		next, err := openSupervisorDirectoryAt(current, nextPath, component)
		if err != nil {
			_ = current.Close()
			return fmt.Errorf("supervisor log path changed during verification: %w", err)
		}
		if err := current.Close(); err != nil {
			_ = next.Close()
			return fmt.Errorf("close supervisor log ancestor: %w", err)
		}
		current = next
		currentPath = nextPath
	}
	defer current.Close()
	if err := requireSameSupervisorFile(expectedDirectory, current, "directory"); err != nil {
		return err
	}
	descriptor, err := unix.Openat(int(current.Fd()), filepath.Base(logPath), unix.O_WRONLY|unix.O_APPEND|unix.O_NONBLOCK|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return fmt.Errorf("reopen supervisor log file: %w", err)
	}
	file := os.NewFile(uintptr(descriptor), logPath)
	if file == nil {
		_ = unix.Close(descriptor)
		return fmt.Errorf("reopen supervisor log file")
	}
	defer file.Close()
	return requireSameSupervisorFile(expectedFile, file, "file")
}

func requireSameSupervisorFile(expected, actual *os.File, description string) error {
	want, err := expected.Stat()
	if err != nil {
		return fmt.Errorf("inspect verified supervisor log %s: %w", description, err)
	}
	got, err := actual.Stat()
	if err != nil {
		return fmt.Errorf("inspect reopened supervisor log %s: %w", description, err)
	}
	if !os.SameFile(want, got) {
		return fmt.Errorf("supervisor log %s changed during verification", description)
	}
	return nil
}

func supervisorLogFileOwnership(_ string, info os.FileInfo) (int, int, error) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, 0, fmt.Errorf("supervisor log ownership metadata is unavailable")
	}
	return int(stat.Uid), int(stat.Gid), nil
}
