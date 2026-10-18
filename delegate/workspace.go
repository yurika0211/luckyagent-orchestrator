package delegate

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

// WorkspacePolicy lets an embedding application retain its filesystem rules
// without making the delegation engine depend on a tool SDK or agent runtime.
// Callbacks must be safe for concurrent use. Missing callbacks use the defaults.
type WorkspacePolicy struct {
	Validate func(string) error
	Expand   func(string) string
}

func workspacePolicy(policies []WorkspacePolicy) WorkspacePolicy {
	policy := WorkspacePolicy{Validate: validateTemporaryWorkspace, Expand: expandWorkspacePath}
	if len(policies) > 0 {
		if policies[0].Validate != nil {
			policy.Validate = policies[0].Validate
		}
		if policies[0].Expand != nil {
			policy.Expand = policies[0].Expand
		}
	}
	return policy
}

// The standalone default only creates workspaces within the OS temporary
// directory. The LuckyAgent tool adapter supplies its existing sandbox policy.
func validateTemporaryWorkspace(path string) error {
	for _, r := range path {
		if r < 32 || strings.ContainsRune("!@#$", r) {
			return fmt.Errorf("invalid path: %s", path)
		}
	}
	clean, err := filepath.Abs(filepath.Clean(path))
	if err != nil {
		return err
	}
	root, err := filepath.Abs(os.TempDir())
	if err != nil {
		return err
	}
	if resolved, err := filepath.EvalSymlinks(root); err == nil {
		root = resolved
	}
	// Resolve the existing ancestor too: the final directory may not exist yet,
	// while one of its parents can already be a symlink outside the temp root.
	ancestor := clean
	var missing []string
	for {
		resolved, resolveErr := filepath.EvalSymlinks(ancestor)
		if resolveErr == nil {
			clean = resolved
			for i := len(missing) - 1; i >= 0; i-- {
				clean = filepath.Join(clean, missing[i])
			}
			break
		}
		if !os.IsNotExist(resolveErr) {
			return resolveErr
		}
		parent := filepath.Dir(ancestor)
		if parent == ancestor {
			return resolveErr
		}
		missing = append(missing, filepath.Base(ancestor))
		ancestor = parent
	}
	rel, err := filepath.Rel(root, clean)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return fmt.Errorf("access denied: delegation workspace is outside temporary directory: %s", path)
	}
	return nil
}

func expandWorkspacePath(path string) string {
	if path == "~" || strings.HasPrefix(path, "~/") || strings.HasPrefix(path, `~\`) {
		if home, err := os.UserHomeDir(); err == nil {
			if path == "~" {
				return home
			}
			return filepath.Join(home, path[2:])
		}
	}
	if runtime.GOOS == "windows" {
		slashPath := filepath.ToSlash(path)
		if slashPath == "/tmp" {
			return os.TempDir()
		}
		if strings.HasPrefix(slashPath, "/tmp/") {
			return filepath.Join(os.TempDir(), strings.TrimPrefix(slashPath, "/tmp/"))
		}
	}
	return path
}
