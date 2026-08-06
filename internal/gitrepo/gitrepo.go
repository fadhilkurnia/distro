package gitrepo

import (
	"fmt"
	"os"
	"path/filepath"

	git "github.com/go-git/go-git/v5"
	gitconfig "github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
)

// Trimsh full 40 character commit hash into 10 characters
func ShortHash(fullHash string) string {
	const n = 10
	if len(fullHash) < n {
		return fullHash
	}
	return fullHash[:n]
}

// Ensure repoDir exists as a git clone inside of workdir
func EnsureCloned(repoDir, repoURL string) error {
	if _, err := os.Stat(repoDir); err == nil {
		return nil
	}
	if _, err := git.PlainClone(repoDir, false, &git.CloneOptions{URL: repoURL}); err != nil {
		return fmt.Errorf("gitrepo: cloning %s: %w", repoURL, err)
	}
	return nil
}

// Checks out commit in the repoDir based on hash
func Checkout(repoDir, hash string) error {
	repo, err := git.PlainOpen(repoDir)
	if err != nil {
		return fmt.Errorf("gitrepo: opening %s: %w", repoDir, err)
	}
	wt, err := repo.Worktree()
	if err != nil {
		return fmt.Errorf("gitrepo: getting worktree for %s: %w", repoDir, err)
	}

	checkoutErr := wt.Checkout(&git.CheckoutOptions{Hash: plumbing.NewHash(hash)})
	if checkoutErr == nil {
		return nil
	}

	// Commit not found in what was already fetched:
	// pull every branch and retry once.
	fetchErr := repo.Fetch(&git.FetchOptions{
		RefSpecs: []gitconfig.RefSpec{"+refs/heads/*:refs/remotes/origin/*"},
	})
	if fetchErr != nil && fetchErr != git.NoErrAlreadyUpToDate {
		return fmt.Errorf("gitrepo: fetching additional refs for %s: %w", repoDir, fetchErr)
	}

	if err := wt.Checkout(&git.CheckoutOptions{Hash: plumbing.NewHash(hash)}); err != nil {
		return fmt.Errorf("gitrepo: checking out %s in %s: %w", hash, repoDir, err)
	}
	return nil
}

// Checks whether path exists on the local filesystem
func FileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// Checks for a "<versionName>-<filename>" override inside workdir 
// if not exists, falls back to plain filename
// Returns a path relative to workdir
func ResolveOverride(workdir, filename, versionName string) string {
	override := fmt.Sprintf("%s-%s", versionName, filename)
	if FileExists(filepath.Join(workdir, override)) {
		return override
	}
	return filename
}
