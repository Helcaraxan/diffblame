package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/plumbing/storer"
	"github.com/go-git/go-git/v5/storage/memory"
	"github.com/sirupsen/logrus"
)

var (
	debug    = flag.Bool("debug", false, "Print debug output.")
	beginRev = flag.String("begin", "", "Commit from which to start the diff-blame.")
	endRev   = flag.String("end", "", "Commit at which to end the diff-blame.")

	logger = logrus.New()

	repo        *git.Repository
	beginCommit *object.Commit
	endCommit   *object.Commit
)

func main() {
	flag.Parse()

	if *debug {
		logger.SetLevel(logrus.DebugLevel)
	}

	setup()

	files := changedFiles()

	for _, c := range computeDiffCommits(files) {
		fmt.Printf("%s > %- 30s %s %s\n", cutString(c.Hash.String(), 6), cutString(c.Committer.Name, 30), c.Committer.When.UTC().Format("02/01/06"), cutString(strings.Split(c.Message, "\n")[0], 80))
	}
}

func cutString(s string, l int) string {
	if len(s) <= l {
		return s
	}
	return s[:l]
}

func setup() {
	wd, err := os.Getwd()
	if err != nil {
		logger.Fatalf("Could not detect current path: %v", err)
	}

	repo, err = git.Clone(memory.NewStorage(), nil, &git.CloneOptions{URL: wd})
	if err != nil {
		logger.Fatalf("Could not open repository in %q: %v", wd, err)
	}

	beginCommit = resolveCommit(*beginRev)
	endCommit = resolveCommit(*endRev)

	logger.Print("Setup done")
}

func resolveCommit(refname string) *object.Commit {
	if !plumbing.IsHash(refname) {
		refname = strings.TrimPrefix(refname, "origin/")
		ref, err := storer.ResolveReference(repo.Storer, plumbing.NewRemoteReferenceName("origin", refname))
		if err != nil {
			logger.Fatalf("Failed to resolve reference %q: %v", refname, err)
		} else if ref.Type() != plumbing.HashReference {
			logger.Fatalf("Reference %q was not resolved to a commit hash but to a reference of type %q.", refname, ref.Type().String())
		}
		refname = ref.Hash().String()
	}

	c, err := repo.CommitObject(plumbing.NewHash(refname))
	if err != nil {
		logger.Fatalf("Failed to resolve commit %q: %v", refname, err)
	}
	return c
}

type changeList struct {
	added   []string
	removed []string
	changed []string
}

func changedFiles() changeList {
	beginTree, err := beginCommit.Tree()
	if err != nil {
		logger.Fatalf("Failed to find the tree object for begin commit %q: %v", beginCommit.Hash.String(), err)
	}
	endTree, err := endCommit.Tree()
	if err != nil {
		logger.Fatalf("Failed to find the tree object for end commit %q: %v", endCommit.Hash.String(), err)
	}

	diff, err := object.DiffTreeWithOptions(context.Background(), beginTree, endTree, &object.DiffTreeOptions{
		DetectRenames: true,
		RenameScore:   70,
		RenameLimit:   0,
	})
	if err != nil {
		logger.Fatalf("Failed to compute the diff for range %s..%s: %v", *beginRev, *endRev, err)
	}

	changelist, err := diff.Patch()
	if err != nil {
		logger.Fatalf("Failed to transform the diff into a list of patches: %v", err)
	}

	var cl changeList
	for _, patch := range changelist.FilePatches() {
		srcFile, dstFile := patch.Files()
		switch {
		case srcFile == nil:
			if strings.Contains(dstFile.Path(), "vendor/") {
				break
			}
			cl.added = append(cl.added, dstFile.Path())
		case dstFile == nil:
			if strings.Contains(srcFile.Path(), "vendor/") {
				break
			}
			cl.removed = append(cl.removed, srcFile.Path())
		case srcFile.Path() != dstFile.Path():
			if strings.Contains(srcFile.Path(), "vendor/") {
				break
			}
			cl.added = append(cl.added, dstFile.Path())
			cl.removed = append(cl.removed, srcFile.Path())
		default:
			if strings.Contains(srcFile.Path(), "vendor/") {
				break
			}
			cl.changed = append(cl.changed, srcFile.Path())
		}
	}
	logger.Debugf("Found changed files: %+v", cl)
	return cl
}

func computeDiffCommits(cl changeList) []*object.Commit {
	commitsSet := map[string]*object.Commit{}

	statuses := map[string]fileStatus{}
	for _, path := range cl.added {
		statuses[path] = fileStatusFound
	}
	logger.Infof("Resolving commits for added files.")
	accumulateCommitsForPaths(endCommit, nil, statuses, addAlways(commitsSet))

	statuses = map[string]fileStatus{}
	for _, path := range cl.removed {
		statuses[path] = fileStatusSeeking
	}
	logger.Infof("Resolving commits for removed files.")
	accumulateCommitsForPaths(endCommit, nil, statuses, addIfNotAncestor(commitsSet, endCommit))

	statuses = map[string]fileStatus{}
	for _, path := range cl.changed {
		statuses[path] = fileStatusFound
	}
	logger.Infof("Resolving commits for changed files.")
	accumulateCommitsForPaths(endCommit, nil, statuses, addIfNotAncestor(commitsSet, endCommit))

	logger.Infof("Sorting commit list.")
	var commits []*object.Commit
	for _, c := range commitsSet {
		commits = append(commits, c)
	}
	sort.Slice(commits, func(i int, j int) bool {
		return commits[i].Committer.When.Before(commits[j].Committer.When)
	})
	return commits
}

type fileStatus uint8

const (
	fileStatusSeeking fileStatus = iota
	fileStatusFound
	fileStatusRemoved
)

func copyMap(m map[string]fileStatus) map[string]fileStatus {
	n := make(map[string]fileStatus, len(m))
	for k, v := range m {
		n[k] = v
	}
	return n
}

func accumulateCommitsForPaths(current *object.Commit, seen map[string]bool, statuses map[string]fileStatus, acc func(*object.Commit) bool) {
	if seen == nil {
		seen = map[string]bool{}
	}

	for {
		if seen[current.Hash.String()] {
			logger.Debugf("Commit %q has already been processed. Skipping.", current.Hash.String())
			return
		}
		seen[current.Hash.String()] = true

		if current.NumParents() != 1 {
			break
		}
		logger.Debugf("Considering commit %s %s.", current.Hash.String()[:6], cutString(strings.Split(current.Message, "\n")[0], 80))

		var process bool
		for path, status := range statuses {
			if status == fileStatusRemoved {
				continue
			}

			if _, err := current.File(path); err != nil {
				if err != object.ErrFileNotFound {
					logger.Fatalf("Unexpected path error for %q on %q: %v", path, current.Hash.String(), err)
				} else if status == fileStatusFound {
					logger.Debugf("No longer considering path %q as it has disappeared from the history.", path)
					statuses[path] = fileStatusRemoved
				}
				logger.Debugf("Processing as file %q has still not been found.", path)
			} else {
				logger.Debugf("Processing as commit contains path %q.", path)
				statuses[path] = fileStatusFound
				process = true
			}
		}

		if process {
			logger.Debugf("Running commit through accumulator.")
			if !acc(current) {
				logger.Debugf("Bailing out based on accumulator output.")
				return
			}
		}

		next, err := current.Parent(0)
		if err != nil {
			logger.Debugf("Could not get sole parent of %q: %v", current.Hash.String(), err)
		}
		current = next
	}

	if current.NumParents() == 0 {
		logger.Debugf("Reached root commit.")
	} else {
		logger.Debugf("Considering merge commit %s %s.", current.Hash.String()[:6], cutString(strings.Split(current.Message, "\n")[0], 80))
	}

	for idx := 0; idx < current.NumParents(); idx++ {
		p, err := current.Parent(idx)
		if err != nil {
			logger.Fatalf("Could not get parent %d of %q: %v", idx+1, current.Hash.String(), err)
		}

		var process bool
		for path, status := range statuses {
			if status == fileStatusRemoved {
				continue
			} else if _, fileErr := p.File(path); fileErr == nil || status == fileStatusSeeking {
				process = true
				break
			}
		}

		ancestor, err := p.IsAncestor(beginCommit)
		if err != nil {
			logger.Fatalf("Could not determine whether %q is an ancestor of %q: %v", p.Hash.String(), beginCommit.Hash.String(), err)
		}

		switch {
		case !process:
			logger.Debugf("Not branching as '%s %s' no longer contains any paths of interest.", p.Hash.String()[:6], cutString(strings.Split(p.Message, "\n")[0], 80))
		case ancestor:
			logger.Debugf("Not branching as '%s %s' is an ancestor of the begin commit.", p.Hash.String()[:6], cutString(strings.Split(p.Message, "\n")[0], 80))
		default:
			accumulateCommitsForPaths(p, seen, copyMap(statuses), acc)
		}
	}
}

func addAlways(commits map[string]*object.Commit) func(c *object.Commit) bool {
	return func(c *object.Commit) bool {
		commits[c.Hash.String()] = c
		return true
	}
}

func addIfNotAncestor(commits map[string]*object.Commit, base *object.Commit) func(c *object.Commit) bool {
	return func(c *object.Commit) bool {
		ancestry, err := c.IsAncestor(base)
		if err != nil {
			logger.Fatalf("Could not determine whether %q is an ancestor of %q: %v", c.Hash.String(), base.Hash.String(), err)
		}
		if !ancestry {
			commits[c.Hash.String()] = c
			return true
		}
		return false
	}
}
